"""
ledger_reconcile.py
Reconciles Redshift payment totals against Oracle Financials GL entries.
Known issue CPR-004: Oracle GL API rate limit (100 req/min) causes failures on peak days.
Fix pending: Implement batch GL entry endpoint (CPR-004).
"""
import requests
import psycopg2
import boto3
import json
import logging
from decimal import Decimal
from datetime import date, datetime
from typing import List, Dict

logger = logging.getLogger(__name__)

ORACLE_GL_BASE_URL   = "https://oracle-financials.insurer.internal/fscmRestApi/resources/v1"
ORACLE_RATE_LIMIT    = 100   # requests per minute
VARIANCE_AUTO_APPROVE= Decimal("1.00")
VARIANCE_ALERT_USD   = Decimal("10000.00")


def get_redshift_payment_totals(payment_date: str) -> Dict[str, Decimal]:
    """Fetch total payments per provider from Redshift for reconciliation."""
    conn   = _get_redshift_connection()
    cur    = conn.cursor()
    cur.execute("""
        SELECT
            provider_npi,
            SUM(final_payment_amount_usd) AS total_paid_usd,
            COUNT(*)                      AS claim_count
        FROM claims_payments.fact_claim_payments
        WHERE payment_batch_date = %s
        AND   payment_status     = 'SUBMITTED'
        GROUP BY provider_npi
    """, (payment_date,))
    totals = {row[0]: Decimal(str(row[1])) for row in cur.fetchall()}
    cur.close()
    conn.close()
    return totals


def get_oracle_gl_entries(payment_date: str) -> Dict[str, Decimal]:
    """
    Fetch GL entries from Oracle Financials for the payment date.
    Known issue CPR-004: Rate limit causes 429 on high volume days.
    Workaround: exponential backoff retry (max 3 attempts).
    """
    secret  = _get_secret("claims/oracle/gl_api")
    headers = {
        "Authorization": f"Bearer {secret['access_token']}",
        "Content-Type":  "application/json",
    }
    gl_totals = {}
    page = 1
    while True:
        resp = _rate_limited_get(
            f"{ORACLE_GL_BASE_URL}/generalLedger/journalEntries",
            headers=headers,
            params={
                "q":      f"accountingDate='{payment_date}';description LIKE 'CLAIM PAYMENT%'",
                "limit":  500,
                "offset": (page - 1) * 500,
            }
        )
        items = resp.get("items", [])
        if not items:
            break
        for item in items:
            provider_npi = item.get("reference3")  # NPI stored in reference3 field
            amount = Decimal(str(item.get("enteredDebit", "0")))
            gl_totals[provider_npi] = gl_totals.get(provider_npi, Decimal("0")) + amount
        if not resp.get("hasMore"):
            break
        page += 1
    return gl_totals


def reconcile(payment_date: str, verbose: bool = False) -> List[Dict]:
    """
    Compare Redshift payment totals to Oracle GL entries.
    Returns list of variance records.
    """
    redshift_totals = get_redshift_payment_totals(payment_date)
    oracle_totals   = get_oracle_gl_entries(payment_date)
    all_npis        = set(list(redshift_totals.keys()) + list(oracle_totals.keys()))

    variances = []
    for npi in all_npis:
        rs_amount  = redshift_totals.get(npi, Decimal("0"))
        gl_amount  = oracle_totals.get(npi, Decimal("0"))
        variance   = abs(rs_amount - gl_amount)

        status = "MATCHED"
        if variance > VARIANCE_ALERT_USD:
            status = "ALERT"
            logger.error(f"LARGE VARIANCE for NPI {npi}: ${variance:,.2f} — escalate to Finance")
        elif variance > VARIANCE_AUTO_APPROVE:
            status = "REVIEW"
            logger.warning(f"Variance for NPI {npi}: ${variance:,.2f} — flagged for Finance review")

        if verbose or status != "MATCHED":
            variances.append({
                "provider_npi":    npi,
                "redshift_amount": float(rs_amount),
                "oracle_gl_amount": float(gl_amount),
                "variance_usd":    float(variance),
                "status":          status,
                "payment_date":    payment_date,
            })

    logger.info(f"Reconciliation {payment_date}: {len(variances)} variances found")
    return variances


def _rate_limited_get(url, headers, params, max_retries=3):
    """GET with exponential backoff for Oracle API rate limiting (CPR-004)."""
    import time
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            if resp.status_code == 429:
                wait = 2 ** attempt * 10
                logger.warning(f"Oracle GL rate limit hit — waiting {wait}s (attempt {attempt+1})")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)


def _get_redshift_connection():
    secret = _get_secret("claims/redshift/payments")
    return psycopg2.connect(
        host="claims-prod-cluster.us-east-1.redshift.amazonaws.com",
        port=5439, dbname="claimsdb",
        user="claims_payments_recon",
        password=secret["password"], sslmode="require",
    )


def _get_secret(secret_name: str) -> dict:
    client = boto3.client("secretsmanager", region_name="us-east-1")
    return json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])
