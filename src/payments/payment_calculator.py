"""
payment_calculator.py
Calculates final payment amounts for approved claims.
Applies deductible, copay, OON adjustments, and benefit limits.
Known issue CPR-002: OON rounding uses ROUND_HALF_UP vs Oracle ROUND_HALF_EVEN.
Fix: switch to Decimal ROUND_HALF_EVEN to match Oracle GL rounding.
"""
import psycopg2
import boto3
import json
import logging
from decimal import Decimal, ROUND_HALF_EVEN, ROUND_HALF_UP
from datetime import date
from typing import List, Dict

logger = logging.getLogger(__name__)


def get_approved_claims(batch_date: str) -> List[Dict]:
    """
    Fetch all APPROVED and PARTIAL claims not yet in a payment batch.
    Includes DISTINCT ON claim_id to prevent duplicates (hotfix for CPR-006).
    """
    conn = _get_redshift_connection()
    cur  = conn.cursor()
    cur.execute("""
        SELECT DISTINCT ON (fca.claim_id)
            fca.claim_id,
            fca.member_id,
            fca.provider_npi,
            fca.final_payment_amount_usd,
            fca.adjudication_decision,
            fca.network_status,
            pd.routing_number   AS provider_routing_number,
            pd.account_number   AS provider_account_number
        FROM claims_adjudicated.fact_claim_adjudication fca
        JOIN claims_rules.provider_disbursement_info pd
            ON fca.provider_npi = pd.provider_npi
        LEFT JOIN claims_payments.fact_claim_payments fcp
            ON fca.claim_id = fcp.claim_id
        WHERE fca.adjudication_decision IN ('APPROVED', 'PARTIAL')
        AND   fca.adjudicated_at::date <= %s
        AND   fcp.claim_id IS NULL    -- not yet paid
        ORDER BY fca.claim_id, fca.adjudicated_at DESC
    """, (batch_date,))
    columns = [desc[0] for desc in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()
    logger.info(f"Found {len(rows)} approved claims pending payment for {batch_date}")
    return rows


def calculate_final_payment(claim: Dict) -> Decimal:
    """
    Calculate final disbursement amount.
    Uses ROUND_HALF_EVEN (banker's rounding) to match Oracle GL.
    Previously used ROUND_HALF_UP — caused CPR-002 reconciliation variances.
    """
    amount = Decimal(str(claim["final_payment_amount_usd"]))
    # Apply rounding — ROUND_HALF_EVEN matches Oracle Financials GL
    return amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)


def _get_redshift_connection():
    secret = _get_secret("claims/redshift/payments")
    return psycopg2.connect(
        host="claims-prod-cluster.us-east-1.redshift.amazonaws.com",
        port=5439, dbname="claimsdb",
        user="claims_payments",
        password=secret["password"],
        sslmode="require",
    )


def _get_secret(secret_name: str) -> dict:
    client = boto3.client("secretsmanager", region_name="us-east-1")
    return json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])
