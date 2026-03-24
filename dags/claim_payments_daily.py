"""
claim_payments_daily.py
Airflow DAG — daily claims payment disbursement and GL reconciliation pipeline.

Schedule:
  - Payment calculator + ACH batch: runs at 14:00 UTC (09:00 EST)
  - ACH must be submitted to FIS by 17:00 EST cutoff
  - GL reconciliation: runs at 14:00 UTC next business day

Pipeline Steps:
  1. wait_for_adjudicated_claims  — sensor checks for APPROVED/PARTIAL claims not yet paid
  2. calculate_payments           — Glue job: calculates final payment amounts per claim
  3. generate_ach_batch           — Python: generates NACHA ACH file grouped by provider NPI
  4. submit_ach_to_fis            — Python: PGP encrypt + SFTP to FIS Global
  5. record_payment_submission    — Python: writes payment batch records to Redshift
  6. wait_for_fis_confirmation    — sensor: polls S3 for FIS confirmation/rejection file
  7. process_fis_response         — Python: updates payment status from FIS response file
  8. run_gl_reconciliation        — Glue job: reconciles Redshift totals vs Oracle GL
  9. notify_finance_team          — Python: emails Sandra Obi with reconciliation summary

SLA: ACH submitted to FIS by 17:00 EST. Alerts sent to #claims-payments-alerts if missed.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sensors.sql import SqlSensor
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import boto3
import json
import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner":            "claims-payments",
    "retries":          2,
    "retry_delay":      timedelta(minutes=15),
    "email_on_failure": True,
    "email":            ["claims-payments-alerts@insurer.com", "marcus.webb@insurer.com"],
    "email_on_retry":   False,
}

S3_OUTBOUND_BUCKET  = "insurer-payments-outbound"
S3_INBOUND_BUCKET   = "insurer-payments-inbound"
REDSHIFT_CONN_ID    = "redshift_claims_prod"
AWS_CONN_ID         = "aws_claims_prod"


def calculate_payments_fn(**context):
    """
    Trigger Glue payment calculator job.
    Fetches APPROVED/PARTIAL claims not yet in a payment batch.
    Applies ROUND_HALF_EVEN rounding to match Oracle GL (fix for CPR-002).
    """
    from src.payments.payment_calculator import get_approved_claims, calculate_final_payment
    batch_date = context["ds"]
    claims = get_approved_claims(batch_date)

    if not claims:
        logger.info(f"No approved claims pending payment for {batch_date}")
        context["ti"].xcom_push(key="claim_count", value=0)
        return

    # Calculate final payment amounts
    for claim in claims:
        claim["final_payment_amount_usd"] = float(calculate_final_payment(claim))

    # Store in XCom for downstream tasks
    context["ti"].xcom_push(key="approved_claims", value=claims)
    context["ti"].xcom_push(key="claim_count", value=len(claims))
    logger.info(f"Calculated payments for {len(claims)} claims for batch {batch_date}")


def generate_ach_batch_fn(**context):
    """
    Generate NACHA-format ACH file from approved claims.
    Groups payments by provider NPI — one ACH entry per provider.
    """
    from src.payments.ach_disbursement import generate_ach_batch
    claims = context["ti"].xcom_pull(key="approved_claims", task_ids="calculate_payments")
    batch_date = context["ds"]

    if not claims:
        logger.info("No claims to process — skipping ACH generation")
        return

    ach_content = generate_ach_batch(claims, batch_date)
    context["ti"].xcom_push(key="ach_content", value=ach_content)
    context["ti"].xcom_push(key="batch_date", value=batch_date)
    logger.info(f"Generated ACH batch for {len(claims)} claims, {batch_date}")


def submit_ach_to_fis_fn(**context):
    """
    PGP-encrypt and SFTP the ACH file to FIS Global.
    Must complete before 17:00 EST cutoff.
    Raises ValueError if called after 17:00 EST — prevents next-day processing confusion.
    """
    from src.payments.ach_disbursement import encrypt_and_upload_ach
    from datetime import datetime
    import pytz

    est = pytz.timezone("US/Eastern")
    now_est = datetime.now(est)
    if now_est.hour >= 17:
        raise ValueError(
            f"ACH submission attempted after 17:00 EST cutoff ({now_est.strftime('%H:%M EST')}). "
            f"File will be held for next business day. Contact FIS: fis-support@fis.com ref INSURER-ACH-4421"
        )

    ach_content = context["ti"].xcom_pull(key="ach_content", task_ids="generate_ach_batch")
    batch_date  = context["ti"].xcom_pull(key="batch_date",  task_ids="generate_ach_batch")

    if not ach_content:
        logger.info("No ACH content — skipping FIS submission")
        return

    s3_key = encrypt_and_upload_ach(ach_content, batch_date)
    context["ti"].xcom_push(key="ach_s3_key", value=s3_key)
    logger.info(f"ACH file submitted to FIS. S3 key: {s3_key}")


def record_payment_submission_fn(**context):
    """
    Write payment batch records to Redshift claims_payments.fact_claim_payments.
    Status set to SUBMITTED — updated to SETTLED/REJECTED when FIS responds.
    """
    import psycopg2
    claims     = context["ti"].xcom_pull(key="approved_claims", task_ids="calculate_payments")
    batch_date = context["ds"]
    ach_batch_id = f"ACH_{batch_date.replace('-', '')}_{context['run_id'][:8]}"

    if not claims:
        return

    secret = _get_secret("claims/redshift/payments")
    conn = psycopg2.connect(
        host="claims-prod-cluster.us-east-1.redshift.amazonaws.com",
        port=5439, dbname="claimsdb",
        user="claims_payments", password=secret["password"], sslmode="require",
    )
    cur = conn.cursor()
    for claim in claims:
        cur.execute("""
            INSERT INTO claims_payments.fact_claim_payments
            (claim_id, member_id, provider_npi, final_payment_amount_usd,
             adjudication_decision, payment_batch_date, ach_batch_id, payment_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'SUBMITTED')
            ON CONFLICT (claim_id) DO NOTHING
        """, (
            claim["claim_id"], claim["member_id"], claim["provider_npi"],
            claim["final_payment_amount_usd"], claim["adjudication_decision"],
            batch_date, ach_batch_id,
        ))
    conn.commit()
    cur.close()
    conn.close()
    context["ti"].xcom_push(key="ach_batch_id", value=ach_batch_id)
    logger.info(f"Recorded {len(claims)} payment submissions for batch {ach_batch_id}")


def process_fis_response_fn(**context):
    """
    Parse FIS confirmation/rejection file from S3.
    Updates payment_status in Redshift to SETTLED or REJECTED.
    Known issue CPR-005: REJ-77 and REJ-82 rejection codes from 2 regional banks
    not handled — those claims fall through to manual processing queue.
    """
    import psycopg2
    batch_date   = context["ds"]
    ach_batch_id = context["ti"].xcom_pull(key="ach_batch_id", task_ids="record_payment_submission")

    s3_client = boto3.client("s3", region_name="us-east-1")

    # FIS returns confirmation or rejection file
    confirmation_key = f"confirmations/{batch_date}/ACH_{batch_date.replace('-','')}_CONFIRM.json"
    rejection_key    = f"rejections/{batch_date}/ACH_{batch_date.replace('-','')}_REJECT.json"

    secret = _get_secret("claims/redshift/payments")
    conn = psycopg2.connect(
        host="claims-prod-cluster.us-east-1.redshift.amazonaws.com",
        port=5439, dbname="claimsdb",
        user="claims_payments", password=secret["password"], sslmode="require",
    )
    cur = conn.cursor()

    # Process confirmations
    try:
        resp = s3_client.get_object(Bucket=S3_INBOUND_BUCKET, Key=confirmation_key)
        confirmations = json.loads(resp["Body"].read())
        for c in confirmations.get("settled_claims", []):
            cur.execute("""
                UPDATE claims_payments.fact_claim_payments
                SET payment_status = 'SETTLED', settlement_date = %s
                WHERE claim_id = %s AND ach_batch_id = %s
            """, (c.get("settlement_date"), c["claim_id"], ach_batch_id))
        logger.info(f"Processed {len(confirmations.get('settled_claims', []))} FIS confirmations")
    except s3_client.exceptions.NoSuchKey:
        logger.warning("No FIS confirmation file found — may be delayed")

    # Process rejections
    try:
        resp = s3_client.get_object(Bucket=S3_INBOUND_BUCKET, Key=rejection_key)
        rejections = json.loads(resp["Body"].read())
        for r in rejections.get("rejected_claims", []):
            reason = r.get("rejection_code", "UNKNOWN")
            # CPR-005: REJ-77 and REJ-82 not mapped — manual processing required
            if reason in ("REJ-77", "REJ-82"):
                logger.warning(f"Unmapped FIS rejection code {reason} for claim {r['claim_id']} — manual processing required")
            cur.execute("""
                UPDATE claims_payments.fact_claim_payments
                SET payment_status = 'REJECTED', rejection_reason = %s
                WHERE claim_id = %s AND ach_batch_id = %s
            """, (reason, r["claim_id"], ach_batch_id))
        logger.info(f"Processed {len(rejections.get('rejected_claims', []))} FIS rejections")
    except s3_client.exceptions.NoSuchKey:
        logger.info("No FIS rejection file — no rejections for this batch")

    conn.commit()
    cur.close()
    conn.close()


def notify_finance_team_fn(**context):
    """
    Send reconciliation summary email to Sandra Obi (Finance).
    Includes variance counts, total amounts, and link to report in Redshift.
    """
    batch_date   = context["ds"]
    claim_count  = context["ti"].xcom_pull(key="claim_count", task_ids="calculate_payments") or 0
    ach_batch_id = context["ti"].xcom_pull(key="ach_batch_id", task_ids="record_payment_submission") or "N/A"

    subject = f"Claims Payment Batch Complete — {batch_date} ({claim_count} claims)"
    body = f"""
    <h3>Daily Claims Payment Summary — {batch_date}</h3>
    <table border="1" cellpadding="5">
        <tr><td><b>Batch ID</b></td><td>{ach_batch_id}</td></tr>
        <tr><td><b>Claims Processed</b></td><td>{claim_count}</td></tr>
        <tr><td><b>Payment Date</b></td><td>{batch_date}</td></tr>
        <tr><td><b>GL Reconciliation</b></td><td>See claims_payments.gl_reconciliation_report in Redshift</td></tr>
    </table>
    <p>Please review any REVIEW or ALERT status variances in the reconciliation report.</p>
    <p>Contact: #claims-payments-alerts | marcus.webb@insurer.com</p>
    """
    send_email(
        to=["sandra.obi@insurer.com", "claims-payments-alerts@insurer.com"],
        subject=subject,
        html_content=body,
    )


def _get_secret(secret_name: str) -> dict:
    client = boto3.client("secretsmanager", region_name="us-east-1")
    return json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])


# ── DAG Definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="claim_payments_daily",
    default_args=default_args,
    schedule_interval="0 14 * * 1-5",   # 14:00 UTC = 09:00 EST, weekdays only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["claims", "payments", "reconciliation", "ach"],
    doc_md="""
    ## claim_payments_daily
    Daily pipeline to disburse approved insurance claim payments to providers
    and reconcile against Oracle Financials GL.

    **ACH cutoff:** 17:00 EST — file must be submitted to FIS before this time.
    **Contacts:** Marcus Webb (engineering), Sandra Obi (finance), #claims-payments-alerts
    **FIS Support:** fis-support@fis.com | ref INSURER-ACH-4421
    """,
) as dag:

    wait_for_adjudicated_claims = SqlSensor(
        task_id="wait_for_adjudicated_claims",
        conn_id=REDSHIFT_CONN_ID,
        sql="""
            SELECT COUNT(*)
            FROM claims_adjudicated.fact_claim_adjudication fca
            LEFT JOIN claims_payments.fact_claim_payments fcp
                ON fca.claim_id = fcp.claim_id
            WHERE fca.adjudication_decision IN ('APPROVED', 'PARTIAL')
            AND   fca.adjudicated_at::date = '{{ ds }}'
            AND   fcp.claim_id IS NULL
        """,
        mode="poke",
        poke_interval=300,  # check every 5 minutes
        timeout=7200,       # wait up to 2 hours
        doc_md="Waits for adjudicated claims from claim-adjudication-engine before processing payments.",
    )

    calculate_payments = PythonOperator(
        task_id="calculate_payments",
        python_callable=calculate_payments_fn,
        doc_md="Calculates final payment amounts. Uses ROUND_HALF_EVEN to match Oracle GL (CPR-002 fix).",
    )

    generate_ach_batch = PythonOperator(
        task_id="generate_ach_batch",
        python_callable=generate_ach_batch_fn,
        doc_md="Generates NACHA ACH file grouped by provider NPI.",
    )

    submit_ach_to_fis = PythonOperator(
        task_id="submit_ach_to_fis",
        python_callable=submit_ach_to_fis_fn,
        doc_md="PGP encrypts and SFTPs ACH file to FIS Global. Fails if called after 17:00 EST cutoff.",
    )

    record_payment_submission = PythonOperator(
        task_id="record_payment_submission",
        python_callable=record_payment_submission_fn,
        doc_md="Writes SUBMITTED payment records to Redshift claims_payments.fact_claim_payments.",
    )

    wait_for_fis_confirmation = S3KeySensor(
        task_id="wait_for_fis_confirmation",
        bucket_name=S3_INBOUND_BUCKET,
        bucket_key="confirmations/{{ ds }}/ACH_{{ ds_nodash }}_CONFIRM.json",
        aws_conn_id=AWS_CONN_ID,
        mode="poke",
        poke_interval=600,   # check every 10 minutes
        timeout=43200,       # wait up to 12 hours for FIS response
        soft_fail=True,      # don't fail DAG if FIS file delayed
        doc_md="Waits for FIS Global confirmation file in S3 inbound bucket.",
    )

    process_fis_response = PythonOperator(
        task_id="process_fis_response",
        python_callable=process_fis_response_fn,
        doc_md="Updates payment status from FIS response. Known issue CPR-005: REJ-77/REJ-82 require manual handling.",
    )

    run_gl_reconciliation = GlueJobOperator(
        task_id="run_gl_reconciliation",
        job_name="payment-reconciliation-job",
        script_location="s3://insurer-glue-scripts/payment_reconciliation_job.py",
        aws_conn_id=AWS_CONN_ID,
        script_args={"--payment_date": "{{ ds }}"},
        doc_md="Reconciles Redshift payment totals against Oracle Financials GL. Known issue CPR-004: rate limit on high-volume days.",
    )

    notify_finance_team = PythonOperator(
        task_id="notify_finance_team",
        python_callable=notify_finance_team_fn,
        trigger_rule="all_done",  # notify even if reconciliation has warnings
        doc_md="Emails Sandra Obi (Finance) with batch summary and reconciliation results.",
    )

    # ── Task dependencies ──────────────────────────────────────────────────────
    (
        wait_for_adjudicated_claims
        >> calculate_payments
        >> generate_ach_batch
        >> submit_ach_to_fis
        >> record_payment_submission
        >> wait_for_fis_confirmation
        >> process_fis_response
        >> run_gl_reconciliation
        >> notify_finance_team
    )
