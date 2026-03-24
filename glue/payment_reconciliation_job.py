"""
payment_reconciliation_job.py
AWS Glue PySpark job — runs daily reconciliation and writes variance
report to Redshift claims_payments.gl_reconciliation_report.
Scheduled via MWAA DAG: claim_payments_daily, task: run_gl_reconciliation.
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from src.reconciliation.ledger_reconcile import reconcile
import psycopg2
import json
import boto3
from datetime import datetime, timedelta

args = getResolvedOptions(sys.argv, ["JOB_NAME", "payment_date"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

PAYMENT_DATE = args.get("payment_date", (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d"))


def write_variance_report(variances: list, payment_date: str):
    """Write reconciliation results to Redshift."""
    if not variances:
        print(f"No variances for {payment_date} — all payments reconciled.")
        return

    secret = json.loads(
        boto3.client("secretsmanager", region_name="us-east-1")
        .get_secret_value(SecretId="claims/redshift/payments")["SecretString"]
    )
    conn = psycopg2.connect(
        host="claims-prod-cluster.us-east-1.redshift.amazonaws.com",
        port=5439, dbname="claimsdb",
        user="claims_payments_recon",
        password=secret["password"], sslmode="require",
    )
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM claims_payments.gl_reconciliation_report
        WHERE payment_date = %s
    """, (payment_date,))

    for v in variances:
        cur.execute("""
            INSERT INTO claims_payments.gl_reconciliation_report
            (provider_npi, redshift_amount, oracle_gl_amount, variance_usd, status, payment_date, reconciled_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            v["provider_npi"], v["redshift_amount"], v["oracle_gl_amount"],
            v["variance_usd"], v["status"], v["payment_date"], datetime.utcnow(),
        ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"Wrote {len(variances)} variance records for {payment_date}")


variances = reconcile(PAYMENT_DATE, verbose=True)
write_variance_report(variances, PAYMENT_DATE)
job.commit()
