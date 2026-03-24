"""
ach_disbursement.py
Generates NACHA-format ACH batch files for provider payment disbursements.
Submits via SFTP to FIS Global payment processor.
Cutoff: 17:00 EST daily — files submitted after cutoff processed next business day.
"""
import boto3
import paramiko
import logging
import json
from datetime import datetime
from decimal import Decimal
from typing import List
import gnupg

logger = logging.getLogger(__name__)

NACHA_COMPANY_ID     = "1234567890"
NACHA_COMPANY_NAME   = "INSURER HEALTH PLAN"
FIS_SFTP_HOST        = "sftp.fis.com"
FIS_SFTP_PORT        = 22
FIS_SFTP_PATH        = "/insurer/claims/incoming/"
S3_OUTBOUND_BUCKET   = "insurer-payments-outbound"
ACH_CUTOFF_HOUR_EST  = 17


def generate_ach_batch(approved_claims: List[dict], batch_date: str) -> str:
    """
    Generate NACHA ACH file content for a list of approved claims.
    Groups by provider NPI — one ACH entry per provider (aggregated payment).
    Returns NACHA-formatted string.
    """
    # Aggregate by provider NPI
    provider_totals = {}
    for claim in approved_claims:
        npi = claim["provider_npi"]
        if npi not in provider_totals:
            provider_totals[npi] = {
                "provider_npi":    npi,
                "routing_number":  claim["provider_routing_number"],
                "account_number":  claim["provider_account_number"],
                "total_amount":    Decimal("0"),
                "claim_ids":       [],
            }
        provider_totals[npi]["total_amount"] += Decimal(str(claim["final_payment_amount_usd"]))
        provider_totals[npi]["claim_ids"].append(claim["claim_id"])

    # Build NACHA file
    batch_date_fmt = datetime.strptime(batch_date, "%Y-%m-%d").strftime("%y%m%d")
    lines = []

    # File header
    lines.append(f"101 {NACHA_COMPANY_ID:<10}{NACHA_COMPANY_NAME:<23}{batch_date_fmt}00094101".ljust(94))

    # Batch header
    lines.append(f"5220{NACHA_COMPANY_NAME:<16}{NACHA_COMPANY_ID:<10}CLMPAY{batch_date_fmt}   1{NACHA_COMPANY_ID:<8}0000001".ljust(94))

    seq = 1
    total_debit = Decimal("0")
    for provider in provider_totals.values():
        amount_cents = int(provider["total_amount"] * 100)
        lines.append(
            f"622"
            f"{provider['routing_number'][:8]}"
            f"{provider['routing_number'][8]}"
            f"{provider['account_number']:<17}"
            f"{amount_cents:010d}"
            f"{'CLAIM PAYMENT':<15}"
            f"{'INSURER':<7}"
            f"0{NACHA_COMPANY_ID[:8]}{seq:07d}"
        )
        total_debit += provider["total_amount"]
        seq += 1

    # Batch control
    total_cents = int(total_debit * 100)
    lines.append(f"820000000{seq-1:06d}{total_cents:012d}{'0':012d}{NACHA_COMPANY_ID:<10}{' ':39}0000001".ljust(94))

    # File control
    lines.append(f"9000001000001{' ':6}{seq-1:08d}{total_cents:012d}{'0':012d}{' ':39}".ljust(94))

    return "\n".join(lines)


def encrypt_and_upload_ach(ach_content: str, batch_date: str) -> str:
    """PGP encrypt ACH file and upload to S3, then SFTP to FIS."""
    gpg = gnupg.GPG()
    fis_pubkey = _get_secret("claims/fis/pgp_public_key")["public_key"]
    gpg.import_keys(fis_pubkey)

    encrypted = gpg.encrypt(ach_content, recipients=["payments@fis.com"])
    if not encrypted.ok:
        raise ValueError(f"PGP encryption failed: {encrypted.status}")

    # Upload to S3
    filename = f"INSURER_ACH_{batch_date.replace('-','')}.ach.pgp"
    s3_key   = f"ach/{batch_date}/{filename}"
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.put_object(
        Bucket=S3_OUTBOUND_BUCKET, Key=s3_key,
        Body=str(encrypted).encode(), ContentType="application/octet-stream",
        ServerSideEncryption="AES256",
    )
    logger.info(f"ACH file uploaded to s3://{S3_OUTBOUND_BUCKET}/{s3_key}")

    # SFTP to FIS
    _sftp_to_fis(str(encrypted).encode(), filename)
    return s3_key


def _sftp_to_fis(file_content: bytes, filename: str):
    """Transfer encrypted ACH file to FIS Global via SFTP."""
    secret = _get_secret("claims/fis/sftp_credentials")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(FIS_SFTP_HOST, port=FIS_SFTP_PORT,
                username=secret["username"], password=secret["password"])
    sftp = ssh.open_sftp()
    with sftp.open(f"{FIS_SFTP_PATH}{filename}", "wb") as f:
        f.write(file_content)
    sftp.close()
    ssh.close()
    logger.info(f"ACH file transferred to FIS: {FIS_SFTP_PATH}{filename}")


def _get_secret(secret_name: str) -> dict:
    client = boto3.client("secretsmanager", region_name="us-east-1")
    return json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])
