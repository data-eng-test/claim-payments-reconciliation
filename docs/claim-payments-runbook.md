# claim-payments-reconciliation Runbook

## Daily ACH Batch Failure
Trigger: claim_payments_daily DAG fails on task submit_ach_batch

1. Check S3 for generated ACH file:
   aws s3 ls s3://insurer-payments-outbound/ach/{date}/
2. If file exists but SFTP failed: retry sftp_to_fis task manually
3. If file missing: check Glue job logs in CloudWatch
   Log group: /aws-glue/jobs/claim-payment-calculator
4. Contact FIS if cutoff time (17:00 EST) is at risk:
   fis-support@fis.com | phone: 1-800-FIS-HELP ref INSURER-ACH-4421
5. Notify Sandra Obi (Finance) if payment will be delayed

## Reconciliation Mismatch
Trigger: reconciliation_check task raises ValueError on amount mismatch

1. Run reconciliation report manually:
   python src/reconciliation/ledger_reconcile.py --date {date} --verbose
2. Check for late adjudication decisions (claims adjudicated after batch cutoff)
3. Check for FIS rejection file in s3://insurer-payments-inbound/rejections/
4. Common cause: OON adjustment rounding — see CLAIM-PAYMENTS-018 for fix
5. Escalate to Sandra Obi if mismatch > $10,000

## Provider Duplicate Payment Risk
If a claim_id appears twice in ACH batch:
1. STOP — do not submit ACH file
2. Run: SELECT claim_id, COUNT(*) FROM staging GROUP BY 1 HAVING COUNT(*) > 1
3. Root cause usually upstream duplicate in claim-adjudication-engine
4. Contact Nadia Torres (adjudication team) immediately
5. Document in JIRA under CPR project before resubmitting
