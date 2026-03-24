# claim-payments-reconciliation

Processes approved and partial claim decisions into actual provider payments.
Disburses via ACH/EFT batch files. Reconciles against general ledger daily.

## Payment Flow
1. Read APPROVED/PARTIAL decisions from claims_adjudicated.fact_claim_adjudication
2. Calculate final payment per claim (after deductibles, copays, OON adjustments)
3. Group by provider NPI → generate ACH batch file
4. Submit ACH file to payment processor (FIS Global)
5. Receive payment confirmation → update claims_payments.fact_claim_payments
6. Reconcile daily payment totals against GL system (Oracle Financials)

## Tech Stack
- Orchestration: Airflow MWAA
- Processing: AWS Glue PySpark
- Payment files: S3 → SFTP to FIS Global (payment processor)
- Reconciliation: Glue job → Redshift claims_payments schema
- GL Integration: Oracle Financials REST API

## SLA
- ACH batch submitted to FIS by 17:00 EST daily
- Provider payments settled within 2 business days
- Reconciliation completed by 09:00 EST next business day

## Key Contacts
- Payments Owner: Marcus Webb (marcus.webb@insurer.com)
- Finance Liaison: Sandra Obi (sandra.obi@insurer.com) — GL Reconciliation
- FIS Support: fis-support@fis.com (reference: INSURER-ACH-4421)
- On-call: #claims-payments-alerts

## Compliance
- All payment records retained 7 years (SOX compliance)
- PII fields (member_id, SSN-derived keys) encrypted at rest (AES-256)
- ACH files encrypted with PGP before SFTP transfer
