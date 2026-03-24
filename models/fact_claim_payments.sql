-- fact_claim_payments.sql
-- Records all payment disbursements and their reconciliation status

{{ config(
    materialized = 'incremental',
    unique_key   = 'payment_id',
    schema       = 'claims_payments'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['fca.claim_id', 'p.payment_batch_date']) }} AS payment_id,
    fca.claim_id,
    fca.member_id,
    fca.provider_npi,
    fca.final_payment_amount_usd,
    fca.adjudication_decision,
    p.payment_batch_date,
    p.ach_batch_id,
    p.payment_status,                   -- SUBMITTED, SETTLED, REJECTED, REVERSED
    p.settlement_date,
    p.rejection_reason,
    r.gl_reconciliation_status,         -- MATCHED, UNMATCHED, PENDING
    r.gl_posting_date,
    r.variance_amount_usd,
    CURRENT_TIMESTAMP AS _loaded_at
FROM {{ ref('fact_claim_adjudication') }} fca
JOIN {{ source('payments', 'ach_payment_records') }} p
    ON fca.claim_id = p.claim_id
LEFT JOIN {{ source('payments', 'gl_reconciliation') }} r
    ON p.ach_batch_id = r.ach_batch_id
    AND fca.claim_id = r.claim_id

{% if is_incremental() %}
WHERE p.payment_batch_date > (SELECT MAX(payment_batch_date) FROM {{ this }})
{% endif %}
