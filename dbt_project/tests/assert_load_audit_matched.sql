select *
from {{ ref('mart_load_audit') }}
where reconciliation_status <> 'MATCHED'