ALTER TABLE runs ADD COLUMN idempotency_key VARCHAR(255);
CREATE UNIQUE INDEX idx_runs_tenant_idempotency ON runs (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
