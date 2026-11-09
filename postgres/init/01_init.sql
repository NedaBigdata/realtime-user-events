CREATE TABLE IF NOT EXISTS users_raw (
    id          BIGSERIAL PRIMARY KEY,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source      TEXT NOT NULL DEFAULT 'randomuser.me',
    payload     JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_users_raw_fetched_at
    ON users_raw (fetched_at DESC);

CREATE TABLE IF NOT EXISTS users_processed_log (
    raw_id     BIGINT PRIMARY KEY REFERENCES users_raw(id) ON DELETE CASCADE,
    sent_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);