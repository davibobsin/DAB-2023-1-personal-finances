CREATE TABLE account_entries (
    timestamp    TIMESTAMP,
    value        INTEGER,
    way          VARCHAR(255),
    source       VARCHAR(255),
    account      VARCHAR(255),
    description  TEXT,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE card_entries (
    timestamp    TIMESTAMP,
    value        INTEGER,
    label        VARCHAR(255),
    description  TEXT,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE errors (
    timestamp   TIMESTAMP,
    step        TEXT,
    description TEXT,
    metadata    TEXT,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE frauds (
    policy        TEXT,
    source_events TEXT[],
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
