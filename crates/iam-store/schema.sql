CREATE TABLE IF NOT EXISTS store_control (
    singleton TINYINT UNSIGNED NOT NULL,
    store_id BINARY(16) NOT NULL,
    token_key BINARY(32) NOT NULL,
    max_observed_now_ms BIGINT NOT NULL,
    closed_through_client_ms BIGINT NOT NULL,
    PRIMARY KEY (singleton)
);

CREATE TABLE IF NOT EXISTS conversations (
    tenant_id BIGINT UNSIGNED NOT NULL,
    conversation_id BIGINT UNSIGNED NOT NULL,
    history_id BINARY(16) NOT NULL,
    next_sequence BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (tenant_id, conversation_id)
);

CREATE TABLE IF NOT EXISTS messages (
    tenant_id BIGINT UNSIGNED NOT NULL,
    conversation_id BIGINT UNSIGNED NOT NULL,
    message_id BINARY(16) NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    device_id BINARY(16) NOT NULL,
    client_msg_id BINARY(16) NOT NULL,
    `sequence` BIGINT UNSIGNED NOT NULL,
    `text` MEDIUMBLOB NOT NULL,
    accepted_at BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, conversation_id, message_id),
    UNIQUE KEY message_order (tenant_id, conversation_id, `sequence`),
    UNIQUE KEY successful_send (tenant_id, user_id, device_id, conversation_id, client_msg_id)
);

CREATE TABLE IF NOT EXISTS rejections (
    tenant_id BIGINT UNSIGNED NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    device_id BINARY(16) NOT NULL,
    conversation_id BIGINT UNSIGNED NOT NULL,
    client_msg_id BINARY(16) NOT NULL,
    digest BINARY(32) NOT NULL,
    error_code INT NOT NULL,
    PRIMARY KEY (tenant_id, user_id, device_id, conversation_id, client_msg_id)
);

CREATE TABLE IF NOT EXISTS members (
    tenant_id BIGINT UNSIGNED NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    conversation_id BIGINT UNSIGNED NOT NULL,
    account_sequence BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (tenant_id, user_id, account_sequence),
    UNIQUE KEY membership (tenant_id, conversation_id, user_id)
);

CREATE TABLE IF NOT EXISTS account_events (
    tenant_id BIGINT UNSIGNED NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    account_sequence BIGINT UNSIGNED NOT NULL,
    conversation_id BIGINT UNSIGNED NOT NULL,
    history_id BINARY(16) NOT NULL,
    access_version BIGINT UNSIGNED NULL,
    PRIMARY KEY (tenant_id, user_id, account_sequence),
    KEY conversation_events (tenant_id, user_id, conversation_id, account_sequence)
);

CREATE TABLE IF NOT EXISTS conversation_retention (
    tenant_id BIGINT UNSIGNED NOT NULL,
    conversation_id BIGINT UNSIGNED NOT NULL,
    floor_sequence BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (tenant_id, conversation_id)
);
