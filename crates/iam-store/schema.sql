CREATE TABLE IF NOT EXISTS store_control (
    singleton TINYINT UNSIGNED NOT NULL,
    store_id BINARY(16) NOT NULL,
    token_key BINARY(32) NOT NULL,
    max_observed_now_ms BIGINT NOT NULL,
    closed_through_client_ms BIGINT NOT NULL,
    friends_ready TINYINT UNSIGNED NOT NULL DEFAULT 0,
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

CREATE TABLE IF NOT EXISTS direct_pairs (
    tenant_id BIGINT UNSIGNED NOT NULL,
    user_low BIGINT UNSIGNED NOT NULL,
    user_high BIGINT UNSIGNED NOT NULL,
    conversation_id BIGINT UNSIGNED NOT NULL,
    friends BOOLEAN NOT NULL,
    PRIMARY KEY (tenant_id, user_low, user_high),
    UNIQUE KEY direct_conversation (tenant_id, conversation_id)
);

CREATE TABLE IF NOT EXISTS friend_requests (
    tenant_id BIGINT UNSIGNED NOT NULL,
    to_user_id BIGINT UNSIGNED NOT NULL,
    from_user_id BIGINT UNSIGNED NOT NULL,
    pending BOOLEAN NOT NULL,
    PRIMARY KEY (tenant_id, to_user_id, from_user_id),
    KEY pending_incoming (tenant_id, to_user_id, pending, from_user_id)
);

CREATE TABLE IF NOT EXISTS friend_attempts (
    tenant_id BIGINT UNSIGNED NOT NULL,
    to_user_id BIGINT UNSIGNED NOT NULL,
    from_user_id BIGINT UNSIGNED NOT NULL,
    request_id BINARY(16) NOT NULL,
    outcome TINYINT UNSIGNED NOT NULL,
    conversation_id BIGINT UNSIGNED NULL,
    legacy_slot TINYINT UNSIGNED NULL,
    pending_slot TINYINT GENERATED ALWAYS AS (IF(outcome = 1, 1, NULL)) VIRTUAL,
    PRIMARY KEY (tenant_id, to_user_id, from_user_id, request_id),
    UNIQUE KEY sender_request (tenant_id, from_user_id, request_id),
    UNIQUE KEY pending_attempt (tenant_id, to_user_id, pending_slot, from_user_id),
    UNIQUE KEY legacy_attempt (tenant_id, to_user_id, from_user_id, legacy_slot),
    KEY legacy_inbox (tenant_id, to_user_id, legacy_slot, pending_slot, from_user_id)
);
