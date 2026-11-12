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
