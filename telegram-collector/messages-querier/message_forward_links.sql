CREATE TABLE telegram_message_forward_links(
    linking_channel_id BIGINT NOT NULL,
    linked_channel_id BIGINT NOT NULL,
    nr_messages INT,
    first_message_date TIMESTAMP,
    last_message_date TIMESTAMP
);
