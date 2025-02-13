CREATE TABLE telegram.message_url_links(
    linking_channel_id BIGINT NOT NULL,
    linked_channel_id BIGINT,
    linked_channel_username VARCHAR(255),
    nr_messages INT,
    first_message_date TIMESTAMP,
    last_message_date TIMESTAMP
);
