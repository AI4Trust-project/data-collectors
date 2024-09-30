CREATE TABLE channels_to_query(
    id BIGINT PRIMARY KEY,
    access_hash BIGINT NOT NULL,
    username VARCHAR(255),
    data_owner VARCHAR(255),
    search_date TIMESTAMP,
    created_at TIMESTAMP,
    channel_last_queried_at TIMESTAMP,
    messages_last_queried_at TIMESTAMP,
    query_id VARCHAR(255),
    search_keyword VARCHAR(255),
    language_code VARCHAR(255),
    nr_participants INT,
    nr_messages INT,
    nr_forwarding_channels INT DEFAULT 0,
    nr_recommending_channels INT DEFAULT 0,
    nr_linking_channels INT DEFAULT 0,
    distance_from_core INT DEFAULT 0,
    collection_priority NUMERIC(7, 6)
);
