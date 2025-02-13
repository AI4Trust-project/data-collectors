CREATE TABLE telegram.channels_to_requery(
    id BIGINT PRIMARY KEY,
    access_hash BIGINT NOT NULL,
    username VARCHAR(255),
    messages_last_queried_at TIMESTAMP,
    distance_from_core INT,
    collection_priority NUMERIC(10, 9)
);
