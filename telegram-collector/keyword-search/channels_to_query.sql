CREATE TABLE channels_to_query(
    id INT PRIMARY KEY,
    access_hash BIGINT NOT NULL,
    data_owner VARCHAR(255),
    search_date TIMESTAMP,
    query_id VARCHAR(255),
    search_keyword VARCHAR(255),
    language_code VARCHAR(255),
    nr_forwarding_channels INT DEFAULT 0,
    distance_from_core INT DEFAULT 0,
    collection_priority INT
);