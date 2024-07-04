CREATE TABLE search_keywords(
    data_owner VARCHAR(255),
    created_at DATE,
    keyword_id VARCHAR(255),
    keyword VARCHAR(255),
    relevance_language VARCHAR(255),
    region_code VARCHAR(255),
    max_results INT,
    safe_search VARCHAR(255) 
);

insert into search_keywords VALUES ('FBK-YOUTUBE', '2024-07-01', '00', 'climate', 'en', 'gb', 50, 'none');