CREATE TABLE youtube_video (
  collection_id VARCHAR(255) PRIMARY KEY,
  virality_metric REAL,
  normalised_subscribers  REAL,
  data_owner VARCHAR(255) NOT NULL,
  created_at TIMESTAMP NOT NULL,
  last_update TIMESTAMP NOT NULL,
  producer VARCHAR(255) NOT NULL,
  video_id VARCHAR(255) NOT NULL,
  keyword_id VARCHAR(255) NOT NULL,
  keyword VARCHAR(255) NOT NULL,
  relevance_language VARCHAR(255) NOT NULL,
  region_code VARCHAR(255) NOT NULL,
  comments_id VARCHAR(255),
  comments_path VARCHAR(255),
  metadata_id VARCHAR(255),
  metadata_path VARCHAR(255),
  thumbnails_id VARCHAR(255),
  thumbnails_path VARCHAR(255),
  videofile_id VARCHAR(255),
  videofile_path VARCHAR(255),
  transcript_id VARCHAR(255),
  transcript_path VARCHAR(255)
);