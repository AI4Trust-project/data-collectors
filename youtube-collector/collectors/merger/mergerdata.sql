CREATE TABLE youtube_video (
  virality_metric REAL,
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
  videofile_path VARCHAR(255)
);