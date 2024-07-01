CREATE TABLE searchKeywords(
    dataOwner VARCHAR(255),
    createdAt DATE,
    keywordId VARCHAR(255),
    keyword VARCHAR(255),
    relevanceLanguage VARCHAR(255),
    regionCode VARCHAR(255),
    maxResults INT,
    safeSearch VARCHAR(255) 
);

insert into searchKeywords VALUES ('FBK-YOUTUBE', '2024-07-01', '00', 'climate', 'en', 'gb', 50, 'none');