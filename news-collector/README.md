# News Articles Collection

![alt text](/imgs/news_diagram.png "Diagram of the News Collection")

## Pipeline Description

### Orchestrator
* Retrieves a list of keywords and configurations for targeted news searches.

### Fetcher
* Utilizes GDELT to search the internet for news articles matching the specified keywords and configurations.
* Stores links to identified articles as "fetched articles."

### Checker
* Examines the domain of each fetched article link.
* Checks for the presence of a robots.txt file.
* If a robots.txt file exists, determines if the website allows web scraping.

### Scraper (Conditional)
* Employs Newspaper3k to extract the plain text content from the article.
* Stores the scraped text as "news scraped."
