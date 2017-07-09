# GoogleBusinessScraper
Simple Python repo for scraping bussiness links from Google

Quick scraping script for Nigerian businesses. Uses `BeautifulSoup4` for website parsing and multi-threading to speed up.

Requires: `Python2.7`, `threading`, `requests`,`BeautifulSoup4`, `urllib2`

To run, clone repo and then `python scrape.py`. Will generate a new file, `scraped.csv` as a *tab*-separated file with top 3 URLs per business.

