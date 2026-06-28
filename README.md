# ReelShort Universal Scraper

## Highlights:

- Scrapper utilizes HTTP proxies to prevent IP-based WAF restrictions
- All IO operations (HTTP requests, files reading/writing) is async, series parsed concurrently
- Activity consists of two phases: firstly, we parse all series short data from /tags/XYZ tabs; secondly, we parse full data from specific /movie/XYZ pages for each series
- All basic settings are stored in .env file
- Selenium/Playwright not needed, as soon as all data can be found in pages source code (SSR, Next.JS)
- All data parsed from `<script>` tag with id `__NEXT_DATA__` (default data placement for Next.JS SSR)
- As soon as data provided conveniently in JSON format in `__NEXT_DATA__`, bs4 actually not needed too, simple regexp would be enough
- Code is ruff-formatted and ruff-complaint (excluded rules specified in `pyproject.toml`)