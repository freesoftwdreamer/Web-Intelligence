from crawl4ai import WebCrawler

# Create an instance of WebCrawler
crawler = WebCrawler()

# Warm up the crawler (load necessary models)
crawler.warmup()

# Run the crawler on a URL
result = crawler.run(url="https://openai.com/api/pricing/")

# Print the extracted content in Markdown format
print(result.markdown)
