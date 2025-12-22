import feedparser

feed = feedparser.parse("https://feeds.feedburner.com/ndtvmovies-latest")

for entry in feed.entries[:2]:
    print("Link:", entry.link)
    print("Title:", entry.title)
    if hasattr(entry, 'summary'):
        print("Summary length:", len(entry.summary))
    if hasattr(entry, 'content'):
        print("Content length:", len(entry.content[0].value))
    else:
        print("No content in RSS")