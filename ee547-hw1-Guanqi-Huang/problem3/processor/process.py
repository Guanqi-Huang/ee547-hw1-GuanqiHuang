"""
Extracts and analyzes text from HTML files.
"""
#!/usr/bin/env python3
import os
import re
import json
import time
from datetime import datetime, timezone

WORD_RE = re.compile(r"\b\w+\b")
SENT_RE = re.compile(r"[.!?]+")
P_TAG_RE = re.compile(r"<p\b", re.IGNORECASE)
def utc(): 
    return datetime.now(timezone.utc).isoformat()

def strip_html(html_content):
    """Remove HTML tags and extract text."""
    # Remove script and style elements
    html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    
    # Extract links before removing tags
    links = re.findall(r'href=[\'"]?([^\'" >]+)', html_content, flags=re.IGNORECASE)
    
    # Extract images
    images = re.findall(r'src=[\'"]?([^\'" >]+)', html_content, flags=re.IGNORECASE)
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', html_content)
    
    # Clean whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text, links, images

def main():
    os.makedirs("/shared/processed", exist_ok=True)
    os.makedirs("/shared/status", exist_ok=True)

    #Wait for /shared/status/fetch_complete.json
    status_file = "/shared/status/fetch_complete.json"
    while not os.path.exists(status_file):
        print("Waiting for fetch_complete.json...", flush=True)
        time.sleep(2)

    #Read all HTML files from /shared/raw/
    processed = []
    raw_dir = "/shared/raw"

    #Loop through HTML files produced by the fetcher
    for fname in sorted(os.listdir(raw_dir)):
        if not fname.endswith(".html"): continue
        path = os.path.join(raw_dir, fname)
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            html = f.read()

        text, links, images = strip_html(html)
	#Count words, sentences, paragraphs
        words = WORD_RE.findall(text)
        sentences = [s.strip() for s in SENT_RE.split(text) if s.strip()]
        para_count = len(P_TAG_RE.findall(html))
        if para_count == 0 and text: para_count = 1

        stats = {
            "word_count": len(words),
            "sentence_count": len(sentences),
            "paragraph_count": para_count,
            "avg_word_length": (sum(len(w) for w in words)/len(words)) if words else 0.0
        }
	#Output format
        out = {
            "source_file": fname,
            "text": text,
            "statistics": stats,
            "links": links,
            "images": images,
            "processed_at": utc()
        }

        #Tell analyzer the work is done and save processed data to /shared/processed/
        out_name = f"/shared/processed/{os.path.splitext(fname)[0]}.json"
        with open(out_name, "w", encoding="utf-8") as g:
            json.dump(out, g, ensure_ascii=False, indent=2)
        processed.append(out_name)

    with open("/shared/status/process_complete.json", "w", encoding="utf-8") as f:
        json.dump({"timestamp": utc(), "files": processed}, f, indent=2)

if __name__ == "__main__":
    main()
