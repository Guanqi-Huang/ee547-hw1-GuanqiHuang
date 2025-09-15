"""
Build a containerized application that fetches paper metadata from the ArXiv API, processes it, 
and generates structured output.
"""

import sys
import os
import json
import re
import time
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import URLError, HTTPError
import xml.etree.ElementTree as ET

ARXIV_ENDPOINT = "http://export.arxiv.org/api/query"
STOPWORDS = {'the','a','an','and','or','but','in','on','at','to','for','of','with','by','from','up','about','into','through',
             'during','is','are','was','were','be','been','being','have','has','had','do','does','did','will','would','could',
             'should','may','might','can','this','that','these','those','i','you','he','she','it','we','they','what','which',
             'who','when','where','why','how','all','each','every','both','few','more','most','other','some','such','as','also',
             'very','too','only','so','than','not'}

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def log_line(path, msg):
    #To make the log processed with one line per event
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{utc_now()} {msg}\n")

#Query the ArXiv API using the search query
def fetch_arxiv_xml(query, max_results, log_path, retries=3, wait=3):
    params = {
        "search_query": query,
        "start": 0,
        "max_results": max_results
    }
    url = f"{ARXIV_ENDPOINT}?{urlencode(params)}"
    req = Request(url, headers={"User-Agent": "EE547-HW01/1.0"})
    for attempt in range(1, retries+1):
        try:
            with urlopen(req, timeout=15) as resp:
                data = resp.read()
            # Respect arXiv polite rate limiting
            time.sleep(3)
            return data
        except HTTPError as e:
            # Rate limiting : If you receive HTTP 429, wait 3 seconds and retry (maximum 3 attempts)
            if e.code == 429 and attempt < retries:
                log_line(log_path, f"[WARN] HTTP 429 received; retrying after {wait}s (attempt {attempt})")
                time.sleep(wait)
                continue
            log_line(log_path, f"[ERROR] HTTPError {e.code}: {e.reason}")
            raise
        except URLError as e:
            log_line(log_path, f"[ERROR] URLError: {e.reason}")
            raise
        except Exception as e:
            log_line(log_path, f"[ERROR] Unknown error: {e}")
            raise

# Extract and process metadata for each paper
def parse_feed(xml_bytes, log_path):
    ns = {
        'a': 'http://www.w3.org/2005/Atom',
        'arxiv': 'http://arxiv.org/schemas/atom'
    }
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        log_line(log_path, f"[ERROR] Invalid XML: {e}")
        return []

    entries = []
    for entry in root.findall('a:entry', ns):
        try:
            full_id = (entry.findtext('a:id', default='', namespaces=ns) or '').strip()
            arxiv_id = full_id.rsplit('/', 1)[-1] if full_id else ''
            title = (entry.findtext('a:title', default='', namespaces=ns) or '').strip()
            abstract = (entry.findtext('a:summary', default='', namespaces=ns) or '').strip()
            published = (entry.findtext('a:published', default='', namespaces=ns) or '').strip()
            updated = (entry.findtext('a:updated', default='', namespaces=ns) or '').strip()
            authors = []
            for a_el in entry.findall('a:author', ns):
                name = a_el.findtext('a:name', default='', namespaces=ns)
                if name:
                    authors.append(name.strip())
            categories = []
            for c_el in entry.findall('a:category', ns):
                term = c_el.get('term')
                if term:
                    categories.append(term.strip())
            if not (arxiv_id and title and abstract):
		#Missing fields: If a paper lacks required fields, skip it and log a warning
                log_line(log_path, f"[WARN] Missing required fields; skipping entry with id={arxiv_id!r}")
                continue
            entries.append({
                "arxiv_id": arxiv_id,
                "title": title,
                "authors": authors,
                "abstract": abstract,
                "categories": categories,
                "published": published,
                "updated": updated
            })
        except Exception as e:
           #Invalid XML: If the API returns malformed XML, log the error and continue with other papers
            log_line(log_path, f"[WARN] Failed to parse an entry: {e}")
            continue
    return entries

_word_re = re.compile(r"[A-Za-z0-9]+")
_sent_re = re.compile(r"[.!?]")

#Generate text analysis statistics
def analyze_abstract(text):
    # Case-insensitive stats; preserve original elsewhere
    words_orig = _word_re.findall(text)
    words_lower = [w.lower() for w in words_orig]
    word_count = len(words_lower)
    unique_count = len(set(words_lower))
    avg_word_len = (sum(len(w) for w in words_lower) / word_count) if word_count else 0.0

    # Sentences by simple split on . ! ?
    sentences = _sent_re.split(text)
    sentences = [s.strip() for s in sentences if s.strip()]
    sentence_count = len(sentences)
    words_per_sentence = [(len(_word_re.findall(s))) for s in sentences]
    avg_wps = (sum(words_per_sentence) / sentence_count) if sentence_count else 0.0

    # Longest/shortest sentence (by word count) – not stored in papers.json, but useful if needed
    # Extract technical terms
    uppercase_terms = sorted({w for w in set(words_orig) if any(c.isupper() for c in w)})
    numeric_terms = sorted({w for w in set(words_orig) if any(c.isdigit() for c in w)})
    hyphenated_terms = sorted(set(re.findall(r"\b\w+(?:-\w+)+\b", text)))

    # Top 20 frequent words excluding stopwords (case-insensitive but track the lower form)
    freq = {}
    for w in words_lower:
        if w in STOPWORDS:
            continue
        freq[w] = freq.get(w, 0) + 1
    top20 = sorted(freq.items(), key=lambda x: (-x[1], x[0]))[:20]

    return {
        "totals": {
            "total_words": word_count,
            "unique_words": unique_count,
            "total_sentences": sentence_count,
            "avg_words_per_sentence": avg_wps,
            "avg_word_length": avg_word_len
        },
        "top_words": top20,
        "uppercase_terms": uppercase_terms,
        "numeric_terms": numeric_terms,
        "hyphenated_terms": hyphenated_terms
    }

def main():
    if len(sys.argv) != 4:
        print("Usage: arxiv_processor.py <query> <max_results 1..100> <output_dir>", file=sys.stderr)
        sys.exit(2)

    query = sys.argv[1]
    try:
        max_results = int(sys.argv[2])
    except ValueError:
        print("Error: max_results must be an integer", file=sys.stderr)
        sys.exit(2)
    if not (1 <= max_results <= 100):
        print("Error: max_results must be between 1 and 100", file=sys.stderr)
        sys.exit(2)

    out_dir = sys.argv[3]
    os.makedirs(out_dir, exist_ok=True)
    papers_path = os.path.join(out_dir, "papers.json")
    corpus_path = os.path.join(out_dir, "corpus_analysis.json")
    log_path = os.path.join(out_dir, "processing.log")

    log_line(log_path, f"Starting ArXiv query: {query}")
    try:
        xml_bytes = fetch_arxiv_xml(query, max_results, log_path)
    except Exception:
        # Network errors: If the ArXiv API is unreachable, write error to log and exit with code 1
        sys.exit(1)

    entries = parse_feed(xml_bytes, log_path)
    log_line(log_path, f"Fetched {len(entries)} result entries after parsing")

    papers = []
    # Aggregate corpus stats
    total_words_all = 0
    unique_global = set()
    longest_abs = 0
    shortest_abs = None
    word_doc_counts = {}  # for top_50 doc frequency
    uppercase_terms_global = set()
    numeric_terms_global = set()
    hyphenated_terms_global = set()

    for e in entries:
        abs_text = e["abstract"]
        stats = analyze_abstract(abs_text)

        totals = stats["totals"]
        total_words_all += totals["total_words"]
        unique_global.update(w for w in _word_re.findall(abs_text.lower()))
        longest_abs = max(longest_abs, totals["total_words"])
        shortest_abs = totals["total_words"] if shortest_abs is None else min(shortest_abs, totals["total_words"])

        # Doc freq for top words
        seen_in_doc = set(w for w, _c in stats["top_words"])
        for w in seen_in_doc:
            word_doc_counts[w] = word_doc_counts.get(w, 0) + 1

        uppercase_terms_global.update(stats["uppercase_terms"])
        numeric_terms_global.update(stats["numeric_terms"])
        hyphenated_terms_global.update(stats["hyphenated_terms"])

        papers.append({
            "arxiv_id": e["arxiv_id"],
            "title": e["title"],
            "authors": e["authors"],
            "abstract": e["abstract"],
            "categories": e["categories"],
            "published": e["published"],
            "updated": e["updated"],
            "abstract_stats": {
                "total_words": totals["total_words"],
                "unique_words": totals["unique_words"],
                "total_sentences": totals["total_sentences"],
                "avg_words_per_sentence": totals["avg_words_per_sentence"],
                "avg_word_length": totals["avg_word_length"]
            }
        })

    # Top 50 words
    corpus_freq = {}
    for p in papers:
        for w in _word_re.findall(p["abstract"].lower()):
            if w in STOPWORDS:
                continue
            corpus_freq[w] = corpus_freq.get(w, 0) + 1
    top50 = sorted(corpus_freq.items(), key=lambda x: (-x[1], x[0]))[:50]
    top50_struct = [{"word": w, "frequency": f, "documents": word_doc_counts.get(w, 0)} for w, f in top50]

    corpus = {
        "query": query,
        "papers_processed": len(papers),
        "processing_timestamp": utc_now(),
        "corpus_stats": {
            "total_abstracts": len(papers),
            "total_words": total_words_all,
            "unique_words_global": len(unique_global),
            "avg_abstract_length": (total_words_all / len(papers)) if papers else 0.0,
            "longest_abstract_words": longest_abs,
            "shortest_abstract_words": (shortest_abs if shortest_abs is not None else 0)
        },
        "top_50_words": top50_struct,
        "technical_terms": {
            "uppercase_terms": sorted(uppercase_terms_global),
            "numeric_terms": sorted(numeric_terms_global),
            "hyphenated_terms": sorted(hyphenated_terms_global)
        },
        "category_distribution": {}
    }

    # Category distribution
    cat_counts = {}
    for p in papers:
        for c in p["categories"]:
            cat_counts[c] = cat_counts.get(c, 0) + 1
    corpus["category_distribution"] = dict(sorted(cat_counts.items()))

    #Structured outputs
    with open(papers_path, "w", encoding="utf-8") as f:
        json.dump(papers, f, ensure_ascii=False, indent=2)
    with open(corpus_path, "w", encoding="utf-8") as f:
        json.dump(corpus, f, ensure_ascii=False, indent=2)

    log_line(log_path, f"Completed processing: {len(papers)} papers")
    print(f"Wrote: {papers_path}")
    print(f"Wrote: {corpus_path}")
    print(f"Log:   {log_path}")

if __name__ == "__main__":
    main()
