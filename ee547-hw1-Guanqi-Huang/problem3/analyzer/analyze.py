"""
Performs corpus-wide analysis
"""

#!/usr/bin/env python3
import os
import json
import  time
import itertools 
import re
from collections import Counter
from datetime import datetime, timezone

WORD_RE = re.compile(r"\b\w+\b")
def utc():
    return datetime.now(timezone.utc).isoformat()

def jaccard_similarity(doc1_words, doc2_words):
    """Calculate Jaccard similarity between two documents."""
    set1 = set(doc1_words)
    set2 = set(doc2_words)
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return len(intersection) / len(union) if union else 0.0

def ngrams(tokens, n):
    #Return n-grams from token list
    return [" ".join(tokens[i:i+n]) for i in range(len(tokens)-n+1)]

def main():
    os.makedirs("/shared/analysis", exist_ok=True)
    os.makedirs("/shared/status", exist_ok=True)
    #Wait for /shared/status/process_complete.json
    done = "/shared/status/process_complete.json"
    while not os.path.exists(done):
        print("Waiting for process_complete.json...", flush=True)
        time.sleep(2)
    #Read all processed files from /shared/processed/
    docs = []
    pdir = "/shared/processed"
    for fname in sorted(os.listdir(pdir)):
        if fname.endswith(".json"):
            with open(os.path.join(pdir, fname), "r", encoding="utf-8") as f:
                data = json.load(f)
                text = data.get("text","")
                toks = [w.lower() for w in WORD_RE.findall(text)]
                docs.append((fname, toks))

    total_words = sum(len(t) for _, t in docs)
    vocab = set(w for _, t in docs for w in t)
    freq = Counter(w for _, t in docs for w in t)
    #Word frequency distribution (top 100 words)
    top100 = [{"word": w, "count": c, "frequency": (c/total_words if total_words else 0.0)}
              for w, c in freq.most_common(100)]
    # Document similarity matrix (Jaccard similarity)
    sims = []
    for (i,(f1,t1)), (j,(f2,t2)) in itertools.combinations(enumerate(docs), 2):
        sims.append({"doc1": f1, "doc2": f2, "similarity": jaccard(t1,t2)})
    # N-gram extraction (bigrams and trigrams)
    bigrams = Counter(b for _, t in docs for b in ngrams(t,2)).most_common(50)
    trigrams = Counter(tr for _, t in docs for tr in ngrams(t,3)).most_common(50)
    sentence_est = max(1, total_words // 20) if total_words else 0
    avg_sentence_len = (total_words / sentence_est) if sentence_est else 0.0
    avg_word_len = (sum(len(w) for w in freq.elements())/total_words) if total_words else 0.0
    complexity = avg_sentence_len * avg_word_len
    #Final report structure
    report = {
        "processing_timestamp": utc(),
        "documents_processed": len(docs),
        "total_words": total_words,
        "unique_words": len(vocab),
        "top_100_words": top100,
        "document_similarity": sims,
        "top_bigrams": [{"bigram": b, "count": c} for b,c in bigrams],
        "top_trigrams": [{"trigram": tr, "count": c} for tr,c in trigrams],
        "readability": {
            "avg_sentence_length": avg_sentence_len,
            "avg_word_length": avg_word_len,
            "complexity_score": complexity
        }
    }

    with open("/shared/analysis/final_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
