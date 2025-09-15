"""
Write a Python application that fetches data from HTTP endpoints, processes the responses, and outputs structured results.
"""

import sys
import os
import json
import re
import time 
import datetime
from urllib import request, error

TIMEOUT = 10
iso_utc = lambda: datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
is_text = lambda ct: (ct or "").lower().find("text") != -1
count_words = lambda s: len(re.findall(r"\b[0-9A-Za-z]+\b", s))
#Fetches data from URLs and computes statistics about the responses.
def fetch(url):
    t0 = time.perf_counter()
    status, body, ct, err = None, b"", None, None
    try:
	# HTTP GET request
        with request.urlopen(url, timeout=TIMEOUT) as r:
            status = r.getcode()
            ct = r.headers.get("Content-Type")
            body = r.read()
    except error.HTTPError as e:
	#Capture the HTTP status code
        status = e.code
        ct = getattr(e, "headers", {}).get("Content-Type") if hasattr(e, "headers") else None
        try: body = e.read() or b""
        except Exception: body = b""
        err = f"HTTP Error {e.code}: {e.reason}"
    except error.URLError as e:
        err = f"URL Error: {e.reason}"
    except Exception as e:
        err = f"Error: {e}"
    #Measure the response time in milliseconds
    rt_ms = (time.perf_counter() - t0) * 1000.0
    wc = None
    #Count the number of words in the response (for text responses only)
    if is_text(ct) and body:
        charset = "utf-8"
        if ct and "charset=" in ct:
            try: charset = ct.split("charset=")[-1].split(";")[0].strip()
            except Exception: pass
        try: txt = body.decode(charset, errors="replace")
        except Exception: txt = body.decode("utf-8", errors="replace")
        wc = count_words(txt)

    entry = {
        "url": url,
        "status_code": status,
        "response_time_ms": rt_ms,
        "content_length": len(body),
        "word_count": wc,
        "timestamp": iso_utc(),
        "error": err,
    }
    success = status is not None and 200 <= status < 300
    return entry, success

def main():
    if len(sys.argv) != 3:
        print("Usage: python fetch_and_process.py <input_urls_file> <output_dir>", file=sys.stderr)
        sys.exit(1)

    in_file, out_dir = sys.argv[1], sys.argv[2]
    os.makedirs(out_dir, exist_ok=True)

    try:
        with open(in_file, "r", encoding="utf-8") as f:
            urls = [ln.strip() for ln in f if ln.strip()]
    except Exception as e:
        print(f"Failed to read input file: {e}", file=sys.stderr); sys.exit(2)

    processing_start = iso_utc()
    responses = []
    status_dist = {}
    total_bytes = 0
    rtimes = []
    succ = 0
    fail = 0

    err_path = os.path.join(out_dir, "errors.log")
    err_f = open(err_path, "w", encoding="utf-8")

    for u in urls:
        entry, ok = fetch(u)
        responses.append(entry)
        rtimes.append(entry["response_time_ms"])
        total_bytes += entry["content_length"]

        sc = entry["status_code"]
        if sc is not None:
            key = str(sc); status_dist[key] = status_dist.get(key, 0) + 1

        if ok:
            succ += 1
        else:
            fail += 1
            if entry["error"]:
                err_f.write(f"[{iso_utc()}] [{u}]: {entry['error']}\n")

    err_f.close()

    summary = {
        "total_urls": len(urls),
        "successful_requests": succ,
        "failed_requests": fail,
        "average_response_time_ms": (sum(rtimes) / len(rtimes)) if rtimes else 0.0,
        "total_bytes_downloaded": total_bytes,
        "status_code_distribution": status_dist,
        "processing_start": processing_start,
        "processing_end": iso_utc(),
    }

    with open(os.path.join(out_dir, "responses.json"), "w", encoding="utf-8") as f:
        json.dump(responses, f, indent=2)
    with open(os.path.join(out_dir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

if __name__ == "__main__":
    main()
