import os
import zstandard as zstd
import json
import csv
import re
from multiprocessing import Pool
from pybloom_live import BloomFilter


def extract_zst_file(file_path, output_directory, keywords):
    output_file_path = os.path.join(output_directory, os.path.basename(file_path).replace('.zst', '.csv'))
    print(f'Processing {file_path}...')

    # Erstellen Sie ein großes Suchmuster, indem Sie alle Schlüsselwörter durch "|" (OR) verbinden
    # Fügen Sie Wortgrenzen um jedes Schlüsselwort hinzu
    pattern = r'\b(?:' + '|'.join(map(re.escape, keywords)) + r')\b'
    regex = re.compile(pattern, re.IGNORECASE)
    written_titles = BloomFilter(capacity=1000000, error_rate=0.1)

    with open(output_file_path, 'w', newline='') as output_file:
        csv_writer = csv.writer(output_file)
        csv_writer.writerow(["created_utc", "title", "selftext", "score", "num_comments", "author", "subreddit"])  # Header-Zeile hinzufügen
        with open(file_path, 'rb') as compressed_file:
            dctx = zstd.ZstdDecompressor(max_window_size=2147483648)
            reader = dctx.stream_reader(compressed_file)
            chunk_size = 500 * 1024 * 1024
            current_line = [b'']
            counters = {keyword: 0 for keyword in keywords}

            def line_generator():
                for chunk in iter(lambda: reader.read(chunk_size), b''):
                    lines = (current_line[0] + chunk).split(b'\n')
                    for line in lines[:-1]:
                        yield line
                    current_line[0] = lines[-1]
                if current_line[0]:
                    yield current_line[0]

            for line in line_generator():
                decoded_line = line.decode('utf-8')
                decoded_line = json.loads(decoded_line)
                title = decoded_line.get("title", "").lower()
                selftext = decoded_line.get("selftext", "")
                score = decoded_line.get("score", "")
                num_comments = decoded_line.get("num_comments", "")
                created_utc = decoded_line.get("created_utc", "")
                author = decoded_line.get("author", "")
                subreddit = decoded_line.get("subreddit", "")

                match = regex.search(title)
                if match is not None and title not in written_titles:
                    csv_writer.writerow([created_utc, title, selftext, score, num_comments, author, subreddit])
                    written_titles.add(title)
                    if match.group() in counters:
                        counters[match.group()] += 1

            print(f"Anzahl der Zeilen, die das Schlüsselwort enthalten für Datei {file_path}:", counters)
