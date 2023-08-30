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

    # Dictionary zur Speicherung von Regex-Patterns für jedes Schlüsselwort
    regexes = {keyword: re.compile(r'(?:^|\W)' + re.escape(keyword.lower()) + r'(?:$|\W)', re.IGNORECASE) for keyword in keywords}

    written_bodies = BloomFilter(capacity=1000000, error_rate=0.1)

    with open(output_file_path, 'w', newline='') as output_file:
        csv_writer = csv.writer(output_file)
        csv_writer.writerow(["created_utc", "body", "score", "ups", "author", "gilded"])  # Header-Zeile hinzufügen
        with open(file_path, 'rb') as compressed_file:
            dctx = zstd.ZstdDecompressor(max_window_size=2147483648)
            reader = dctx.stream_reader(compressed_file)
            chunk_size = 500 * 1024 * 1024
            current_line = b''

            def line_generator():
                for chunk in iter(lambda: reader.read(chunk_size), b''):
                    nonlocal current_line
                    lines = (current_line + chunk).split(b'\n')
                    for line in lines[:-1]:
                        yield line
                    current_line = lines[-1]
                if current_line:
                    yield current_line

            counters = {keyword: 0 for keyword in keywords}
            for line in line_generator():
                decoded_line = line.decode('utf-8')
                decoded_line = json.loads(decoded_line)
                body = decoded_line.get("body", "").lower()
                score = decoded_line.get("score", "")
                ups = decoded_line.get("ups", "")
                created_utc = decoded_line.get("created_utc", "")
                author = decoded_line.get("author", "")
                gilded = decoded_line.get("gilded", "")

                for keyword, regex in regexes.items():
                    match = regex.search(body)
                    if match is not None and body not in written_bodies:
                        csv_writer.writerow([created_utc, body, score, ups, author, gilded])
                        written_bodies.add(body)
                        counters[keyword] += 1

            print(f"Anzahl der Zeilen, die das Schlüsselwort enthalten für Datei {file_path}:", counters)
