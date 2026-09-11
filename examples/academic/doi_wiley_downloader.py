"""Wiley DOI candidates through the observed Tianjin Resource 2 proxy."""
import argparse
import csv
from pathlib import Path
from urllib.parse import quote
from doi_tju_med_downloader import run_tju_med_downloader, skip_row
from platforms.validation import sanitize_filename_doi


def proxy_url(doi):
    # Portal setProxyUrl(1, url): use the entry route, not its internal redirect.
    return 'https://p.lib.tju.edu.cn/-https://onlinelibrary.wiley.com/doi/' + quote(doi, safe='/')


def candidates(input_dir, existing):
    seen = set()
    for path in sorted(input_dir.glob('*.csv')):
        if path.name.startswith('._'):
            continue
        with path.open(encoding='utf-8-sig') as handle:
            for row in csv.DictReader(handle):
                doi = row.get('doi', '').strip()
                if not doi.lower().startswith(('10.1002/', '10.1111/')) or skip_row(input_dir, row):
                    continue
                filename = sanitize_filename_doi(doi).lower()
                if doi.lower() in seen or filename in existing:
                    continue
                seen.add(doi.lower())
                yield dict(doi=doi, title=row.get('title', ''), filename=filename, url=proxy_url(doi))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('target', type=Path)
    parser.add_argument('--limit', type=int, default=10)
    parser.add_argument('--batch-size', type=int, default=5)
    args = parser.parse_args()
    run_tju_med_downloader(args.target, args.limit, args.batch_size, platform_id='wiley',
                          candidates_factory=candidates, workflow_name='doi-wiley-tju-downloader.yaml')
