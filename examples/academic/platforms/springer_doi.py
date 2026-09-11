"""Resolve DOI aliases without following requests onto publisher websites."""
import json
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace


def canonical_doi(doi):
    current = doi.strip()
    class StopRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None
    opener = urllib.request.build_opener(StopRedirect)
    seen = set()
    for _ in range(8):
        if current.lower() in seen:
            raise ValueError('DOI redirect cycle')
        seen.add(current.lower())
        url = 'https://doi.org/' + urllib.parse.quote(current, safe='/():;._-')
        try:
            with opener.open(url, timeout=15):
                raise ValueError('DOI resolver did not redirect')
        except urllib.error.HTTPError as error:
            if error.code not in (301, 302, 303, 307, 308):
                raise
            target = urllib.parse.urlsplit(urllib.parse.urljoin(url, error.headers['Location']))
            error.close()
        if target.scheme not in ('http', 'https') or target.username or target.password:
            raise ValueError('Invalid DOI redirect')
        if target.hostname in ('doi.org', 'dx.doi.org'):
            candidate = urllib.parse.unquote(target.path.lstrip('/'))
            if not candidate.lower().startswith(doi.split('/')[0].lower() + '/'):
                raise ValueError('DOI redirect changed registrant')
            current = candidate
        elif target.hostname in ('link.springer.com', 'link.springer.nature.com'):
            path = urllib.parse.unquote(target.path)
            marker = '/' + current.split('/')[0] + '/'
            if marker.lower() in path.lower():
                candidate = path[path.lower().index(marker.lower()) + 1:]
                if candidate.lower() == current.lower():
                    current = candidate
            return current
        else:
            raise ValueError('DOI did not resolve to Springer')
    raise ValueError('Too many DOI redirects')


def resolve_aliases(works, cache_path):
    cache = {}
    if cache_path.exists():
        for line in cache_path.read_text().splitlines():
            try:
                row = json.loads(line)
                cache[row['source'].lower()] = row['canonical']
            except (ValueError, KeyError, TypeError):
                continue
    def resolve(work):
        source = work.doi.strip().lower()
        try:
            canonical = cache.get(source) or canonical_doi(work.doi)
            return replace(work, doi=canonical, extra={**work.extra, 'source_doi': work.doi}), None
        except (OSError, ValueError) as error:
            return work, type(error).__name__
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(resolve, works))
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open('a') as handle:
        for original, (resolved, error) in zip(works, results):
            if error is None and original.doi.lower() not in cache:
                handle.write(json.dumps({'source': original.doi, 'canonical': resolved.doi}) + '\n')
    return results
