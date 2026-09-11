"""DOI-bound IEEE article metadata extraction; never infer IDs from DOI suffixes."""
import json
import re
from html.parser import HTMLParser
from urllib.parse import unquote


def normalize_doi(value):
    return re.sub(r'^https?://(?:dx\.)?doi.org/', '', unquote(str(value)).strip(), flags=re.I).lower()


class Metadata(HTMLParser):
    def __init__(self):
        super().__init__(); self.meta={}; self.links=[]
    def handle_starttag(self, tag, attrs):
        attrs=dict(attrs)
        if tag=='meta':
            self.meta[(attrs.get('name') or attrs.get('property') or '').lower()]=attrs.get('content','')
        if tag=='link' and attrs.get('rel','').lower()=='canonical':
            self.links.append(attrs.get('href',''))


def article_urls(html, doi):
    parser=Metadata(); parser.feed(html)
    expected=normalize_doi(doi)
    reported=parser.meta.get('citation_doi') or parser.meta.get('dc.identifier') or parser.meta.get('prism.doi')
    json_dois=re.findall(r'"doi"\s*:\s*"([^"\n]+)"',html,re.I)
    if reported and normalize_doi(reported)!=expected:return []
    if not reported and not any(normalize_doi(value)==expected for value in json_dois):return []
    urls=parser.links+[parser.meta.get(key,'') for key in ('citation_pdf_url','citation_abstract_html_url','og:url')]
    # Accept structured IDs only in the same JSON object as the requested DOI.
    for chunk in re.findall(r'\{[^{}]{0,200000}\}',html):
        try: data=json.loads(chunk)
        except ValueError:continue
        if normalize_doi(data.get('doi',''))==expected:
            number=str(data.get('articleNumber') or data.get('arnumber') or '')
            if number.isdigit():urls.append('https://ieeexplore.ieee.org/document/'+number+'/')
    return [url for url in urls if url]
