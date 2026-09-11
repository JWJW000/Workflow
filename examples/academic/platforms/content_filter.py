"""Exclude publication notices without treating body/reference mentions as titles."""
from __future__ import annotations
import fcntl
import json
import re
from datetime import datetime, timezone
from pathlib import Path

NOTICE_TYPES = {'erratum','corrigendum','correction','retraction','retraction-notice','editorial','paratext','table-of-contents'}
TITLE_NOTICE = re.compile(r'^(?:(?:publisher|author)[’\']?s?\s+)?(?:errat(?:um|a)|corrigend(?:um|a))\b|^(?:(?:publisher|author)[’\']?s?\s+)?(?:correction|retraction|expression of concern)(?:\s*(?:[:：\[]|$)|\s+(?:to|notice|note)\b)|^(?:index|publication requirements and index|editorial policy and style information|advertisement|editorial|table of contents|front cover|back cover|blank page|contents)(?:\s*[:：]|\s*$)|^(?:勘误|更正声明|撤稿声明|撤稿通知|目录)(?:\s*[:：]|\s*$)', re.I)

def non_article_reason(title: str = '', document_type: str = '') -> str | None:
    kind = document_type.strip().lower().replace('_','-')
    if kind in NOTICE_TYPES:
        return f'Excluded publication type: {kind}'
    normalized = re.sub(r'\s+', ' ', title).strip()
    if TITLE_NOTICE.search(normalized):
        return 'Excluded publication notice title'
    return None

def pdf_notice_reason(text: str) -> str | None:
    # Inspect only the heading area, ending before abstract/body/references.
    for line in text.splitlines()[:16]:
        line=line.strip()
        if re.match(r'^(abstract|introduction|references|1[. ]+introduction)\b',line,re.I):
            break
        if non_article_reason(line):
            return 'PDF heading identifies a publication notice'
    return None

def exclusion_path(input_dir: Path) -> Path:
    root = input_dir.parent if input_dir.name == 'total_journals' else input_dir
    return root / '.doi_download_state' / 'non_article_manifest.jsonl'

_CACHE = {}
def excluded_dois(path: Path) -> set[str]:
    try: stamp=(path.stat().st_mtime_ns,path.stat().st_size)
    except FileNotFoundError:return set()
    if path not in _CACHE or _CACHE[path][0] != stamp:
        rows=set()
        for line in path.read_text(encoding='utf-8').splitlines():
            try:rows.add(json.loads(line)['doi'].lower())
            except (ValueError,KeyError,TypeError):continue
        _CACHE[path]=(stamp,rows)
    return _CACHE[path][1]

def record_exclusion(path: Path, doi: str, title: str, reason: str) -> None:
    if not doi:return
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('a+',encoding='utf-8') as f:
        fcntl.flock(f,fcntl.LOCK_EX)
        if doi.lower() in excluded_dois(path):return
        f.write(json.dumps({'doi':doi.lower(),'title':title,'status':'non_article','reason':reason,'recorded_at':datetime.now(timezone.utc).isoformat()},ensure_ascii=False)+'\n')
        f.flush()

def skip_row(input_dir: Path, row: dict) -> bool:
    path=exclusion_path(input_dir)
    doi=str(row.get('doi') or '').strip()
    if doi.lower() in excluded_dois(path):return True
    reason=non_article_reason(str(row.get('title') or ''),str(row.get('type') or row.get('document_type') or row.get('work_type') or ''))
    if reason:
        record_exclusion(path,doi,str(row.get('title') or ''),reason)
        return True
    return False
