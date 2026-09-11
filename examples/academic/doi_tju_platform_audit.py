#!/usr/bin/env python3
"""Run a truthful Tianjin University Resource Access 2 browser audit."""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parent))

from platforms.base import Work
from platforms.publisher import publish_staged_pdf
from platforms.runtime_state import PlatformRuntimeState, save_runtime_state
from platforms.tju_audit import TJU_AUDIT_PLATFORMS, TJUAuditAdapter
from platforms.validation import sanitize_filename_doi


try:
    from platforms.content_filter import skip_row
except ImportError:
    from examples.academic.platforms.content_filter import skip_row


def collect_45_real_samples(input_dir: Path) -> Dict[str, List[Dict[str, Any]]]:
    """Collect at most one OA and two non-OA real rows for each platform."""
    prefix_map = {prefix: key for key, _, prefix, _ in TJU_AUDIT_PLATFORMS}
    selected = {key: {"oa": [], "non_oa": []} for key, _, _, _ in TJU_AUDIT_PLATFORMS}
    for csv_file in sorted(path for path in input_dir.glob("*.csv") if not path.name.startswith("._")):
        with csv_file.open(encoding="utf-8-sig", errors="replace") as handle:
            for row in csv.DictReader(handle):
                if skip_row(input_dir, row):
                    continue
                doi = (row.get("doi") or "").strip()
                if not doi:
                    continue
                is_oa = (row.get("is_oa") or "").strip().lower() in {"true", "1", "t"}
                for prefix, key in prefix_map.items():
                    if doi.lower().startswith(prefix + "/"):
                        bucket = "oa" if is_oa else "non_oa"
                        quota = 1 if is_oa else 2
                        if len(selected[key][bucket]) < quota:
                            selected[key][bucket].append({
                                "doi": doi,
                                "title": (row.get("title") or "Untitled").strip(),
                                "publisher": (row.get("publisher") or "").strip(),
                                "journal": (row.get("journal") or "").strip(),
                                "is_oa": is_oa,
                            })
                        break
    return {key: selected[key]["oa"] + selected[key]["non_oa"] for key, _, _, _ in TJU_AUDIT_PLATFORMS}


def runner_prefix(root: Path, override: Path | None) -> list[str]:
    if override:
        if not override.is_file():
            raise SystemExit(f"runner not found: {override}")
        return [str(override)]
    binary = root / "target" / "debug" / "drission-workflow"
    return [str(binary)] if binary.is_file() else ["cargo", "run", "-p", "workflow-cli", "--"]


def locate_artifact(root: Path, filename: str) -> Path | None:
    matches = [path for path in root.rglob(filename) if path.is_file()]
    return max(matches, key=lambda path: path.stat().st_mtime) if matches else None


def run_audit(
    target_dir: Path,
    output_samples: Path,
    output_manifest: Path,
    output_summary: Path,
    limit: int = 45,
    runner_override: Path | None = None,
    workflow_override: Path | None = None,
) -> None:
    input_dir = target_dir / "total_journals" if (target_dir / "total_journals").is_dir() else target_dir
    root = Path(__file__).resolve().parents[2]
    workflow = workflow_override or root / "examples/templates/doi-tju-platform-audit.yaml"
    if not workflow.is_file():
        raise SystemExit(f"workflow not found: {workflow}")

    samples_by_platform = collect_45_real_samples(input_dir)
    names = {key: name for key, name, _, _ in TJU_AUDIT_PLATFORMS}
    selected: list[tuple[str, str, Dict[str, Any]]] = []
    for key, _, _, _ in TJU_AUDIT_PLATFORMS:
        for item in samples_by_platform.get(key, []):
            if limit and len(selected) >= limit:
                break
            selected.append((key, names[key], item))

    output_samples.parent.mkdir(parents=True, exist_ok=True)
    output_samples.write_text(json.dumps(samples_by_platform, ensure_ascii=False, indent=2), encoding="utf-8")
    artifact_parent = target_dir / ".doi_download_tmp" / "tju_platform_audit"
    artifact_parent.mkdir(parents=True, exist_ok=True)
    # A unique evidence directory prevents an old PDF/screenshot from being
    # mistaken for output of the current calibration run.
    artifact_dir = Path(tempfile.mkdtemp(prefix="run_", dir=artifact_parent))
    adapter = TJUAuditAdapter()
    articles = []
    for key, _, item in selected:
        work = Work(doi=item["doi"], title=item["title"], publisher=item["publisher"], is_oa=item["is_oa"])
        resolved = adapter.resolve(work)
        articles.append({
            "doi": item["doi"],
            "doi_stem": sanitize_filename_doi(item["doi"]).removesuffix(".pdf"),
            "filename": sanitize_filename_doi(item["doi"]),
            "platform": key,
            "url": resolved.landing_url,
        })

    runtime = PlatformRuntimeState(platform_id="tju_audit", state="running", reason="browser calibration")
    save_runtime_state(target_dir, runtime)
    started = time.monotonic()
    input_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", encoding="utf-8", delete=False) as handle:
            json.dump({"articles": articles}, handle, ensure_ascii=False)
            input_path = Path(handle.name)
        command = runner_prefix(root, runner_override) + ["run", str(workflow), "--inputs", str(input_path), "--artifacts", str(artifact_dir)]
        browser_env = os.environ.copy()
        for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
            browser_env.pop(name, None)
        print(f"Starting truthful TJU browser audit: {len(selected)} DOI(s)", flush=True)
        returncode = subprocess.run(command, cwd=root, env=browser_env, check=False).returncode
    except Exception as error:
        runtime.state = "failed"
        runtime.reason = str(error)
        save_runtime_state(target_dir, runtime)
        raise
    finally:
        if input_path:
            input_path.unlink(missing_ok=True)

    batch_duration_ms = int((time.monotonic() - started) * 1000)
    records = []
    successes = 0
    quarantine_dir = target_dir / ".doi_download_state" / "quarantine"
    output_dir = target_dir / "oapdf"
    for key, platform_name, item in selected:
        filename = sanitize_filename_doi(item["doi"])
        staged = locate_artifact(artifact_dir, filename)
        partial = next(iter(artifact_dir.rglob(filename + ".crdownload")), None)
        screenshot_name = f"tju_{key}_{filename.removesuffix('.pdf')}.png"
        screenshot = locate_artifact(artifact_dir, screenshot_name)
        if staged:
            outcome = publish_staged_pdf(staged, output_dir, quarantine_dir, item["doi"], item["title"], "tju_audit")
            status = outcome.status
            validation = "valid_pdf" if outcome.published else "invalid_pdf"
            failure_class = outcome.failure_class
            pdf_triggered = True
            file_path = str(outcome.final_path) if outcome.final_path else None
            successes += int(outcome.published)
        else:
            status = "download_timeout" if partial else "no_pdf_produced"
            validation = "not_available"
            failure_class = "download_timeout" if partial else "navigation_failed"
            pdf_triggered = partial is not None
            file_path = None
        records.append({
            "platform": key,
            "platform_name": platform_name,
            **item,
            "access_route": "tju_resource_2",
            "landing_url": f"https://doi.org/{item['doi']}",
            "institution_identified": None,
            "fulltext_entitled": True if file_path else None,
            "status": status,
            "pdf_triggered": pdf_triggered,
            "validation": validation,
            "failure_class": failure_class,
            "file": file_path,
            "batch_duration_ms": batch_duration_ms,
            "duration_ms": None,
            "screenshot": str(screenshot) if screenshot else None,
            "workflow_exit_code": returncode,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        })

    with output_manifest.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    output_summary.write_text(
        "# 天津大学资源访问二真实浏览器审计\n\n"
        f"运行：{len(records)}；有效 PDF：{successes}；工作流退出码：{returncode}；批次耗时：{batch_duration_ms / 1000:.1f}s。\n\n"
        "机构识别和全文权限没有页面证据时记为未知，不再推测。逐条事实以 JSONL manifest 为准。\n",
        encoding="utf-8",
    )
    runtime.state = "stopped" if returncode == 0 else "failed"
    runtime.reason = f"attempts={len(records)}, valid_pdf={successes}, exit={returncode}"
    runtime.recent_attempts = len(records)
    runtime.recent_successes = successes
    runtime.last_success_at = datetime.now(timezone.utc).isoformat() if successes else None
    save_runtime_state(target_dir, runtime)
    print(f"TJU audit finished: valid_pdf={successes}/{len(records)}; manifest={output_manifest}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TJU Resource Access 2 browser auditor")
    parser.add_argument("target", nargs="?", default="/Volumes/PortableSSD/doi撞库202608028")
    parser.add_argument("--limit", type=int, default=45, help="maximum samples; 0 means all sampled rows")
    parser.add_argument("--runner", type=Path)
    parser.add_argument("--workflow", type=Path)
    args = parser.parse_args()
    if args.limit < 0:
        parser.error("--limit must be >= 0")
    return args


if __name__ == "__main__":
    args = parse_args()
    target = Path(args.target).expanduser().resolve()
    state_dir = target / ".doi_download_state"
    run_audit(
        target,
        state_dir / "tju_audit_samples.json",
        state_dir / "tju_platform_audit_manifest.jsonl",
        state_dir / "tju_platform_audit_summary.md",
        args.limit,
        args.runner.expanduser().resolve() if args.runner else None,
        args.workflow.expanduser().resolve() if args.workflow else None,
    )
