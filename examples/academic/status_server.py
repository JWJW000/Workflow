#!/usr/bin/env python3
"""Serve the DOI corpus dashboard and control platform download processes.

Usage:
  python3 examples/academic/status_server.py              # export doi_status.json
  python3 examples/academic/status_server.py --serve 8899 # run local DOI dashboard
"""

from __future__ import annotations

import argparse
import calendar
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
import csv
import heapq
import http.server
import json
import os
import re
import signal
import shlex
import socket
import socketserver
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse, parse_qs
from urllib.request import urlopen

try:
    from .platforms.chrome_session import browser_page_health, discover_debug_port, ensure_usable_page, open_start_page, prepare_task_browser_profile, prepare_pdf_download_profile, resolve_chrome_binary
    from .platforms.validation import sanitize_filename_doi
except ImportError:
    from platforms.chrome_session import browser_page_health, discover_debug_port, ensure_usable_page, open_start_page, prepare_task_browser_profile, prepare_pdf_download_profile, resolve_chrome_binary
    from platforms.validation import sanitize_filename_doi

DEFAULT_DOI_TARGET = Path(
    os.environ.get(
        "DRISSION_DOI_TARGET",
        "/Volumes/PortableSSD/doi撞库202608028" if sys.platform == "darwin" else "doi-data",
    )
)
_DOI_STATUS_CACHE: Dict[str, Any] = {}
_DOI_STATUS_LOCK = threading.Lock()


@dataclass(frozen=True)
class DownloadTask:
    id: str
    name: str
    script: str
    state_prefix: str
    access: str
    profile_marker: str
    note: str = ""
    enabled: bool = True
    maturity: str = "production"
    cooldown_seconds: int = 1800
    batch_size: int = 100


@dataclass
class BrowserGroup:
    id: str
    name: str
    profile: Path
    process: subprocess.Popen | None = None
    endpoint: str | None = None
    pid: int | None = None


MAX_ACTIVE_BROWSER_GROUPS = 0
CHROME_APP = Path(resolve_chrome_binary())


def task_readiness(root: Path, target: Path, task: DownloadTask) -> tuple[bool, str]:
    """Return whether a task can run against this deployment's real inputs."""
    if not target.is_dir():
        return False, f"DOI 数据目录不存在: {target}"
    input_dir = target / "total_journals" if (target / "total_journals").is_dir() else target
    if not any(p.is_file() and p.suffix.lower() == ".csv" and not p.name.startswith("._") for p in input_dir.iterdir()):
        return False, f"DOI 清单为空: {input_dir}"
    script = root / "examples" / "academic" / task.script
    if not script.is_file():
        return False, f"下载脚本不存在: {script}"
    return True, ""


def browser_group_id(task: DownloadTask) -> str | None:
    """Each browser task owns its profile, page and download directory."""
    return None if task.access.strip().lower() == "direct_http" else task.id


def get_download_tasks() -> List[DownloadTask]:
    """Dynamically load download tasks from platform_registry.json."""
    registry_file = Path(__file__).resolve().parent / "platform_registry.json"
    if registry_file.is_file():
        try:
            data = json.loads(registry_file.read_text(encoding="utf-8"))
            tasks = []
            script_map = {
                "ieee": "doi_ieee_downloader.py",
                "springer": "doi_springer_downloader.py",
                "nature": "doi_nature_downloader.py",
                "acs": "doi_acs_downloader.py",
                "aps": "doi_aps_downloader.py",
                "aaa": "doi_aaa_downloader.py",
                "elsevier": "doi_elsevier_downloader.py",
                "cambridge": "doi_cambridge_downloader.py",
                "rsc": "doi_rsc_downloader.py",
                "tju_audit": "doi_tju_platform_audit.py",
                "tju_med": "doi_tju_med_downloader.py",
                "oa_repository": "doi_oa_repository_downloader.py",
            }
            for p in data.get("platforms", []):
                pid = p["id"]
                script = script_map.get(pid, f"doi_{pid}_downloader.py")
                tasks.append(
                    DownloadTask(
                        id=pid,
                        name=p.get("label", pid),
                        script=script,
                        state_prefix=p.get("state_prefix", pid),
                        access=p.get("access_mode", "campus_vpn"),
                        profile_marker=p.get("profile_dir", ""),
                        note=p.get("note", ""),
                        enabled=bool(p.get("enabled", False)),
                        maturity=p.get("maturity", "experimental"),
                        cooldown_seconds=int(p.get("cooldown_seconds", 1800)),
                        batch_size=max(1, int(p.get("batch_size", 100))),
                    )
                )
            if tasks:
                return tasks
        except Exception:
            pass

    return [
        DownloadTask("ieee", "IEEE Xplore", "doi_ieee_downloader.py", "ieee_buaa_webvpn", "北航 WebVPN", "profiles/doi-ieee-campus"),
        DownloadTask("springer", "SpringerLink", "doi_springer_downloader.py", "springer_buaa_webvpn", "北航 WebVPN", "profiles/doi-springer-campus"),
        DownloadTask("nature", "Nature", "doi_nature_downloader.py", "nature_fudan_vpn", "复旦 EasyConnect", "profiles/doi-nature-fudan-campus"),
        DownloadTask("acs", "ACS Publications", "doi_acs_downloader.py", "acs_buaa_webvpn", "北航 WebVPN", "profiles/doi-acs-buaa-campus"),
        DownloadTask("aps", "APS Journals", "doi_aps_downloader.py", "aps_buaa_webvpn", "北航 WebVPN", "profiles/doi-aps-buaa-campus", "遇到 Error 1015 时先冷却，避免连续重试"),
        DownloadTask("aaa", "AAA Journals", "doi_aaa_downloader.py", "aaa_buaa_webvpn", "北航 WebVPN", "profiles/doi-aaa-buaa-campus"),
        DownloadTask("elsevier", "Elsevier / Cell", "doi_elsevier_downloader.py", "elsevier_buaa_webvpn", "北航 WebVPN", "profiles/doi-elsevier-buaa-campus", "当前出口被 ScienceDirect 拒绝，需人工处理", False, "blocked"),
        DownloadTask("cambridge", "Cambridge Core", "doi_cambridge_downloader.py", "cambridge_buaa_webvpn", "北航 WebVPN", "profiles/doi-cambridge-buaa-campus", "校准阶段", False, "calibrating"),
        DownloadTask("rsc", "RSC Publications", "doi_rsc_downloader.py", "rsc_buaa_webvpn", "北航 WebVPN", "profiles/doi-rsc-buaa-campus", "实验阶段", False, "experimental"),
    ]


DOWNLOAD_TASKS = tuple(get_download_tasks())


def _count_csv_records(csv_files: List[Path], target: Path, fingerprint: tuple) -> int:
    cache_path = target / ".doi_download_state" / "dashboard_input_cache.json"
    serial_fingerprint = [list(item) for item in fingerprint]
    try:
        cached = json.loads(cache_path.read_text(encoding="utf-8"))
        if cached.get("fingerprint") == serial_fingerprint:
            return int(cached["records"])
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        pass

    records = 0
    for path in csv_files:
        with path.open(encoding="utf-8-sig", errors="replace", newline="") as handle:
            reader = csv.reader(handle)
            next(reader, None)
            records += sum(1 for _ in reader)
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(
            json.dumps({"fingerprint": serial_fingerprint, "records": records}, ensure_ascii=False),
            encoding="utf-8",
        )
    except OSError:
        pass
    return records


def get_doi_dashboard_data(target: Path) -> Dict[str, Any]:
    """Return cached progress for the DOI CSV corpus and flat PDF archive."""
    target = target.expanduser().resolve()
    input_dir = target / "total_journals" if (target / "total_journals").is_dir() else target
    output_dir = target / "oapdf"
    csv_files = sorted(p for p in input_dir.glob("*.csv") if not p.name.startswith("._"))
    csv_fingerprint = tuple((p.name, p.stat().st_size, p.stat().st_mtime_ns) for p in csv_files)
    try:
        output_mtime = output_dir.stat().st_mtime_ns
    except OSError:
        output_mtime = 0
    fingerprint = (str(target), csv_fingerprint, output_mtime)

    with _DOI_STATUS_LOCK:
        cached = _DOI_STATUS_CACHE.get("value")
        if cached and cached["fingerprint"] == fingerprint and time.monotonic() - cached["created"] < 30:
            return cached["data"]

    input_records = _count_csv_records(csv_files, target, csv_fingerprint)
    month_files = None
    if re.fullmatch(r'\d{4}-\d{2}', target.name):
        month_files = set()
        for path in csv_files:
            with path.open(encoding='utf-8-sig', newline='') as handle:
                for row in csv.DictReader(handle):
                    doi = row.get('doi', '').strip().lower()
                    if doi and (not row.get('published') or row['published'].startswith(target.name)):
                        month_files.add(sanitize_filename_doi(doi))
        input_records = len(month_files)
    now = time.time()
    pdf_records = []
    invalid_count = 0
    total_bytes = 0
    named_by_doi = 0
    if output_dir.is_dir():
        for entry in os.scandir(output_dir):
            if not entry.name.lower().endswith(".pdf") or entry.name.startswith("._"):
                continue
            if month_files is not None and entry.name not in month_files:
                continue
            try:
                stat = entry.stat()
                valid = stat.st_size >= 1024
                # Previously observed HTML placeholders are small (notably 63 KB).
                # Avoid opening every multi-megabyte PDF on the external drive.
                if valid and stat.st_size <= 128 * 1024:
                    with open(entry.path, "rb") as handle:
                        valid = handle.read(4) == b"%PDF"
            except OSError:
                continue
            if not valid:
                invalid_count += 1
                continue
            total_bytes += stat.st_size
            stem = entry.name[:-4]
            named_by_doi += int(entry.name.lower().startswith("10.") and "_" in stem)
            pdf_records.append((stat.st_mtime, stat.st_size, entry.name))

    recent = heapq.nlargest(40, pdf_records)
    downloaded = len(pdf_records)
    data = {
        "status": "ok",
        "timestamp": int(now),
        "inputPath": str(input_dir),
        "outputPath": str(output_dir),
        "summary": {
            "csvFiles": len(csv_files),
            "inputRecords": input_records,
            "downloaded": downloaded,
            "remaining": max(0, input_records - downloaded),
            "completionRate": round(downloaded / input_records * 100, 3) if input_records else 0,
            "totalGb": round(total_bytes / (1024 ** 3), 2),
            "lastHour": sum(1 for mtime, _, _ in pdf_records if mtime >= now - 3600),
            "last24Hours": sum(1 for mtime, _, _ in pdf_records if mtime >= now - 86400),
            "invalidPdfs": invalid_count,
            "doiNamed": named_by_doi,
        },
        "recent": [
            {"name": name, "size": size, "mtime": int(mtime)}
            for mtime, size, name in recent
        ],
    }
    with _DOI_STATUS_LOCK:
        _DOI_STATUS_CACHE["value"] = {"fingerprint": fingerprint, "created": time.monotonic(), "data": data}
    return data


class TaskControlError(RuntimeError):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


class DownloadTaskManager:
    """Controls a fixed allow-list of local DOI download commands."""

    def __init__(self, root: Path, target: Path, max_parallel: int = MAX_ACTIVE_BROWSER_GROUPS):
        self.max_parallel = max(0, max_parallel)
        self._retry_timers: Dict[str, threading.Timer] = {}
        self._run_baseline: Dict[str, int] = {}
        self.root = root.resolve()
        self.target = target.expanduser().resolve()
        self._managed: Dict[str, subprocess.Popen] = {}
        self._desired: set[str] = set()
        self._single_month_tasks: set[str] = set()
        self._queued_limits: Dict[str, Optional[int]] = {}
        self._queue: deque[str] = deque()
        self._group_active: Dict[str, str] = {}
        self._started_at: Dict[str, float] = {}
        self._browser_recoveries: Dict[str, int] = {}
        self._browser_health: Dict[str, str] = {}
        self._active_limits: Dict[str, int] = {}
        self._browser_groups: Dict[str, BrowserGroup] = {
            task.id: BrowserGroup(task.id, task.name, self.root / ".drission-workflow" / "profiles" / f"chrome-task-{task.id}")
            for task in get_download_tasks() if browser_group_id(task)
        }
        self._lock = threading.RLock()
        self._adopt_shared_browsers()

    def _adopt_shared_browsers(self) -> None:
        """Reconnect after a dashboard restart without killing live sessions."""
        for group in self._browser_groups.values():
            port = discover_debug_port(group.profile)
            if port:
                group.endpoint = f"http://127.0.0.1:{port}"
                group.pid = self._browser_pid(group)

    def _read_outcome(self, task_id: str) -> dict:
        try:
            return json.loads((self.target / '.doi_download_state/task_outcomes' / f'{task_id}.json').read_text())
        except (OSError, ValueError):
            return {}

    def _save_outcome(self, task_id: str, state: str, reason: str, exit_code=None) -> None:
        folder = self.target / '.doi_download_state/task_outcomes'
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f'{task_id}.json'
        temp = path.with_suffix('.tmp')
        temp.write_text(json.dumps({'state':state, 'reason':reason, 'exit_code':exit_code,
                                   'finished_at':datetime.now().astimezone().isoformat()}, ensure_ascii=False))
        temp.replace(path)

    def _task(self, task_id: str) -> DownloadTask:
        for task in get_download_tasks():
            if task.id == task_id:
                return task
        raise TaskControlError(f"未知下载任务: {task_id}", 404)

    @staticmethod
    def _processes() -> List[Dict[str, Any]]:
        try:
            result = subprocess.run(
                ["ps", "-axo", "pid=,ppid=,etime=,command="],
                check=True,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except (OSError, subprocess.SubprocessError):
            return []
        records = []
        for line in result.stdout.splitlines():
            parts = line.strip().split(None, 3)
            if len(parts) != 4:
                continue
            try:
                records.append({"pid": int(parts[0]), "ppid": int(parts[1]), "elapsed": parts[2], "command": parts[3]})
            except ValueError:
                continue
        return records

    def _matching(self, task: DownloadTask, processes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        script_marker = f"examples/academic/{task.script}"
        matches = []
        for process in processes:
            if script_marker not in process['command']:
                continue
            try:
                args = shlex.split(process['command'])
            except ValueError:
                continue
            if any(arg == str(self.target) or (not arg.startswith('-') and (self.root / arg).resolve() == self.target) for arg in args[1:]):
                matches.append(process)
        return matches

    def _manifest_stats(self, task: DownloadTask) -> Dict[str, Any]:
        manifest = self.target / ".doi_download_state" / f"{task.state_prefix}_manifest.jsonl"
        latest: Dict[str, Dict[str, Any]] = {}
        try:
            with manifest.open(encoding="utf-8") as handle:
                for line in handle:
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    doi = str(record.get("doi", "")).lower()
                    if doi:
                        latest[doi] = record
        except OSError:
            pass
        downloaded = sum(1 for item in latest.values() if item.get("status") == "downloaded")
        failed = sum(1 for item in latest.values() if str(item.get("status", "")).endswith("failed"))
        last_activity = max((str(item.get("recorded_at", "")) for item in latest.values()), default=None)
        return {"downloaded": downloaded, "failed": failed, "lastActivity": last_activity}

    def _log_path(self, task: DownloadTask) -> Path:
        return self.target / "oapdf" / f"{task.state_prefix}.log"

    def _log_tail(self, task: DownloadTask, max_lines: int = 80) -> List[str]:
        path = self._log_path(task)
        try:
            with path.open(encoding="utf-8", errors="replace") as handle:
                return list(handle.readlines())[-max_lines:]
        except OSError:
            return []

    def status(
        self,
        task: DownloadTask,
        include_log: bool = False,
        processes: List[Dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        if processes is None:
            processes = self._processes()
        matches = self._matching(task, processes)
        stats = self._manifest_stats(task)

        # Load dynamic runtime state if available
        runtime_file = self.target / ".doi_download_state" / "runtime" / f"{task.id}.json"
        cooldown_until = None
        needs_login = False
        reason = ""
        outcome = self._read_outcome(task.id)
        with self._lock:
            queued_ids = list(self._queue)
            continuous = task.id in self._desired
            managed = self._managed.get(task.id)
            managed_running = managed is not None and managed.poll() is None
            scheduler_running = task.id in self._group_active.values()
        is_running = bool(matches) or managed_running or scheduler_running
        dyn_status = "running" if is_running else "stopped"
        if runtime_file.is_file():
            try:
                rdata = json.loads(runtime_file.read_text(encoding="utf-8"))
                reason = rdata.get("reason", "")
                cooldown_until = rdata.get("cooldown_until")
                needs_login = bool(rdata.get("needs_login", False))
                if not is_running and rdata.get("state") in ("cooldown", "needs_login", "failed"):
                    dyn_status = rdata["state"]
            except Exception:
                pass
        if not is_running and outcome:
            if outcome.get('state') == 'running':
                dyn_status, reason = 'failed', '任务进程已退出，未留下完成结果；请查看日志'
            else:
                dyn_status = outcome.get("state", dyn_status)
                reason = outcome.get("reason", reason)
        if not is_running and task.id in queued_ids:
            dyn_status = "queued"

        preparing = (self.target / '.collecting').exists()
        ready, readiness_reason = task_readiness(self.root, self.target, task)
        # Preserve an observed task outcome/runtime state; readiness describes
        # only a fresh idle task and must not hide failures or completed runs.
        has_prior_state = bool(outcome) or runtime_file.is_file()
        if not is_running and not ready and not has_prior_state and task.id not in queued_ids:
            dyn_status, reason = "not_ready", readiness_reason
        if preparing and not is_running:
            dyn_status, reason = 'preparing', '正在收集本月期刊 DOI；清单完成后自动启动下载'
            cooldown_until = None
        result = {
            "id": task.id,
            "name": task.name,
            "access": task.access,
            "maturity": getattr(task, "maturity", "production"),
            "enabled": task.enabled and ready and not preparing,
            "canStart": task.enabled and ready and not preparing,
            "note": task.note,
            "status": dyn_status,
            "reason": reason,
            "lastExitCode": outcome.get("exit_code"),
            "lastFinishedAt": outcome.get("finished_at"),
            "browserGroup": browser_group_id(task),
            "browserHealth": self._browser_health.get(task.id, "unchecked"),
            "queuePosition": queued_ids.index(task.id) + 1 if task.id in queued_ids else None,
            "continuous": continuous,
            "pids": [p["pid"] for p in matches],
            "elapsed": matches[0]["elapsed"] if matches else None,
            "logPath": str(self._log_path(task)),
            "cooldownUntil": cooldown_until,
            "needsLogin": needs_login,
            "recentSuccessRate": (
                round(stats["downloaded"] / (stats["downloaded"] + stats["failed"]), 3)
                if (stats["downloaded"] + stats["failed"]) > 0
                else None
            ),
            **stats,
        }
        if include_log:
            result["log"] = self._log_tail(task)
        return result

    def list(self) -> Dict[str, Any]:
        processes = self._processes()
        # Reload the declarative registry on every list request.  This keeps a
        # long-running dashboard in sync when platforms are added or their
        # maturity/enabled flags change.
        tasks = get_download_tasks()
        return {
            "target": str(self.target),
            "timestamp": int(time.time()),
            "maxActiveBrowserGroups": self.max_parallel,
            "browserGroups": self._browser_group_status(),
            "tasks": [self.status(task, processes=processes) for task in tasks],
        }

    @staticmethod
    def _free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind(("127.0.0.1", 0))
            return int(listener.getsockname()[1])

    @staticmethod
    def _browser_alive(group: BrowserGroup) -> bool:
        if not group.endpoint:
            return False
        try:
            with urlopen(f"{group.endpoint}/json/version", timeout=0.5) as response:
                return response.status == 200
        except OSError:
            return False

    def _browser_pid(self, group: BrowserGroup) -> int | None:
        profile_marker = f"--user-data-dir={group.profile}"
        endpoint_port = urlparse(group.endpoint or "").port
        port_marker = f"--remote-debugging-port={endpoint_port}" if endpoint_port else ""
        for process in self._processes():
            command = process["command"]
            if profile_marker in command and (not port_marker or port_marker in command):
                return int(process["pid"])
        if group.process is not None and group.process.poll() is None:
            return group.process.pid
        return None

    def _check_browser_page(self, port: int, group_id: str = "") -> None:
        try:
            if group_id == "nature":
                open_start_page(port, "https://www.nature.com/", clean_empty_tabs=True)
            else:
                start_url = ("https://eds.tju.edu.cn/" if group_id == "elsevier" else
                             "http://p.lib.tju.edu.cn/" if group_id in {"tju_med", "tju_audit"} else
                             "https://d.buaa.edu.cn/")
                open_start_page(port, start_url, clean_empty_tabs=True)
        except (OSError, RuntimeError, ValueError) as error:
            raise TaskControlError("共享浏览器页面连接失败；任务尚未启动，请重试", 503) from error

    def _ensure_browser_locked(self, group_id: str) -> BrowserGroup:
        group = self._browser_groups.setdefault(
            group_id,
            BrowserGroup(
                group_id,
                group_id,
                self.root / ".drission-workflow" / "profiles" / f"doi-{group_id}-shared",
            ),
        )
        if self._browser_alive(group):
            self._check_browser_page(urlparse(group.endpoint).port, group_id)
            return group
        chrome_app = Path(resolve_chrome_binary())
        if not chrome_app.is_file():
            raise TaskControlError(f"Google Chrome/Chromium 浏览器不存在: {chrome_app}", 500)

        if self._browser_pid(group):
            raise TaskControlError('任务浏览器仍在运行，不能改写资料或启动第二个实例', 409)

        group.profile.mkdir(parents=True, exist_ok=True)
        prepare_pdf_download_profile(group.profile)
        port = self._free_port()
        endpoint = f"http://127.0.0.1:{port}"
        cmd = [
            str(chrome_app),
            f"--remote-debugging-port={port}",
            "--remote-debugging-address=127.0.0.1",
            f"--user-data-dir={group.profile}",
            "--start-minimized",
            "--disable-background-timer-throttling",
            "--disable-renderer-backgrounding",
            "--disable-backgrounding-occluded-windows",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-popup-blocking",
            "--hide-crash-restore-bubble",
            "--disable-features=ProfileErrorDialog,Translate,OptimizationHints,MediaRouter",
        ]
        if group_id == "nature":
            cmd.extend(["--no-proxy-server", "--dns-over-https-mode=off"])
        headless = os.environ.get("DRISSION_HEADLESS", os.environ.get("DRISSION_CHROME_HEADLESS", "1"))
        if sys.platform != "darwin" and headless.lower() not in {"0", "false", "no"}:
            cmd.append("--headless=new")

        log_path = self.target / "oapdf" / f"browser_group_{group_id}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_handle = log_path.open("ab", buffering=0)
        try:
            process = subprocess.Popen(
                cmd,
                cwd=self.root,
                stdin=subprocess.DEVNULL,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        finally:
            log_handle.close()

        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise TaskControlError(f"{group.name} 共享浏览器启动失败", 500)
            try:
                with urlopen(f"{endpoint}/json/version", timeout=0.5) as response:
                    if response.status == 200:
                        group.process = process
                        group.endpoint = endpoint
                        # Ego may relaunch itself with `--restart` and reparent
                        # the real browser to launchd. Track that adopted PID.
                        group.pid = self._browser_pid(group) or process.pid
                        self._check_browser_page(port, group_id)
                        return group
            except OSError:
                time.sleep(0.2)
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass
        raise TaskControlError(f"{group.name} 共享浏览器启动超时", 500)

    def _close_browser_locked(self, group_id: str) -> None:
        group = self._browser_groups.get(group_id)
        if group is None:
            return
        processes = self._processes()
        profile_marker = f"--user-data-dir={group.profile}"
        roots = {
            process["pid"] for process in processes if profile_marker in process["command"]
        }
        if group.pid:
            roots.add(group.pid)
        self._signal_pids(self._descendants(processes, roots), signal.SIGTERM)
        # Reap our child before dropping Popen: a zombie still appears in ps
        # and would make restart_browser incorrectly report an occupied profile.
        if group.process is not None:
            try:
                group.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                group.process.kill()
                group.process.wait(timeout=5)
        group.process = None
        group.endpoint = None
        group.pid = None

    def _browser_group_status(self) -> List[Dict[str, Any]]:
        with self._lock:
            queued_by_group: Dict[str, List[str]] = {}
            for task_id in self._queue:
                group_id = browser_group_id(self._task(task_id))
                if group_id:
                    queued_by_group.setdefault(group_id, []).append(task_id)
            return [
                {
                    "id": group.id,
                    "name": group.name,
                    "status": "running" if group.id in self._group_active else ("idle" if self._browser_alive(group) else "stopped"),
                    "activeTask": self._group_active.get(group.id),
                    "queuedTasks": queued_by_group.get(group.id, []),
                    "health": self._browser_health.get(group.id, "unchecked"),
                    "pid": (group.pid or self._browser_pid(group)) if self._browser_alive(group) else None,
                }
                for group in self._browser_groups.values()
            ]

    def _spawn_locked(self, task: DownloadTask, limit: int, group_id: str | None) -> None:
        self._run_baseline[task.id] = self._manifest_stats(task)["downloaded"]
        self._save_outcome(task.id, 'running', '任务已启动')
        runtime_file = self.target / '.doi_download_state/runtime' / f'{task.id}.json'
        runtime_file.parent.mkdir(parents=True, exist_ok=True)
        runtime_file.write_text(json.dumps({'state':'starting','reason':'任务启动中','needs_login':False}))
        script = self.root / "examples" / "academic" / task.script
        log_path = self._log_path(task)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_handle = log_path.open("ab", buffering=0)
        stamp = datetime.now().astimezone().isoformat(timespec="seconds")
        log_handle.write(
            f"\n[{stamp}] dashboard batch started (limit={limit}, browser_group={group_id or 'none'})\n".encode()
        )
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        if group_id:
            group = self._ensure_browser_locked(group_id)
            env["DRISSION_SHARED_BROWSER_ENDPOINT"] = str(group.endpoint)
            env["DRISSION_SHARED_BROWSER_GROUP"] = group_id
        cmd = [sys.executable, "-u", str(script), str(self.target), "--limit", str(limit)]
        if task.id == 'oa_repository' and task.id in self._single_month_tasks:
            cmd.append('--single-month')
        try:
            process = subprocess.Popen(
                cmd,
                cwd=self.root,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        finally:
            log_handle.close()
        self._managed[task.id] = process
        self._active_limits[task.id] = limit
        self._started_at[task.id] = time.monotonic()
        if group_id:
            self._group_active[group_id] = task.id
        threading.Thread(
            target=self._watch_process,
            args=(task.id, process, group_id),
            daemon=True,
            name=f"doi-task-{task.id}",
        ).start()

    def _schedule_locked(self) -> None:
        # Include standalone retries and orphan task browsers, never personal Chrome
        # or its renderer/helper processes, in the same browser budget.
        profiles = (f"--user-data-dir={self.root}/.drission-workflow/profiles/",
                    "--user-data-dir=.drission-workflow/profiles/")
        chrome_command = str(Path(resolve_chrome_binary()))
        actual_browsers = sum(
            p['command'].startswith(chrome_command + ' ')
            and any(marker in p['command'] for marker in profiles)
            and '--type=' not in p['command']
            for p in self._processes()
        )
        active_browser_groups = max(len(self._group_active), actual_browsers)
        for task_id in list(self._queue):
            task = self._task(task_id)
            if task_id in self._desired and not task.enabled:
                self._queue.remove(task_id)
                self._desired.discard(task_id)
                self._queued_limits.pop(task_id, None)
                self._save_outcome(task_id, 'stopped', '平台已禁用，停止自动续跑')
                continue
            group_id = browser_group_id(task)
            group = self._browser_groups.get(group_id)
            reuses_browser = bool(group and self._browser_alive(group))
            if group_id and (group_id in self._group_active or (self.max_parallel > 0 and active_browser_groups >= self.max_parallel and not reuses_browser)):
                continue
            self._queue.remove(task_id)
            limit = self._queued_limits.pop(task_id, task.batch_size)
            try:
                self._spawn_locked(task, int(limit or task.batch_size), group_id)
            except (TaskControlError, OSError, RuntimeError) as error:
                self._save_outcome(task_id, 'failed', f'启动失败：{error}')
                self._retry_later_locked(task_id)
                continue
            if group_id and not reuses_browser:
                active_browser_groups += 1

    def _close_unused_browsers_locked(self) -> None:
        for group_id in list(self._browser_groups):
            # Free inactive independent browser slots after a batch ends.
            if group_id not in self._group_active:
                self._close_browser_locked(group_id)

    def _retry_later_locked(self, task_id: str, delay: float = 60) -> None:
        if task_id not in self._desired:
            return
        previous = self._retry_timers.pop(task_id, None)
        if previous:
            previous.cancel()
        def resume():
            with self._lock:
                if self._retry_timers.get(task_id) is not timer:
                    return
                self._retry_timers.pop(task_id, None)
                if task_id not in self._desired or task_id in self._managed:
                    return
                if task_id not in self._queue:
                    self._queue.append(task_id)
                    self._queued_limits[task_id] = self._task(task_id).batch_size
                self._schedule_locked()
        previous_outcome = self._read_outcome(task_id)
        self._save_outcome(task_id, 'cooldown', f"{previous_outcome.get('reason', '本批失败')}；{int(delay)} 秒后自动继续后续批次", previous_outcome.get('exit_code'))
        timer = threading.Timer(delay, resume)
        timer.daemon = True
        self._retry_timers[task_id] = timer
        timer.start()

    def _recover_browser_locked(self, task_id: str, process: subprocess.Popen, group_id: str) -> None:
        """Stop the affected worker before replacing its page; replay unfinished input."""
        if self._managed.get(task_id) is not process or process.poll() is not None:
            return
        task = self._task(task_id)
        attempt = self._browser_recoveries.get(task_id, 0) + 1
        self._browser_recoveries[task_id] = attempt
        reason = f"浏览器异常（{self._browser_health.get(task_id)}）；已使用 {min(attempt, 2)}/2 次恢复，不计为论文下载失败"
        self._save_outcome(task_id, 'recovering', reason)
        with self._log_path(task).open('a') as log:
            log.write(f"\n[browser_recovery] {reason}\n")
        targets = self._descendants(self._processes(), {process.pid})
        # Freeze the worker first so runner termination cannot publish false DOI failures.
        self._signal_pids({process.pid}, signal.SIGSTOP)
        self._signal_pids(targets, signal.SIGKILL)
        process.wait(timeout=5)
        self._managed.pop(task_id, None)
        self._started_at.pop(task_id, None)
        self._run_baseline.pop(task_id, None)
        self._group_active.pop(group_id, None)
        retry_limit = self._active_limits.pop(task_id, task.batch_size)
        if attempt > 2:
            self._desired.discard(task_id)
            self._queued_limits.pop(task_id, None)
            self._save_outcome(task_id, 'failed', '浏览器连续异常，已停止自动重试；请检查内存、磁盘和VPN后重新启动任务')
            self._close_browser_locked(group_id)
            self._schedule_locked()
            return
        group = self._browser_groups[group_id]
        def restart_browser():
            old_pid = group.pid or self._browser_pid(group)
            self._close_browser_locked(group_id)
            deadline = time.monotonic() + 5
            while old_pid and any(p['pid'] == old_pid for p in self._processes()):
                if time.monotonic() >= deadline:
                    raise RuntimeError('旧浏览器尚未退出，停止重启以避免Profile冲突')
                time.sleep(.2)
        try:
            # First recovery replaces stuck tabs, preserving the authenticated profile.
            # A repeated crash escalates to a restart of this dedicated browser only.
            if attempt == 1 and group.endpoint:
                try:
                    port = urlparse(group.endpoint).port
                    ensure_usable_page(port, close_unresponsive=True)
                    if browser_page_health(port) != 'healthy':
                        raise RuntimeError('replacement page did not respond')
                except (OSError, RuntimeError, ValueError):
                    restart_browser()
            else:
                restart_browser()
            self._ensure_browser_locked(group_id)
            self._browser_health[task_id] = 'healthy'
            self._save_outcome(task_id, 'queued', '浏览器已恢复；重试未完成批次，已有PDF按原去重规则跳过')
            if task_id not in self._queue:
                self._queue.appendleft(task_id)
            self._queued_limits[task_id] = retry_limit
            self._schedule_locked()
        except (TaskControlError, OSError, RuntimeError, ValueError) as error:
            self._desired.discard(task_id)
            self._queued_limits.pop(task_id, None)
            self._save_outcome(task_id, 'failed', f'浏览器恢复失败，任务已暂停：{error}')
            self._close_browser_locked(group_id)
            self._schedule_locked()

    def _watch_process(self, task_id: str, process: subprocess.Popen, group_id: str | None) -> None:
        unhealthy = 0
        unhealthy_since = None
        while group_id and process.poll() is None:
            try:
                process.wait(timeout=5)
                break
            except subprocess.TimeoutExpired:
                pass
            with self._lock:
                if self._managed.get(task_id) is not process:
                    return
                endpoint = self._browser_groups[group_id].endpoint
            try:
                health = browser_page_health(urlparse(endpoint or '').port) if endpoint else 'browser_unavailable'
            except (OSError, RuntimeError, ValueError):
                health = 'browser_unavailable'
            with self._lock:
                if self._managed.get(task_id) is not process:
                    return
                self._browser_health[task_id] = health
                unhealthy = 0 if health == 'healthy' else unhealthy + 1
                now = time.monotonic()
                if health == 'healthy':
                    unhealthy_since = None
                elif unhealthy_since is None:
                    unhealthy_since = now
                # CDP timeouts also occur during navigation; allow sustained loading.
                confirmed = health in {'renderer_crashed', 'browser_unavailable'}
                sustained = unhealthy_since is not None and now - unhealthy_since >= 90
                if unhealthy >= 2 and (confirmed or sustained) and process.poll() is None:
                    self._recover_browser_locked(task_id, process, group_id)
                    return
        return_code = process.wait()
        with self._lock:
            if self._managed.get(task_id) is not process:
                return
            self._managed.pop(task_id, None)
            self._active_limits.pop(task_id, None)
            started = self._started_at.pop(task_id, time.monotonic())
            if group_id and self._group_active.get(group_id) == task_id:
                self._group_active.pop(group_id, None)
            task = self._task(task_id)
            downloaded = max(0, self._manifest_stats(task)['downloaded'] - self._run_baseline.pop(task_id, 0))
            try:
                runtime = json.loads((self.target / '.doi_download_state/runtime' / f'{task_id}.json').read_text())
            except (OSError, ValueError):
                runtime = {}
            state = runtime.get('state')
            progressed = downloaded > 0 or int(runtime.get('processed_records', 0)) > 0
            if state not in {'failed', 'needs_login', 'cooldown'}:
                state = 'failed' if return_code != 0 else ('completed' if progressed else 'no_output')
            reason = runtime.get('reason') or (f'本批新增 {downloaded} 篇有效 PDF' if downloaded else '本批未新增有效 PDF；可能无候选、已处理或内容被排除，请查看日志')
            if return_code != 0:
                reason = f'退出码 {return_code}；{reason}'
            self._save_outcome(task_id, state, reason, return_code)
            # Failed continuous batches retry with a delay; empty exhausted batches finish.
            if task_id in self._desired and return_code == 0 and progressed and state == 'completed':
                self._browser_recoveries[task_id] = 0
                if task_id not in self._queue:
                    self._queue.append(task_id)
                    self._queued_limits[task_id] = self._task(task_id).batch_size
            elif task_id in self._desired and state == 'failed':
                self._retry_later_locked(task_id)
            elif task_id in self._desired and state == 'cooldown':
                try:
                    delay = datetime.fromisoformat(runtime['cooldown_until']).timestamp() - time.time()
                except (KeyError, TypeError, ValueError):
                    delay = task.cooldown_seconds
                self._retry_later_locked(task_id, max(1, delay))
            else:
                self._desired.discard(task_id)
                self._queued_limits.pop(task_id, None)
            self._schedule_locked()
            self._close_unused_browsers_locked()

    def start(self, task_id: str, limit: Optional[int] = None, single_month: bool = False) -> Dict[str, Any]:
        if (self.target / '.collecting').exists():
            raise TaskControlError('正在收集本月期刊 DOI；清单完成后自动启动下载', 409)
        task = self._task(task_id)
        ready, readiness_reason = task_readiness(self.root, self.target, task)
        if not ready:
            raise TaskControlError(readiness_reason, 409)
        if not task.enabled and limit is None:
            raise TaskControlError(task.note or "该任务当前已禁用", 409)
        if not self.target.is_dir():
            raise TaskControlError(f"DOI 数据目录不存在: {self.target}", 409)
        script = self.root / "examples" / "academic" / task.script
        if not script.is_file():
            raise TaskControlError(f"下载脚本不存在: {script}", 500)

        with self._lock:
            current = self.status(task)
            if current["status"] in {"running", "queued"} or task_id in self._retry_timers:
                raise TaskControlError(f"{task.name} 已经在运行或排队", 409)
            if single_month:
                self._single_month_tasks.add(task_id)
            else:
                self._single_month_tasks.discard(task_id)
            if limit is None:
                self._desired.add(task.id)
                cycle_limit = task.batch_size
            else:
                cycle_limit = max(1, int(limit))
            self._queued_limits[task.id] = cycle_limit
            self._browser_recoveries[task.id] = 0
            self._browser_health.pop(task.id, None)
            self._queue.append(task.id)
            self._schedule_locked()
        time.sleep(0.15)
        return self.status(task, include_log=True)

    def calibrate(self, task_id: str) -> Dict[str, Any]:
        """Execute calibrated sample run: Cambridge 20, RSC 5, TJU 45, OA 100, others 10."""
        limits = {
            "cambridge": 20,
            "rsc": 5,
            "tju_audit": 45,
            "oa_repository": 100,
        }
        limit = limits.get(task_id, 10)
        return self.start(task_id, limit=limit, single_month=True)

    @staticmethod
    def _descendants(processes: List[Dict[str, Any]], roots: set[int]) -> set[int]:
        result = set(roots)
        changed = True
        while changed:
            changed = False
            for process in processes:
                if process["ppid"] in result and process["pid"] not in result:
                    result.add(process["pid"])
                    changed = True
        return result

    @staticmethod
    def _signal_pids(pids: set[int], sig: signal.Signals) -> None:
        for pid in sorted(pids, reverse=True):
            try:
                os.kill(pid, sig)
            except (ProcessLookupError, PermissionError):
                pass

    def stop(self, task_id: str) -> Dict[str, Any]:
        task = self._task(task_id)
        with self._lock:
            self._desired.discard(task.id)
            timer = self._retry_timers.pop(task.id, None)
            if timer:
                timer.cancel()
                self._save_outcome(task.id, "stopped", "已手动停止自动续跑")
            self._queued_limits.pop(task.id, None)
            self._queue = deque(item for item in self._queue if item != task.id)
            processes = self._processes()
            roots = {p["pid"] for p in self._matching(task, processes)}
            managed = self._managed.get(task.id)
            if managed is not None and managed.poll() is None:
                roots.add(managed.pid)
            targets = self._descendants(processes, roots)
            # A previously interrupted runner can leave its dedicated Chrome orphaned.
            if task.profile_marker:
                targets.update(p["pid"] for p in processes if task.profile_marker in p["command"])
            # Detach the watcher before releasing the lock so its exit cannot
            # overwrite an explicit stop with "failed" and trigger a retry.
            self._managed.pop(task.id, None)
            self._started_at.pop(task.id, None)
            self._active_limits.pop(task.id, None)
            self._run_baseline.pop(task.id, None)
            group_id = browser_group_id(task)
            if group_id and self._group_active.get(group_id) == task.id:
                self._group_active.pop(group_id, None)
            self._save_outcome(task.id, 'stopped', '已手动停止；等待用户重新启动')
            if not targets:
                self._schedule_locked()
                self._close_unused_browsers_locked()
                return self.status(task, include_log=True)

            self._signal_pids(targets, signal.SIGINT)
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                alive = {p["pid"] for p in self._processes()} & targets
                if not alive:
                    break
                time.sleep(0.15)
            alive = {p["pid"] for p in self._processes()} & targets
            self._signal_pids(alive, signal.SIGTERM)
            self._managed.pop(task.id, None)
            self._schedule_locked()
            self._close_unused_browsers_locked()
        time.sleep(0.15)
        return self.status(task, include_log=True)

    def stop_all(self) -> None:
        with self._lock:
            # Clear every queued/retry job before stopping any worker: stop() schedules the queue.
            self._desired.clear()
            self._queue.clear()
            self._queued_limits.clear()
            for timer in self._retry_timers.values():
                timer.cancel()
            self._retry_timers.clear()
            for task in get_download_tasks():
                self.stop(task.id)

    def stop_browser_group(self, group_id: str) -> Dict[str, Any]:
        with self._lock:
            if group_id not in self._browser_groups:
                raise TaskControlError(f"未知浏览器组: {group_id}", 404)
            task_ids = {
                task.id for task in get_download_tasks() if browser_group_id(task) == group_id
            }
            for task_id in task_ids:
                self._desired.discard(task_id)
                self._queued_limits.pop(task_id, None)
                timer = self._retry_timers.pop(task_id, None)
                if timer:
                    timer.cancel()
            self._queue = deque(item for item in self._queue if item not in task_ids)
            processes = self._processes()
            roots = {
                process["pid"]
                for task in get_download_tasks()
                if task.id in task_ids
                for process in self._matching(task, processes)
            }
            for task_id in task_ids:
                managed = self._managed.get(task_id)
                if managed is not None and managed.poll() is None:
                    roots.add(managed.pid)
            targets = self._descendants(processes, roots)
            self._signal_pids(targets, signal.SIGINT)

            # Do not release the shared-browser slot until every downloader in
            # this group has really stopped.  Otherwise the scheduler may open
            # the next browser while old Chrome children are still alive.
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                alive = {process["pid"] for process in self._processes()} & targets
                if not alive:
                    break
                time.sleep(0.15)
            alive = {process["pid"] for process in self._processes()} & targets
            self._signal_pids(alive, signal.SIGTERM)
            for task_id in task_ids:
                self._managed.pop(task_id, None)
                self._started_at.pop(task_id, None)
                self._save_outcome(task_id, 'stopped', '已手动停止浏览器组；等待用户重新启动')
            self._group_active.pop(group_id, None)
            self._close_browser_locked(group_id)
            self._schedule_locked()
            self._close_unused_browsers_locked()
        return {"browserGroups": self._browser_group_status(), "stopped": group_id}

    def detail(self, task_id: str) -> Dict[str, Any]:
        return self.status(self._task(task_id), include_log=True)


class MonthlyDownloadController:
    """One selected month, using the existing platform queues and download checkpoints."""

    def __init__(self, manager: DownloadTaskManager):
        self.manager = manager
        self.base = manager.target.parent if re.fullmatch(r'\d{4}-\d{2}', manager.target.name) else None
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread = None
        self.state = dict(enabled=False, month=manager.target.name if self.base else '',
                          phase='stopped', reason='选择月份后开启全天下载', journalsDone=0, journalsTotal=0, collectionErrors=0)
        self.path = self.base / '.doi_download_state/monthly_control.json' if self.base else None
        if self.path and self.path.is_file():
            try:
                saved = json.loads(self.path.read_text())
                if isinstance(saved, dict):
                    self.state.update({key: saved[key] for key in self.state if key in saved})
            except (OSError, ValueError):
                pass

    def status(self) -> dict:
        with self._lock:
            return dict(self.state, available=bool(self.base),
                        busy=bool(self._thread and self._thread.is_alive()),
                        minMonth=f'{self.manager.target.name[:4]}-01' if self.base else '',
                        maxMonth=min(f'{self.manager.target.name[:4]}-12', datetime.now().strftime('%Y-%m')) if self.base else '')

    def _save(self, **changes) -> None:
        with self._lock:
            self.state.update(changes)
            if self.path:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                temp = self.path.with_suffix('.tmp')
                temp.write_text(json.dumps(self.state, ensure_ascii=False))
                temp.replace(self.path)

    def start(self, month: str) -> dict:
        if not self.base:
            raise TaskControlError('当前控制台未配置月度目录', 409)
        if not isinstance(month, str) or not re.fullmatch(r'\d{4}-(0[1-9]|1[0-2])', month):
            raise TaskControlError('月份格式应为 YYYY-MM')
        if month[:4] != self.manager.target.name[:4] or month > datetime.now().strftime('%Y-%m'):
            raise TaskControlError('请选择当前资料年份内已开始的月份')
        folder = self.base / month
        if folder.resolve().parent != self.base.resolve():
            raise TaskControlError('月份目录不能指向资料目录之外')
        with self._lock:
            if self._thread and self._thread.is_alive():
                if self.state['enabled'] and self.state['month'] == month:
                    return self.status()
                raise TaskControlError('请先关闭当前月份总开关，待任务停止后再切换月份', 409)
            self._stop = threading.Event()
            self._save(enabled=True, month=month, phase='preparing', reason='准备所选月份，正在停止旧任务',
                       journalsDone=0, journalsTotal=0, collectionErrors=0)
            self._thread = threading.Thread(target=self._run, args=(folder,), daemon=True, name='monthly-downloads')
            self._thread.start()
            return self.status()

    def stop(self) -> dict:
        with self._lock:
            self._stop.set()
            self._save(enabled=False, phase='stopping', reason='正在停止下载与自动续跑')
        self.manager.stop_all()
        with self._lock:
            if not self._thread or not self._thread.is_alive():
                self._save(phase='stopped', reason='全天下载已关闭，已下载文件和收集进度保留')
        return self.status()

    def resume(self) -> None:
        if self.state['enabled']:
            try:
                self.start(self.state['month'])
            except TaskControlError as error:
                self._save(enabled=False, phase='failed', reason=str(error))

    def _collect(self, folder: Path) -> None:
        source = folder / 'total_journals' if (folder / 'total_journals').is_dir() else folder
        if (any(not p.name.startswith('._') for p in source.glob('*.csv'))
                and not (folder / '.collecting').exists() and not (folder / 'collection_errors.json').exists()):
            return
        # Reuse the journal list, ISSN handling, pagination and per-journal cache.
        sys.path.insert(0, str(self.manager.root / 'outputs/journal_2026_download'))
        from run_months import fetch_journal, JOURNALS, FIELDS
        (folder / 'metadata').mkdir(parents=True, exist_ok=True)
        marker = folder / '.collecting'
        marker.touch()
        year, month = map(int, folder.name.split('-'))
        start = f'{folder.name}-01'
        end = min(f'{folder.name}-{calendar.monthrange(year, month)[1]:02d}', datetime.now().strftime('%Y-%m-%d'))
        rows = {}
        errors = []
        self._save(phase='collecting', reason='正在收集本月期刊 DOI，完成后自动下载', journalsTotal=len(JOURNALS))
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {pool.submit(fetch_journal, j, start, end, folder, self._stop.is_set): j for j in JOURNALS}
            for done, future in enumerate(as_completed(futures), 1):
                if self._stop.is_set():
                    for pending in futures:
                        pending.cancel()
                    return
                try:
                    rows.update((r['doi'], r) for r in future.result())
                except Exception as error:
                    errors.append(f"{futures[future]['期刊名称']}：{type(error).__name__}")
                self._save(journalsDone=done)
        if self._stop.is_set():
            return
        if errors:
            (folder / 'collection_errors.json').write_text(json.dumps(errors, ensure_ascii=False))
            self._save(collectionErrors=len(errors))
            if not rows:
                raise RuntimeError(f'{len(errors)} 本期刊收集失败，未取得可下载清单；重新开启可续收')
        else:
            (folder / 'collection_errors.json').unlink(missing_ok=True)
        temp = source / 'reversed_dois_refreshed_part_1.csv.tmp'
        with temp.open('w', encoding='utf-8-sig', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(sorted(rows.values(), key=lambda r: r['published'], reverse=True))
        temp.replace(temp.with_suffix(''))
        marker.unlink()

    def _run(self, folder: Path) -> None:
        awake = None
        try:
            if sys.platform == 'darwin':
                awake = subprocess.Popen(['/usr/bin/caffeinate', '-i', '-w', str(os.getpid())],
                                         stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.manager.stop_all()
            if self._stop.is_set():
                return
            folder.mkdir(parents=True, exist_ok=True)
            if not (folder / 'oapdf').exists():
                (self.base / 'oapdf').mkdir(exist_ok=True)
                (folder / 'oapdf').symlink_to(self.base / 'oapdf', target_is_directory=True)
            if self.manager.target != folder:
                self.manager = DownloadTaskManager(self.manager.root, folder, self.manager.max_parallel or 5)
                self.manager.stop_all()  # Recover workers left by a previous server process for this month.
            self._collect(folder)
            if self._stop.is_set():
                return
            self._save(phase='running', reason='全天下载中；自动分批续跑，冷却后继续，等待登录的平台暂停')
            for task in get_download_tasks():
                if not task.enabled:
                    continue
                with self.manager._lock:
                    if self._stop.is_set():
                        return
                    try:
                        runtime = json.loads((folder / '.doi_download_state/runtime' / f'{task.id}.json').read_text())
                    except (OSError, ValueError):
                        runtime = {}
                    if runtime.get('needs_login') or runtime.get('state') == 'needs_login':
                        self.manager._save_outcome(task.id, 'needs_login', runtime.get('reason', '请先完成登录'))
                        continue
                    try:
                        delay = datetime.fromisoformat(runtime['cooldown_until']).timestamp() - time.time()
                    except (KeyError, TypeError, ValueError):
                        delay = 0
                    if delay > 0:
                        self.manager._desired.add(task.id)
                        self.manager._single_month_tasks.add(task.id)
                        self.manager._retry_later_locked(task.id, delay)
                    else:
                        self.manager.start(task.id, single_month=True)
            while not self._stop.wait(5):
                with self.manager._lock:
                    if self.manager._desired or self.manager._queue or self.manager._managed:
                        continue
                self._save(enabled=False, phase='finished', reason='本月自动通道已结束；未取得全文的记录请查看各平台原因')
                return
        except Exception as error:
            try:
                self.manager.stop_all()
            finally:
                self._save(enabled=False, phase='failed', reason=f'月度下载已暂停：{error}')
        finally:
            if self._stop.is_set():
                self._save(enabled=False, phase='stopped', reason='全天下载已关闭，已下载文件和收集进度保留')
            if awake:
                awake.terminate()
                awake.wait(timeout=5)

# Top-100 Gold Open Access Journals (Fully OA by Policy)
GOLD_OA_ISSNS = {
    "2059-3635",  # Signal Transduction and Targeted Therapy
    "2311-6706",  # Nano-Micro Letters
    "2770-596X",  # iMeta
    "2667-1417",  # eScience
    "2520-8136",  # Electrochemical Energy Reviews
    "2054-9369",  # Military Medical Research
    "2523-3548",  # Cancer Communications
    "2772-834X",  # Advanced Powder Materials
    "2468-2667",  # The Lancet Public Health
    "2731-6084",  # Nature Water
    "2731-0396",  # Nature Chemical Engineering
    "2365-9440",  # International Journal of Educational Technology in Higher Education
    "2041-1723",  # Nature Communications
    "2399-3642",  # Communications Biology
    "2399-3650",  # Communications Physics
    "1553-7374",  # PLoS Pathogens
    "1545-7885",  # PLoS Biology
    "1549-1676",  # PLOS Medicine
    "2589-5370",  # EBioMedicine
    "2666-3791",  # Cell Reports Medicine
    "2211-1247",  # Cell Reports
    "2051-5545",  # World Psychiatry (Free / Open)
}


def repo_root() -> Path:
    env = os.environ.get("DRISSION_ROOT")
    if env:
        return Path(env).expanduser().resolve()
    return Path(__file__).resolve().parents[2]


def clean_name(s: str) -> str:
    s = re.sub(r'[\\/*?:"<>|]', "_", s)
    return s.strip()


def normalize_str(s: str) -> str:
    if not s:
        return ""
    if s.lower().endswith(".pdf"):
        s = s[:-4]
    return re.sub(r"[^\w\d]+", "", s.lower())


def is_valid_pdf_file(path: Path) -> bool:
    try:
        if not path.is_file() or path.stat().st_size < 1024 or path.name.startswith("._"):
            return False
        with path.open("rb") as f:
            return f.read(4).startswith(b"%PDF")
    except OSError:
        return False


def load_oa_metadata_index(root: Path) -> Dict[str, bool]:
    """Build quick lookup index for OA status from OpenAlex cache or analysis files if present."""
    oa_index = {}
    reports_dir = root / "examples" / "academic"
    for rpath in reports_dir.glob("*oa*.json"):
        try:
            data = json.loads(rpath.read_text(encoding="utf-8"))
            if isinstance(data, dict) and "details" in data:
                for w in data["details"].get("downloaded_oa", []) + data["details"].get("missing_oa", []):
                    if "title" in w:
                        oa_index[normalize_str(w["title"])] = True
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and "title" in item and item.get("is_oa"):
                        oa_index[normalize_str(item["title"])] = True
        except Exception:
            pass
    return oa_index


def get_current_dashboard_data(root: Path, target_base: Path, year: int = 2026) -> Dict[str, Any]:
    jpath = root / "examples" / "academic" / "journals_top100.json"
    if not jpath.is_file():
        return {"error": "journals_top100.json not found"}

    journals = json.loads(jpath.read_text(encoding="utf-8"))
    oa_index = load_oa_metadata_index(root)

    total_downloaded_pdfs = 0
    total_downloaded_bytes = 0
    total_oa_count = 0
    total_non_oa_count = 0
    total_journals = len(journals)
    completed_journals = 0
    partial_journals = 0
    pending_journals = 0

    platform_stats = {}
    journal_records = []

    for j in journals:
        j_id = j["id"]
        name = clean_name(j["name"])
        platform = j.get("platform", "OTHER")
        issn = (j.get("issn") or "").replace(" ", "").upper()
        slug = j.get("journalSlug", "")

        is_gold_oa_journal = issn in {x.upper().replace(" ", "") for x in GOLD_OA_ISSNS}

        jdir = target_base / name
        y_dir = jdir / str(year)

        pdf_files = []
        seen_stems = set()

        def _collect(p: Path):
            if is_valid_pdf_file(p) and p.stem not in seen_stems:
                seen_stems.add(p.stem)
                size = p.stat().st_size
                mtime = int(p.stat().st_mtime)

                # Determine OA status
                norm_stem = normalize_str(p.stem)
                is_oa = is_gold_oa_journal or oa_index.get(norm_stem, False)

                # Keyword heuristic
                if not is_oa and any(k in norm_stem for k in ["openaccess", "correction", "authorcorrection", "publishercorrection", "snapp"]):
                    is_oa = True

                pdf_files.append({
                    "name": p.name,
                    "size": size,
                    "mtime": mtime,
                    "is_oa": is_oa
                })

        if y_dir.is_dir():
            for p in y_dir.glob("*.pdf"):
                _collect(p)

        if jdir.is_dir():
            for p in jdir.glob("*.pdf"):
                _collect(p)

        count = len(pdf_files)
        oa_count = sum(1 for p in pdf_files if p["is_oa"])
        non_oa_count = count - oa_count

        total_downloaded_pdfs += count
        total_oa_count += oa_count
        total_non_oa_count += non_oa_count

        j_bytes = sum(p["size"] for p in pdf_files)
        total_downloaded_bytes += j_bytes

        if count >= 20:
            status = "completed"
            completed_journals += 1
        elif count > 0:
            status = "in_progress"
            partial_journals += 1
        else:
            status = "pending"
            pending_journals += 1

        if platform not in platform_stats:
            platform_stats[platform] = {"total": 0, "downloaded": 0, "oa": 0, "non_oa": 0}
        platform_stats[platform]["total"] += 1
        platform_stats[platform]["downloaded"] += count
        platform_stats[platform]["oa"] += oa_count
        platform_stats[platform]["non_oa"] += non_oa_count

        journal_records.append({
            "id": j_id,
            "name": j["name"],
            "platform": platform,
            "issn": issn,
            "slug": slug,
            "status": status,
            "count": count,
            "oa_count": oa_count,
            "non_oa_count": non_oa_count,
            "bytes": j_bytes,
            "files": sorted(pdf_files, key=lambda x: x["mtime"], reverse=True)[:25]  # top 25 latest
        })

    return {
        "timestamp": int(time.time()),
        "year": year,
        "storage_path": str(target_base.resolve()),
        "summary": {
            "total_journals": total_journals,
            "completed_journals": completed_journals,
            "partial_journals": partial_journals,
            "pending_journals": pending_journals,
            "total_pdfs": total_downloaded_pdfs,
            "total_oa_pdfs": total_oa_count,
            "total_non_oa_pdfs": total_non_oa_count,
            "total_gb": round(total_downloaded_bytes / (1024 ** 3), 2),
            "completion_rate": round((completed_journals / total_journals) * 100, 1)
        },
        "platforms": platform_stats,
        "journals": journal_records
    }


_trend_cache = {}
_trend_lock = threading.Lock()


def get_download_trends(target: Path, selected_date: str | None = None) -> dict:
    tz = ZoneInfo("Asia/Shanghai")
    day = datetime.strptime(selected_date, "%Y-%m-%d").date() if selected_date else datetime.now(tz).date()
    key = str(target.resolve())
    with _trend_lock:
        cached = _trend_cache.get(key)
        if cached is None or time.monotonic() - cached[0] >= 60:
            first_success = {}
            skipped = 0
            state = target / ".doi_download_state"
            # Use production manifests only; smoke and validation runs are excluded.
            prefixes = {task.state_prefix for task in get_download_tasks()}
            for prefix in sorted(prefixes):
                try:
                    handle = (state / f"{prefix}_manifest.jsonl").open(encoding="utf-8", errors="replace")
                except OSError:
                    continue
                with handle:
                    for line in handle:
                        try:
                            record = json.loads(line)
                            if record.get("status") != "downloaded":
                                continue
                            doi = str(record.get("doi") or "").strip().lower()
                            stamp = datetime.fromisoformat(record["recorded_at"].replace("Z", "+00:00"))
                            if not doi or stamp.tzinfo is None:
                                raise ValueError("Missing DOI or timezone")
                            stamp = stamp.astimezone(tz)
                            if doi not in first_success or stamp < first_success[doi]:
                                first_success[doi] = stamp
                        except (ValueError, KeyError, TypeError, AttributeError):
                            skipped += 1
            counts = {}
            for stamp in first_success.values():
                hours = counts.setdefault(stamp.date().isoformat(), [0] * 24)
                hours[stamp.hour] += 1
            cached = (time.monotonic(), counts, skipped)
            _trend_cache[key] = cached
    counts = cached[1]
    daily = []
    for offset in range(29, -1, -1):
        date_key = (day - timedelta(days=offset)).isoformat()
        daily.append({"date": date_key, "count": sum(counts.get(date_key, []))})
    hours = counts.get(day.isoformat(), [0] * 24)
    return {"timezone": "Asia/Shanghai", "date": day.isoformat(), "daily": daily,
            "hourly": [{"hour": hour, "count": count} for hour, count in enumerate(hours)],
            "dayTotal": sum(hours), "periodTotal": sum(item["count"] for item in daily),
            "firstDate": min(counts) if counts else None, "ignoredRecords": cached[2]}


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    task_manager: DownloadTaskManager | None = None
    monthly_controller: MonthlyDownloadController | None = None

    @property
    def manager(self):
        return self.monthly_controller.manager if self.monthly_controller else self.task_manager

    def _send_json(self, data: Any, status: int = 200) -> None:
        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            # A dashboard tab can close while an expensive status scan is finishing.
            pass

    def do_GET(self):
        root = repo_root()
        path = urlparse(self.path).path
        if path == '/api/monthly':
            self._send_json(self.monthly_controller.status() if self.monthly_controller else {'available': False})
            return
        if path == "/api/status":
            if not self.manager:
                self._send_json({"error": "任务管理器尚未初始化"}, 503)
            else:
                self._send_json(get_doi_dashboard_data(self.manager.target))
            return

        if path == "/api/download-trends" and self.manager:
            try:
                selected = parse_qs(urlparse(self.path).query).get("date", [None])[0]
                self._send_json(get_download_trends(self.manager.target, selected))
            except (ValueError, OverflowError):
                self._send_json({"error": "日期格式应为 YYYY-MM-DD"}, 400)
            return

        if path == "/api/tasks" and self.manager:
            self._send_json(self.manager.list())
            return

        match = re.fullmatch(r"/api/tasks/([a-z0-9_-]+)", path)
        if match and self.manager:
            try:
                self._send_json(self.manager.detail(match.group(1)))
            except TaskControlError as error:
                self._send_json({"error": str(error)}, error.status)
            return

        if path == "/" or path == "/index.html":
            html_path = root / "examples" / "academic" / "dashboard.html"
            if html_path.is_file():
                content = html_path.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
                return

        super().do_GET()

    def do_POST(self):
        path = urlparse(self.path).path
        if path in {'/api/monthly/start', '/api/monthly/stop'}:
            try:
                if not self.monthly_controller:
                    raise TaskControlError('月度控制器尚未初始化', 503)
                origin = self.headers.get('Origin')
                if origin and urlparse(origin).netloc != self.headers.get('Host'):
                    raise TaskControlError('仅允许从本控制台操作', 403)
                if self.headers.get_content_type() != 'application/json':
                    raise TaskControlError('请求必须使用 application/json')
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 < length <= 1024:
                    raise TaskControlError('请求体大小不正确')
                payload = json.loads(self.rfile.read(length))
                if not isinstance(payload, dict):
                    raise TaskControlError('请求体必须是 JSON 对象')
                result = self.monthly_controller.start(payload.get('month')) if path.endswith('/start') else self.monthly_controller.stop()
                self._send_json(result)
            except TaskControlError as error:
                self._send_json({'error': str(error)}, error.status)
            except (ValueError, TypeError):
                self._send_json({'error': '无效请求参数'}, 400)
            except Exception as error:
                self._send_json({'error': f'月度任务控制失败：{error}'}, 500)
            return
        group_match = re.fullmatch(r"/api/browser-groups/([a-z0-9_-]+)/stop", path)
        if group_match and self.manager:
            try:
                self._send_json(self.manager.stop_browser_group(group_match.group(1)))
            except TaskControlError as error:
                self._send_json({"error": str(error)}, error.status)
            except Exception as error:
                self._send_json({"error": f"浏览器组控制失败: {error}"}, 500)
            return
        match = re.fullmatch(r"/api/tasks/([a-z0-9_-]+)/(start|stop|calibrate)", path)
        if not match or not self.manager:
            self._send_json({"error": "接口不存在"}, 404)
            return
        task_id, action = match.groups()
        try:
            if action != 'stop' and self.monthly_controller and self.monthly_controller.state['phase'] in {'preparing', 'collecting', 'stopping'}:
                raise TaskControlError('月度任务正在准备或停止，请稍后再操作单个平台', 409)
            if action == "start":
                result = self.manager.start(task_id, single_month=bool(self.monthly_controller and self.monthly_controller.base))
            elif action == "stop":
                result = self.manager.stop(task_id)
            else:
                result = self.manager.calibrate(task_id)
            self._send_json(result)
        except TaskControlError as error:
            self._send_json({"error": str(error)}, error.status)
        except Exception as error:
            self._send_json({"error": f"任务控制失败: {error}"}, 500)

    def log_message(self, format: str, *args: Any) -> None:
        if urlparse(self.path).path not in {"/api/status", "/api/tasks"}:
            super().log_message(format, *args)


class DashboardServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main():
    parser = argparse.ArgumentParser(description="DOI download control dashboard server")
    parser.add_argument("--export", type=str, default="examples/academic/doi_status.json", help="Export DOI status to JSON path")
    parser.add_argument("--serve", type=int, nargs="?", const=8899, help="Run HTTP server on specified port (default: 8899)")
    parser.add_argument("--host", default="127.0.0.1", help="HTTP bind address (default: 127.0.0.1)")
    parser.add_argument("--max-parallel", type=int, default=MAX_ACTIVE_BROWSER_GROUPS, help="Browser concurrency limit; 0 means unlimited")
    parser.add_argument("--doi-target", type=Path, default=DEFAULT_DOI_TARGET, help="DOI CSV dataset directory controlled by the dashboard")
    args = parser.parse_args()

    root = repo_root()
    data = get_doi_dashboard_data(args.doi_target)

    out_file = root / args.export
    out_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Status JSON exported to: {out_file}")

    if args.serve:
        port = args.serve
        DashboardHandler.task_manager = DownloadTaskManager(root, args.doi_target, args.max_parallel)
        DashboardHandler.monthly_controller = MonthlyDownloadController(DashboardHandler.task_manager)
        print(f"\n=======================================================")
        print(f"🚀 DOI Download Dashboard running at:")
        print(f"👉 http://{args.host}:{port}")
        print(f"DOI task target: {DashboardHandler.task_manager.target}")
        print(f"=======================================================\n")
        with DashboardServer((args.host, port), DashboardHandler) as httpd:
            DashboardHandler.monthly_controller.resume()
            try:
                httpd.serve_forever()
            except KeyboardInterrupt:
                print("\nServer stopped.")


if __name__ == "__main__":
    main()
