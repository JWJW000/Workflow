"""Tests for status server task loading and API contracts."""

from __future__ import annotations

from pathlib import Path
import pytest

from examples.academic.status_server import (
    DownloadTask,
    get_download_tasks,
    DownloadTaskManager,
    browser_group_id,
    get_doi_dashboard_data,
)


def test_get_download_tasks():
    tasks = get_download_tasks()
    task_ids = [t.id for t in tasks]
    assert "ieee" in task_ids
    assert "springer" in task_ids
    assert "nature" in task_ids
    assert "acs" in task_ids
    assert "aps" in task_ids
    assert "aaa" in task_ids
    assert "elsevier" in task_ids
    assert "cambridge" in task_ids
    assert "rsc" in task_ids


def test_download_task_manager_list(tmp_path: Path):
    root = Path(__file__).resolve().parents[3]
    mgr = DownloadTaskManager(root=root, target=tmp_path)
    res = mgr.list()
    assert "tasks" in res
    assert len(res["tasks"]) >= 7

    # Validate task fields matching contract
    for t in res["tasks"]:
        assert "id" in t
        assert "name" in t
        assert "access" in t
        assert "maturity" in t
        assert "enabled" in t
        assert "status" in t
        assert "pids" in t
        assert "downloaded" in t
        assert "failed" in t
        assert "logPath" in t
        assert "browserGroup" in t
        assert "queuePosition" in t
        assert "canStart" in t

    assert res["maxActiveBrowserGroups"] == 0
    assert {group["id"] for group in res["browserGroups"]} >= {"ieee", "elsevier", "nature"}


def test_empty_target_is_reported_unready_without_starting_download(tmp_path: Path):
    root = Path(__file__).resolve().parents[3]
    manager = DownloadTaskManager(root=root, target=tmp_path)
    result = manager.list()
    assert result["tasks"]
    assert all(item["status"] == "not_ready" and item["canStart"] is False for item in result["tasks"])
    assert get_doi_dashboard_data(tmp_path)["status"] == "ok"


def test_browser_tasks_are_isolated_by_task():
    buaa = DownloadTask("a", "A", "a.py", "a", "buaa_webvpn", "a")
    tju_eds = DownloadTask("b", "B", "b.py", "b", "tju_eds", "b")
    tju_resource = DownloadTask("c", "C", "c.py", "c", "tju_resource_2", "c")
    fudan = DownloadTask("d", "D", "d.py", "d", "fudan_easyconnect", "d")
    direct = DownloadTask("e", "E", "e.py", "e", "direct_http", "")

    assert browser_group_id(buaa) == "a"
    assert browser_group_id(tju_eds) == "b"
    assert browser_group_id(tju_resource) == "c"
    assert browser_group_id(fudan) == "d"
    assert browser_group_id(direct) is None


def test_same_campus_tasks_run_in_parallel(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    root = Path(__file__).resolve().parents[3]
    (tmp_path / "input.csv").write_text("doi\n10.1000/example\n")
    mgr = DownloadTaskManager(root=root, target=tmp_path)
    started = []

    class Running:
        def poll(self):
            return None

    def fake_spawn(task, limit, group_id):
        started.append((task.id, limit, group_id))
        mgr._managed[task.id] = Running()
        if group_id:
            mgr._group_active[group_id] = task.id

    monkeypatch.setattr(mgr, "_spawn_locked", fake_spawn)
    mgr.start("ieee")
    second = mgr.start("springer")

    assert started == [("ieee", 500, "ieee"), ("springer", next(t.batch_size for t in get_download_tasks() if t.id == "springer"), "springer")]
    assert second["status"] == "running"
    assert second["browserGroup"] == "springer"
    assert second["queuePosition"] is None
    with pytest.raises(Exception): mgr.start("ieee")


def test_browser_tasks_start_without_default_limit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    root = Path(__file__).resolve().parents[3]
    (tmp_path / "input.csv").write_text("doi\n10.1000/example\n")
    mgr = DownloadTaskManager(root=root, target=tmp_path)
    started = []

    class Running:
        def poll(self):
            return None

    def fake_spawn(task, limit, group_id):
        started.append(group_id)
        mgr._managed[task.id] = Running()
        if group_id:
            mgr._group_active[group_id] = task.id

    monkeypatch.setattr(mgr, "_spawn_locked", fake_spawn)
    mgr.start("ieee")
    mgr.start("nature")
    third = mgr.start("elsevier")
    fourth = mgr.start("acs")

    assert started == ["ieee", "nature", "elsevier", "acs"]
    assert third["status"] == "running"
    assert fourth["status"] == "running"


@pytest.mark.parametrize('exit_code,runtime_state,progress,processed,expected', [(1,'running',0,20,'failed'),(0,'needs_login',0,20,'needs_login'),(0,'stopped',0,0,'no_output'),(0,'stopped',2,2,'completed'),(0,'stopped',0,20,'completed')])
def test_exit_outcome_is_explicit_and_no_progress_does_not_repeat(tmp_path,monkeypatch,exit_code,runtime_state,progress,processed,expected):
    import json
    root=Path(__file__).resolve().parents[3]
    mgr=DownloadTaskManager(root,tmp_path)
    class Process:
        def wait(self):return exit_code
        def poll(self):return exit_code
    process=Process();mgr._managed['ieee']=process;mgr._desired.add('ieee');mgr._run_baseline['ieee']=0
    folder=tmp_path/'.doi_download_state/runtime';folder.mkdir(parents=True)
    (folder/'ieee.json').write_text(json.dumps({'state':runtime_state,'reason':'fixture reason','processed_records':processed}))
    monkeypatch.setattr(mgr,'_manifest_stats',lambda task:{'downloaded':progress,'failed':0,'lastActivity':None})
    monkeypatch.setattr(mgr,'_schedule_locked',lambda:None)
    monkeypatch.setattr(mgr,'_close_unused_browsers_locked',lambda:None)
    retries=[]
    monkeypatch.setattr(mgr,'_retry_later_locked',lambda task_id:retries.append(task_id))
    mgr._watch_process('ieee',process,'ieee')
    assert retries == (['ieee'] if expected == 'failed' else [])
    status=mgr.status(mgr._task('ieee'),processes=[])
    assert mgr._read_outcome('ieee')['state']==expected
    assert status['status']==('queued' if expected=='completed' else expected)
    assert status['lastExitCode']==exit_code
    assert ('ieee' in mgr._queue)==(expected=='completed')


def test_isolated_tasks_have_distinct_profile_directories(tmp_path):
    mgr=DownloadTaskManager(Path(__file__).resolve().parents[3],tmp_path)
    profiles=[g.profile for g in mgr._browser_groups.values()]
    assert len(profiles)==len(set(profiles))


def test_process_from_other_dataset_is_not_this_task(tmp_path):
    root=Path(__file__).resolve().parents[3];mgr=DownloadTaskManager(root,tmp_path)
    script=str(root/'examples/academic/doi_springer_downloader.py')
    rows=[{'pid':1,'command':f'python {script} {tmp_path}'},{'pid':2,'command':f'python {script} /tmp/other-dataset'}]
    assert [p['pid'] for p in mgr._matching(mgr._task('springer'),rows)]==[1]


def test_delayed_retry_can_be_cancelled_and_advances_queue(tmp_path, monkeypatch):
    mgr = DownloadTaskManager(Path(__file__).resolve().parents[3], tmp_path)
    timers = []
    class Timer:
        def __init__(self, delay, callback):
            assert delay == 60
            self.callback = callback
            timers.append(self)
        def start(self): pass
        def cancel(self): pass
    monkeypatch.setattr('examples.academic.status_server.threading.Timer', Timer)
    monkeypatch.setattr(mgr, '_schedule_locked', lambda: None)
    mgr._desired.add('ieee')
    mgr._retry_later_locked('ieee')
    assert mgr._read_outcome('ieee')['state'] == 'cooldown'
    timers[-1].callback()
    assert list(mgr._queue) == ['ieee']
    mgr._queue.clear()
    mgr._retry_later_locked('ieee')
    mgr._desired.discard('ieee')
    timers[-1].callback()
    assert not mgr._queue


def test_collecting_month_blocks_early_start(tmp_path, monkeypatch):
    from examples.academic import status_server as ss
    monkeypatch.setattr(ss.DownloadTaskManager, '_adopt_shared_browsers', lambda self: None)
    manager = ss.DownloadTaskManager(tmp_path, tmp_path)
    (tmp_path / '.collecting').touch()
    import pytest
    with pytest.raises(ss.TaskControlError, match='正在收集'):
        manager.start('ieee')
    state = manager.status(manager._task('ieee'), processes=[])
    assert state['status'] == 'preparing'
    assert state['enabled'] is False
    assert state['cooldownUntil'] is None


def test_stop_direct_task_does_not_signal_unrelated_processes(tmp_path, monkeypatch):
    monkeypatch.setattr(DownloadTaskManager, '_adopt_shared_browsers', lambda self: None)
    mgr = DownloadTaskManager(tmp_path, tmp_path)
    task = DownloadTask('oa_repository', 'OA', 'doi_oa_repository_downloader.py', 'oa_repository_direct', 'direct_http', '')
    monkeypatch.setattr(mgr, '_task', lambda task_id: task)
    rows = [{'pid': 101, 'ppid': 1, 'command': f'python examples/academic/{task.script} {tmp_path}'},
            {'pid': 202, 'ppid': 1, 'command': 'unrelated browser'}]
    snapshots = iter([rows, [], []])
    monkeypatch.setattr(mgr, '_processes', lambda: next(snapshots))
    signals = []
    monkeypatch.setattr(mgr, '_signal_pids', lambda pids, sig: signals.append(set(pids)))
    monkeypatch.setattr(mgr, '_schedule_locked', lambda: None)
    monkeypatch.setattr(mgr, '_close_unused_browsers_locked', lambda: None)
    monkeypatch.setattr(mgr, 'status', lambda *args, **kwargs: {})
    mgr.stop('oa_repository')
    assert signals[0] == {101}
    assert all(202 not in pids for pids in signals)
    assert mgr._read_outcome(task.id)['state'] == 'stopped'


def test_stop_idle_failure_persists_and_old_watcher_cannot_overwrite(tmp_path, monkeypatch):
    monkeypatch.setattr(DownloadTaskManager, '_adopt_shared_browsers', lambda self: None)
    (tmp_path / "input.csv").write_text("doi\n10.1000/example\n")
    mgr = DownloadTaskManager(tmp_path, tmp_path)
    monkeypatch.setattr(mgr, '_processes', lambda: [])
    monkeypatch.setattr(mgr, '_schedule_locked', lambda: None)
    monkeypatch.setattr(mgr, '_close_unused_browsers_locked', lambda: None)
    class Exited:
        def poll(self): return 1
        def wait(self): return 1
    process = Exited()
    mgr._managed['acs'] = process
    mgr._save_outcome('acs', 'failed', 'prior failure')
    mgr.stop('acs')
    mgr._watch_process('acs', process, None)
    assert mgr._read_outcome('acs')['state'] == 'stopped'
    assert mgr.status(mgr._task('acs'), processes=[])['status'] == 'stopped'


def test_browser_limit_counts_standalone_main_processes(tmp_path, monkeypatch):
    from examples.academic.status_server import CHROME_APP
    monkeypatch.setattr(DownloadTaskManager, '_adopt_shared_browsers', lambda self: None)
    mgr = DownloadTaskManager(root=tmp_path, target=tmp_path, max_parallel=5)
    commands = [f'{CHROME_APP} --user-data-dir=.drission-workflow/profiles/orphan-{i}' for i in range(5)]
    monkeypatch.setattr(mgr, '_processes', lambda: [{'command': c} for c in commands])
    monkeypatch.setattr(mgr, '_browser_alive', lambda group: False)
    started = []
    monkeypatch.setattr(mgr, '_spawn_locked', lambda *args: started.append(args))
    mgr._queue.append('nature')
    mgr._schedule_locked()
    assert not started and list(mgr._queue) == ['nature']
    commands[-1] = str(CHROME_APP)  # A personal browser is outside the task budget.
    commands.append(f'{CHROME_APP} Helper --type=renderer --user-data-dir=.drission-workflow/profiles/orphan-0')
    mgr._schedule_locked()
    assert len(started) == 1 and not mgr._queue
