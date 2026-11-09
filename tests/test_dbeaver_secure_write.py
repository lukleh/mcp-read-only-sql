"""Credential files must be private before writing and replaced atomically."""

import os
import stat

import pytest

from mcp_read_only_sql.config import dbeaver_import


@pytest.mark.parametrize("filename", ["connections.yaml", "connections.yaml.bak.20261108_120000"])
@pytest.mark.parametrize("existing", [False, True])
def test_credentials_are_private_before_first_write(tmp_path, monkeypatch, filename, existing):
    path = tmp_path / filename
    if existing:
        path.write_text("old content\n")
        path.chmod(0o644)
    original_fdopen = os.fdopen
    observed = []

    class RecordingWriter:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return self.handle.__exit__(*args)

        def write(self, content):
            observed.append(stat.S_IMODE(os.fstat(self.handle.fileno()).st_mode))
            assert not path.exists() or path.read_text() == "old content\n"
            return self.handle.write(content)

        def flush(self):
            self.handle.flush()

        def fileno(self):
            return self.handle.fileno()

    monkeypatch.setattr(os, "fdopen", lambda *args, **kwargs: RecordingWriter(original_fdopen(*args, **kwargs)))
    previous_umask = os.umask(0)
    try:
        dbeaver_import._write_text_file_secure(path, "password: synthetic-secret\n")
    finally:
        os.umask(previous_umask)
    assert observed == [0o600]
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert path.read_bytes() == b"password: synthetic-secret\n"
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("operation", ["fsync", "replace"])
def test_failed_atomic_write_preserves_previous_credentials(tmp_path, monkeypatch, operation):
    path = tmp_path / "connections.yaml"
    path.write_text("password: old-secret\n")

    def fail_operation(*args):
        raise OSError("write failed")

    monkeypatch.setattr(os, operation, fail_operation)
    with pytest.raises(OSError, match="write failed"):
        dbeaver_import._write_text_file_secure(path, "password: new-secret\n")
    assert path.read_bytes() == b"password: old-secret\n"
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("filename", ["connections.yaml", "connections.yaml.bak.20261108_120000"])
def test_atomic_write_refuses_symlink_without_changing_target(tmp_path, filename):
    target = tmp_path / "target"
    target.write_text("unchanged\n")
    target.chmod(0o644)
    path = tmp_path / filename
    path.symlink_to(target)
    with pytest.raises(ValueError, match="Refusing symlinked output"):
        dbeaver_import._write_text_file_secure(path, "password: synthetic-secret\n")
    assert path.is_symlink()
    assert target.read_text() == "unchanged\n"
    assert stat.S_IMODE(target.stat().st_mode) == 0o644


@pytest.mark.parametrize("unchanged", [False, True])
def test_import_refuses_symlink_before_chmod_or_backup(tmp_path, monkeypatch, unchanged):
    from tests.test_dbeaver_import import _run_import, _write_dbeaver_workspace

    workspace = _write_dbeaver_workspace(tmp_path, [{
        "id": "c1", "name": "Analytics", "configuration": {"host": "ch1", "port": "8123"},
    }])
    monkeypatch.setattr(dbeaver_import.DBeaverImporter, "_decrypt_credentials", lambda self: ({}, {}))
    target = tmp_path / "target.yaml"
    _run_import(monkeypatch, [str(workspace), "--output", str(target)])
    if not unchanged:
        target.write_text("[]\n")
    original = target.read_bytes()
    target.chmod(0o644)
    output = tmp_path / "connections.yaml"
    output.symlink_to(target)
    with pytest.raises(SystemExit) as error:
        _run_import(monkeypatch, [str(workspace), "--output", str(output)])
    assert error.value.code == 1
    assert output.is_symlink()
    assert target.read_bytes() == original
    assert stat.S_IMODE(target.stat().st_mode) == 0o644
    assert not list(tmp_path.glob("*.bak.*"))
