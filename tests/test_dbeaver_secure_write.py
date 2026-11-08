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


def test_atomic_write_replaces_symlink_without_following_it(tmp_path):
    target = tmp_path / "target"
    target.write_text("unchanged\n")
    path = tmp_path / "connections.yaml"
    path.symlink_to(target)
    dbeaver_import._write_text_file_secure(path, "password: synthetic-secret\n")
    assert not path.is_symlink()
    assert target.read_text() == "unchanged\n"
    assert path.read_bytes() == b"password: synthetic-secret\n"
