# Download Folder: Use `meta.id` Instead of `download_checksum` Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix a gap where `DuplicatedFolderException` fails to guard against re-downloading the same logical entry when the server returns a non-deterministic binary (e.g., zip files with embedded timestamps).

**Architecture:** `CacheMetadata.download_folder` currently uses `download_checksum` (MD5 of the raw server response) as the folder name. Since the checksum varies with non-deterministic server responses, two downloads of the same logical entry (`meta.id`) land in different folders and bypass the duplicate detection guard. The fix is to make `download_folder` derive from `meta.id` (which is deterministic for the same template + download_args + extra_key). `download_checksum` remains stored in metadata for content-change auditing, but is no longer used for folder naming.

**Tech Stack:** Python, pytest, uv

---

### Task 1: Understand the current behavior and write a failing test

**Files:**
- Test: `tests/test_cache.py`

**Step 1: Read the relevant code**

Read `brasa/engine/cache.py` lines 175–190 (the `download_folder` property and `id` property).
Read `tests/test_cache.py` to find a good place to add the new test.

**Step 2: Write the failing test**

Add this test to `tests/test_cache.py`:

```python
def test_download_folder_is_stable_across_different_checksums():
    """download_folder must not change when download_checksum changes for the same entry."""
    meta = CacheMetadata("some-template")
    meta.download_args = {"refdate": "2024-01-02"}
    meta.extra_key = ""

    meta.download_checksum = "checksum_a"
    folder_a = meta.download_folder

    meta.download_checksum = "checksum_b"
    folder_b = meta.download_folder

    assert folder_a == folder_b, (
        "download_folder must be deterministic per meta.id, "
        "not vary with download_checksum"
    )
```

**Step 3: Run it to verify it fails**

```bash
uv run pytest tests/test_cache.py::test_download_folder_is_stable_across_different_checksums -v
```

Expected: FAIL — `folder_a != folder_b` because the current implementation embeds `download_checksum` in the path.

---

### Task 2: Fix `download_folder` to use `meta.id`

**Files:**
- Modify: `brasa/engine/cache.py` (the `download_folder` property, ~line 183)

**Step 1: Update the property**

Current code (around line 182–190):

```python
@property
def download_folder(self) -> str:
    """Path to the download folder for this cache entry."""
    if self.download_checksum == "":
        return ""
    else:
        return str(
            Path(CacheManager._raw_folder) / self.template / self.download_checksum
        )
```

Replace with:

```python
@property
def download_folder(self) -> str:
    """Path to the download folder for this cache entry.

    Based on ``meta.id`` (template + download_args + extra_key) so the
    path is deterministic regardless of the raw server response checksum.
    ``download_checksum`` is still stored for content-change auditing.
    """
    if self.id == "":
        return ""
    return str(Path(CacheManager._raw_folder) / self.template / self.id)
```

**Step 2: Run the new test**

```bash
uv run pytest tests/test_cache.py::test_download_folder_is_stable_across_different_checksums -v
```

Expected: PASS.

**Step 3: Run the full test suite**

```bash
uv run pytest --no-integration
```

Expected: all pass. If any test checks the literal folder path format (e.g. asserting it contains the checksum), update that test to reflect the new scheme.

**Step 4: Commit**

```bash
git add brasa/engine/cache.py tests/test_cache.py
git commit -m "fix: base download_folder on meta.id instead of download_checksum

Prevents non-deterministic server responses (e.g. zip files with
embedded timestamps) from creating multiple raw folders for the same
logical cache entry, which silently bypassed DuplicatedFolderException."
```

---

### Task 3: Verify linting and pre-commit

**Step 1: Run ruff**

```bash
uv run ruff check . && uv run ruff format --check .
```

Fix any issues with:

```bash
uv run ruff check . --fix && uv run ruff format .
```

**Step 2: Run pre-commit**

```bash
uv run pre-commit run --all-files
```

Expected: all hooks pass.

**Step 3: Commit any formatting fixes**

```bash
git add -u
git commit -m "style: ruff formatting fixes"
```

---

## Notes

- The old folder layout was `raw/{template}/{download_checksum}/`. The new layout is `raw/{template}/{meta.id}/`. Any existing cache on disk will have folders named by checksum — they will become orphans. The `doctor` module's `check_orphan_raw()` (if it exists) should eventually clean these up; no migration is needed for correctness.
- `download_checksum` keeps its role as a content fingerprint stored in the DB metadata row. It can be used in future work to detect when a server silently changes the content of a file.
