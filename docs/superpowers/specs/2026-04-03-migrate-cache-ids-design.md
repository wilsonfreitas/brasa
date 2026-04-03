# Migrate Cache Metadata IDs — Design Spec

## Problem

After WIL-34 introduced `DownloadArgs` normalization, the hash computation for `cache_metadata.id` changed. The old code pickled `datetime` objects (from JSON `object_hook` deserialization) while the new code pickles canonical strings (`"YYYY-MM-DDTHH:MM:SS"`). This produces different MD5 hashes for the same logical download arguments.

The scan script (WIL-35) revealed:
- 52,527 total `cache_metadata` rows
- 24,366 rows (46.4%) have IDs that no longer match what the current code would compute
- 0 rows have non-canonical date strings — the mismatch is entirely due to `datetime` objects vs. string representation in pickle

**Impact:** If a user re-downloads or re-processes data for an already-cached entry, the new code computes a different `id`, causing duplicate rows or lookup misses instead of proper cache hits.

## Solution

A one-time manual migration script (`scripts/migrate_cache_ids.py`) that recomputes all `cache_metadata.id` values using the current `DownloadArgs`-based hash and updates both `cache_metadata.id` and `download_trials.cache_id` in a single atomic transaction.

## Design

### Modes

- **Dry-run (default):** Scans all rows, computes the old→new ID mapping, prints a summary. No database writes.
- **Apply (`--apply` flag):** Backs up the database file, then executes the migration in a single SQLite transaction.

### Algorithm

1. Locate the database at `$BRASA_DATA_PATH/meta/meta.db` (or default `.brasa-cache/meta/meta.db`)
2. In `--apply` mode: copy `meta.db` → `meta.db.bak-YYYYMMDD-HHMMSS`
3. `SELECT id, template, download_args, extra_key FROM cache_metadata`
4. For each row:
   - `json.loads(download_args)` → raw dict
   - `DownloadArgs(raw_dict)` → normalized args
   - `generate_checksum_for_template(template, normalized_args, extra_key)` → `new_id`
   - If `old_id != new_id` → add to migration map `{old_id: new_id}`
5. **Collision check:** Verify all `new_id` values are unique. If any two `old_id`s map to the same `new_id`, abort with an error listing the collisions.
6. In `--apply` mode, within a single transaction:
   - For each `(old_id, new_id)`: `UPDATE cache_metadata SET id = ? WHERE id = ?`
   - For each `(old_id, new_id)`: `UPDATE download_trials SET cache_id = ? WHERE cache_id = ?`
   - `COMMIT`
7. Print summary table (same format as scan script)

### Safety guarantees

- **Backup:** File-level copy before any writes
- **Collision detection:** Aborts before any UPDATE if two old IDs would collapse
- **Atomic transaction:** All updates succeed or all roll back
- **Dry-run default:** Must explicitly pass `--apply` to write
- **Idempotent:** Running after migration reports 0 changes needed

### Database tables affected

| Table | Column | Action |
|-------|--------|--------|
| `cache_metadata` | `id` | UPDATE old → new |
| `download_trials` | `cache_id` | UPDATE old → new |

### Not affected

- **Filesystem:** Raw folders are keyed by `download_checksum`, not `id`
- **`dataset_catalog`:** Uses `layer/dataset_name` as its `id`, unrelated
- **Application code:** WIL-34 already handles new data correctly

### Output format

```
Migrate cache_metadata IDs (DownloadArgs normalization)

Template                                    Total    Migrated
-------------------------------------------------------------
b3-bvbg086                                   2041        2041
b3-cotahist-daily                             559         555
...
-------------------------------------------------------------
Total rows:      52527
To migrate:      24366  (46.4%)
Collisions:      0

[DRY RUN] No changes made. Pass --apply to execute migration.
```

In `--apply` mode, the last line becomes:
```
[APPLIED] Migration complete. Backup at meta.db.bak-20260403-143021
```

### Usage

```bash
# Preview changes (safe, no writes)
uv run python scripts/migrate_cache_ids.py

# Execute migration
uv run python scripts/migrate_cache_ids.py --apply
```

### Error scenarios

| Scenario | Behavior |
|----------|----------|
| Database not found | Print error, exit |
| No rows to migrate | Print "Nothing to migrate", exit cleanly |
| Collision detected | Print colliding IDs, abort before any UPDATE |
| Transaction failure | SQLite rolls back automatically |
| Script interrupted during `--apply` | SQLite rolls back; backup file is intact |
