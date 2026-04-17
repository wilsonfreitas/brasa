# WIL-29 — B3 Zip Checksum Vulnerability

## Context

Downloads from B3 URLs like `https://www.b3.com.br/pesquisapregao/download?filelist=PR%y%m%d.zip`
return a zip file that in turn contains another zip file. The downloaded `BytesIO` is passed to
`generate_checksum_from_file` (`brasa/util.py:127`), which computes an MD5 over the **raw outer
container bytes**.

Zip containers include non-deterministic metadata — file modification timestamps, OS byte, extra
fields, central-directory ordering — that can vary between requests even when the logical payload
(inner files) is identical. When B3 regenerates the zip on each request, the outer bytes differ,
producing different checksums for identical content. This breaks the deduplication check at
`brasa/engine/download.py:149`: `raw/{template}/{checksum}/` folders are never matched as duplicates,
wasting storage and leaving orphaned raw folders behind.

**Goal:** replace the raw-bytes checksum with a **content-based** checksum that hashes only the
logical file contents inside the zip (including recursive inner zips), so identical payloads always
produce identical checksums regardless of container metadata. Apply this only for templates whose
downloader is configured with `format: zip`; all other formats keep the current behavior.

**Templates directly affected** (all use `format: zip` + `datetime_download`):
- `b3-bvbg086` (`PR%y%m%d.zip`, market prices, outer zip → inner zip → multiple versioned XMLs)
- `b3-bvbg087` (`IR%y%m%d.zip`, indexes, outer zip → inner zip → XMLs)
- `b3-bvbg028` (`IN%y%m%d.zip`, instruments, outer zip → inner zip → multiple versioned XMLs)

## Approach

1. **Add `generate_checksum_from_zip(fp: IO) -> str`** in `brasa/util.py` — a purely in-memory,
   recursive, deterministic hash of a zip's logical contents.
2. **Integrate it at the single download integration point** (`brasa/engine/download.py:146`) with a
   conditional on `template.downloader.format == "zip"`.
3. **Test** the new helper directly plus an integration test that downloads a real B3 zip twice and
   asserts the checksums match.

### Algorithm for `generate_checksum_from_zip`

Work entirely in-memory (no disk I/O, no temp files) using `zipfile.ZipFile` over `BytesIO`:

```
hash = md5()
open fp as zipfile.ZipFile
for each name in sorted(zf.namelist()):
    hash.update(name.encode("utf-8"))
    hash.update(b"\x00")            # separator (name/content boundary)
    content = zf.read(name)
    if zipfile.is_zipfile(BytesIO(content)):
        inner_checksum = recurse on BytesIO(content)
        hash.update(inner_checksum.encode("ascii"))
    else:
        hash.update(content)
    hash.update(b"\x00")            # entry terminator
fp.seek(0)
return hash.hexdigest()
```

Properties that matter:
- **Order-independent within the container** — entries are sorted before hashing, so a different
  central-directory ordering of the same logical files produces the same hash.
- **Name-sensitive** — if a new versioned XML is added (e.g. `BVBG.086.01_250328_02.xml`), the hash
  changes. This is the intended behavior: a new version is legitimately new content.
- **Recursive** — nested zips are hashed structurally, not by their container bytes.
- **Safe** — a recursion depth cap (e.g. 8) guards against pathological nesting / zip bombs.
- **Preserves `fp` state** — seeks back to 0 on exit so the downstream `shutil.copyfileobj` at
  `download.py:160` still writes the original bytes to disk (we only change what we *hash*, not
  what we *store*).

### Why not reuse `unzip_recursive`?

`unzip_recursive` (`brasa/util.py:150`) extracts to `gettempdir()`, requires a filename, and leaves
temp files around. For checksum computation we want a pure, side-effect-free, `IO`-based function —
cleaner, faster, and testable without filesystem setup. The extraction path during actual download
processing continues to use `unzip_recursive` unchanged.

## Tasks

- [ ] **Task 1 — Diagnostic reproduction.** Write a one-off script (or pytest marked `integration`)
  that downloads the same B3 zip twice via `datetime_download` and asserts that (a) raw checksums
  differ and (b) content-based checksums match. Confirms the hypothesis and gives a regression
  fixture.
- [ ] **Task 2 — Implement `generate_checksum_from_zip`** in `brasa/util.py` following the algorithm
  above. Add `io` import if not present. Include docstring explaining the recursive contract and
  the `fp.seek(0)` guarantee.
- [ ] **Task 3 — Unit tests** in `tests/test_zip_checksum.py`:
  - Identical content with different zip metadata → same checksum (fabricate two zips in-memory
    with different `ZipInfo.date_time` but same payload).
  - Different content → different checksum.
  - Nested zip: outer containing inner containing files → stable across container rebuilds.
  - Reordered entries in the central directory → same checksum (sorted iteration).
  - Adding a new file → different checksum.
  - `fp.seek(0)` is honored (post-call read returns full bytes).
  - Recursion depth cap raises cleanly.
- [ ] **Task 4 — Integrate at download site.** In `brasa/engine/download.py:146`, branch on
  `template.downloader.format`:
  ```python
  if template.downloader.format == "zip":
      checksum = generate_checksum_from_zip(fp)
  else:
      checksum = generate_checksum_from_file(fp)
  ```
  Update the import at `download.py:12`.
- [ ] **Task 5 — Integration verification.** Add a test (marked `@pytest.mark.integration`) that
  invokes `_download_marketdata` twice against `b3-bvbg086` for the same `refdate` and asserts the
  resulting `meta.download_checksum` is stable across runs.
- [ ] **Task 6 — Delete the standalone plan document.** Remove
  `docs/superpowers/plans/2026-03-29-investigate-zip-checksum-vulnerability.md` — the refined plan
  lives in the Linear issue from now on.

## Critical Files

| File | Role |
|---|---|
| `brasa/util.py:127-132` | `generate_checksum_from_file` — reference, keep as-is |
| `brasa/util.py` (new) | Add `generate_checksum_from_zip` after `generate_checksum_from_file` |
| `brasa/engine/download.py:12` | Update import |
| `brasa/engine/download.py:146` | Conditional checksum dispatch on `template.downloader.format` |
| `brasa/engine/template.py:356-378` | `MarketDataDownloader.format` — already available, just read |
| `brasa/engine/cache.py:189-197` | `CacheMetadata.download_folder` — unchanged, confirms layout |
| `tests/test_zip_checksum.py` (new) | Unit tests for the new helper |
| `tests/test_downloads.py` | Add integration test for stable checksum across runs |
| `docs/superpowers/plans/2026-03-29-investigate-zip-checksum-vulnerability.md` | Delete |

## Acceptance Criteria

- Two consecutive downloads of the same B3 zip (e.g. `b3-bvbg086` for a given refdate) produce
  **identical** `meta.download_checksum`, even when the raw outer-container bytes differ.
- A download for a different refdate, or for the same refdate with a new inner versioned XML,
  produces a **different** checksum.
- Non-zip formats (`base64`, raw) are unaffected — still use `generate_checksum_from_file`.
- No new temp files or disk I/O introduced by the checksum computation.
- All new unit tests pass offline; the integration test is marked `@pytest.mark.integration` and
  skippable with `--no-integration`.
- `uv run pytest`, `uv run ruff check .`, `uv run ruff format --check .`, and
  `uv run pre-commit run --all-files` all pass (Definition of Done).

## Verification

```bash
# Unit tests (offline)
uv run pytest tests/test_zip_checksum.py -v

# Integration tests against real B3 endpoints
uv run pytest tests/test_downloads.py -v -k "bvbg086"

# Full suite + lint + pre-commit
uv run pytest
uv run ruff check . && uv run ruff format --check .
uv run pre-commit run --all-files
```

Manual end-to-end check:

```python
from brasa import download_marketdata
# Run twice — should not create a duplicate folder under raw/b3-bvbg086/
download_marketdata("b3-bvbg086", refdate="2026-03-28")
download_marketdata("b3-bvbg086", refdate="2026-03-28")  # expect DuplicatedFolderException or cache hit
```

## Known Risks / Notes

- **Cache migration**: existing `raw/b3-bvbg086/{old_checksum}/` folders will no longer be matched
  after the fix. They become orphans. The existing `doctor` tooling
  (`brasa/engine/doctor.py:210-242`) already detects orphaned folders — run it after deployment to
  clean up.
- **Recursion cap**: set conservatively (depth 8). Real B3 files only nest one level; the cap is
  purely defensive against pathological inputs.
- **No backwards-compat shim**: this is a bug fix, not a schema change — old checksums are simply
  wrong and should be replaced.
