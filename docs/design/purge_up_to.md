# `binlog_server purge_up_to <config> <binlog_name>`

Status: **Approved** &nbsp;·&nbsp; Owner: storage subsystem &nbsp;·&nbsp; Related: [`list.md`](./list.md)

> **Phasing.** The shipped v1 covers only the happy-path purge described in
> §3–§5.1 and the empty-storage end state in §5.5. The crash-recovery
> self-healing described in §5.4 is intentionally **phase 2** and is not
> implemented yet — if a `purge_up_to` aborts mid-operation, the next
> `binlog_server` startup will fail at the existing storage validators
> (extra payload files, dangling `.json` metadata, or a stray
> `binlog.index.tmp` left over by the backend's atomic-overwrite
> implementation), requiring manual cleanup. Self-healing will be
> addressed in a follow-up once the basic command has soaked.

## 1. Goal

Make it possible to reclaim space in a binlog storage by removing a
contiguous **prefix** of binlog files. The user names a single binlog
file `<binlog_name>`; the command removes that file and every file
older than it, and updates `binlog.index` accordingly.

## 2. Assumption: single live process per storage

Exactly one `binlog_server` process touches a given storage directory
at any time. The operator is responsible for stopping any running
`fetch` / `pull` (and in the future, any `serve`) before issuing
`purge_up_to`.

The storage directory is treated as a single-writer datastore, the same
way a SQLite file or an LMDB environment treats its home. We do **not**
enforce this in code — no PID file, no advisory lock, no IPC claim
registry. The original design that did enforce this with
`fcntl(F_OFD_SETLK)` byte-range locks is preserved in section §7
("Alternatives considered") for future reference, in case the
single-process assumption ever needs to be relaxed.

## 3. CLI

```
binlog_server purge_up_to <json_config_file> <binlog_name>
```

4 arguments total — same shape as `search_by_timestamp` /
`search_by_gtid_set`. The config is required because it identifies the
storage (`storage.uri`) and the replication mode against which the
on-disk state is validated.

`<binlog_name>` is a composite binlog name (e.g. `binlog.000007`) and
is parsed with `binsrv::composite_binlog_name::parse`.

The full CLI surface after this change is documented in
[`list.md` §2](./list.md#2-cli).

### 3.1. Backend support

v1 supports the **`file` backend only**. If `storage.backend == s3`,
the handler refuses with:

> `"purge_up_to is only supported on local filesystem storage"`

S3 is a straightforward follow-up (see §6).

## 4. Behaviour

- Output: JSON on `stdout`. On success, a minimal `ok` response
  reporting the purged range; on failure, a standard `error_response`.
  Exit code is 0 / 1 respectively, matching the existing
  `search_by_*` commands.
- Order: removal proceeds oldest → newest within the victim set,
  bracketed by a single atomic index rewrite (see §5.1).
- Idempotency: calling `purge_up_to` with a name that has already been
  removed is an error (`"no such binlog file"`), not a silent no-op.
  Re-running with a still-present older name is fine; it will only
  remove the still-present prefix.
- Concurrency: per §2 it is the operator's responsibility to ensure no
  other `binlog_server` is touching the directory. The implementation
  performs no runtime check for this.

## 5. Implementation outline

### 5.1. Algorithm

Let `S = target.get_sequence_number()` and let `victims` be the
contiguous prefix of `binlog_records_` with sequence ≤ S. (Records are
already loaded in sequence order — invariant of `load_binlog_index`.)

```
0. Validate <target>:
   - well-formed                (composite_binlog_name::parse)
   - present in binlog_records_ (else: "no such binlog file")
   - target.base_name matches the front of binlog_records_
     (cheap sanity — one base_name per storage today)

1. Build the new binlog.index body (only the survivors).

2. Rewrite binlog.index atomically:
   - backend->put_object("binlog.index", new_body)
   - backend's put_object is contractually atomic-overwrite (see §5.3),
     so this is a single call - no temp/rename plumbing in the storage
     layer

3. Drop `victims` from in-memory binlog_records_ and update
   purged_gtids_ to the new front's previous_gtids (in GTID mode).

4. For each victim (oldest -> newest):
   - backend->remove_object(generate_binlog_metadata_name(victim))
   - backend->remove_object(victim.str())
   Errors here are logged but do not abort — the index already
   excludes them, so any leftover is recoverable on next startup
   (see §5.4).

5. fsync the directory once more, return success JSON.
```

The hazardous window is **between step 2 and step 4**: the index
already excludes the victims, but their payload / `.json` files may
still be on disk. See §5.4.

### 5.2. New `storage_construction_mode_type`

```cpp
enum class storage_construction_mode_type : std::uint8_t {
  querying_only,
  streaming,
  purging        // <-- new
};
```

`purging`:

- behaves like `querying_only` on construction (load index, validate,
  load per-binlog metadata, no writes),
- is the *only* mode allowed to call the new `storage::purge_up_to`,
- is forbidden from calling `open_binlog`, `write_event`,
  `close_binlog`, etc. (a new `ensure_purging_mode()` mirrors the
  existing `ensure_streaming_mode()`).

### 5.3. New / strengthened backend operations

```cpp
// binsrv::basic_storage_backend
void put_object(std::string_view name, util::const_byte_span content);
   // contract STRENGTHENED to atomic-overwrite: a reader (or the
   // next-startup constructor) sees either the previous bytes in full
   // or the new bytes in full, never a partial mix
void remove_object(std::string_view name);   // new — unlink + (TODO) fsync dir
```

`do_*` overrides:

- `filesystem_storage_backend`:
  - `do_put_object` implements atomic-overwrite via the canonical POSIX
    write-temp-then-rename idiom (`<name>.tmp` + `rename(2)`). A crash
    mid-write leaves only `<name>.tmp`, never a truncated `<name>`; a
    subsequent legitimate put for the same name simply truncates and
    overwrites the stale tmp before the rename, so no explicit cleanup
    is needed on the happy path.
  - `do_remove_object` wraps `std::filesystem::remove`. TODO: `fsync`
    the parent directory after each `rename` / `remove` for full
    durability against power loss.
- `s3_storage_backend`:
  - `do_put_object` is already atomic per key — S3 `PutObject` either
    publishes the new bytes in full or fails, leaving the previous
    object intact. No change required.
  - `do_remove_object` raises `std::logic_error` for v1; phase 2 will
    map it to S3 `DeleteObject`. The handler also refuses
    `purge_up_to` against an S3 storage_config up front (see §3.1),
    so the stub is purely defense-in-depth.

Rejected alternative: expose a `rename_object` primitive in the
backend API. It would push the temp-file mechanic into the storage
layer and force an awkward `Copy + Delete` shape on S3, where
`PutObject` is already atomic. Encapsulating the atomicity inside
`put_object` is a strictly simpler abstraction and makes S3 support
in phase 2 nearly free.

### 5.4. Crash recovery — self-healing on startup

If `purge_up_to` is killed between steps 2 and 4, the directory ends
up with payload / `.json` files that are not referenced by the
already-rewritten `binlog.index`. Today's `validate_binlog_index`
rejects this as `"storage contains an object that is not referenced
in the binlog index"`.

Resolution: extend `validate_binlog_index` so that the *specific*
discrepancy "files present on disk whose composite name has a
sequence number strictly less than the front of the index" is
**not** treated as an error. Instead the orphans are deleted on the
spot and the event is logged at `info` level
(`"storage: removed N purge-leftover binlog object(s)"`).

Trigger is narrow and unambiguous:

- only objects whose name parses as a valid composite binlog name
  (or `<composite>.json`) are eligible,
- only sequence numbers strictly below the index front qualify,
- any other discrepancy (e.g. a gap *inside* the index, an object
  with a sequence number ahead of the front, a random unrelated
  file) still raises, exactly as today.

This makes `purge_up_to` effectively atomic from the operator's
point of view: either it ran to completion, or the next
`binlog_server` invocation (any subcommand that constructs `storage`)
silently finishes the job.

### 5.5. Edge case — purge to the last file

When `<binlog_name>` equals the current tail, every record is in
`victims` and the storage becomes logically empty. The chosen
behaviour:

- write a zero-byte `binlog.index` (do not delete it),
- keep `metadata.json` untouched (so `replication_mode` is preserved
  across the purge),
- the next `pull` / `fetch` against this storage starts from scratch,
  the same way it would against a directory that only contains
  `metadata.json` and an empty `binlog.index`.

`load_binlog_index` already handles an empty index file as
"zero records", so no extra code is needed for this case beyond
ensuring we still go through steps 2 and 4 even when `victims` is
the entire `binlog_records_`.

### 5.6. Files touched / added

```
src/binsrv/operation_mode_type.hpp           (+ purge_up_to enumerator)
src/binsrv/storage_fwd.hpp                   (+ purging mode)
src/binsrv/basic_storage_backend.hpp/.cpp    (+ remove_object;
                                              put_object contract
                                              strengthened to
                                              atomic-overwrite)
src/binsrv/filesystem_storage_backend.*      (implement remove_object;
                                              rewrite put_object with
                                              tmp+rename idiom)
src/binsrv/s3_storage_backend.*              (stub remove_object;
                                              put_object unchanged -
                                              already atomic)
src/binsrv/storage.hpp/.cpp                  (+ purge_up_to,
                                              ensure_purging_mode,
                                              phase-2 self-healing in
                                              validate_binlog_index)
src/app.cpp                                  (+ handle_purge_up_to,
                                              wire into check_cmd_args / usage)
```

No JSON config schema changes. No new utility module.

## 6. S3 backend (future)

The algorithm applies verbatim. S3 `PutObject` is already atomic per
key, so the storage-layer `save_binlog_index()` call from step 2
"just works" — no temp file, no rename. The only remaining piece is
mapping `do_remove_object` to S3 `DeleteObject` (a one-liner against
the existing `aws_context`).

Caveats worth documenting alongside the future S3 support:

- Bucket versioning: `DeleteObject` against a versioned bucket only
  writes a delete marker — storage is not actually reclaimed until
  the lifecycle policy expires the old versions. Operator concern,
  not a `binlog_server` concern.
- Object lock / WORM: blocks both overwrite and delete; incompatible
  with `purge_up_to`.
- The same step-2 → step-4 window still applies on S3; the
  phase-2 self-healing rule from §5.4 works unchanged.

## 7. Alternatives considered

### 7.1. IPC claim registry with `fcntl(F_OFD_SETLK)` (rejected)

The original design assumed multiple cooperating `binlog_server`
processes could share one storage directory. To make `purge_up_to`
safe in that world it proposed:

- a single zero-byte `binlog.lock` file in the storage root,
- byte-range advisory locks on it (`F_OFD_SETLK` on Linux,
  `F_SETLK` on macOS), where byte offset = sequence number,
- claims:
  - streaming writer: SHARED `[current_seq, current_seq]`,
  - forward serving reader (future): SHARED `[R, +∞)`,
  - purger: EXCLUSIVE `[0, S]`,
- `purge_up_to` would atomically `try_lock` its EXCLUSIVE range and
  refuse with `"binlog file(s) up to <target> are in use by another
  binlog_server process"` on conflict.

This was rejected once we adopted the single-process assumption in
§2. It is intentionally kept here so that if/when multi-process
sharing becomes a requirement (e.g. once a `serve` subcommand
exists), the design can be revisited without re-deriving it from
scratch. The full original write-up lives in chat history under
the initial `delete_binlogs` design rounds.

### 7.2. Strict validator + separate `cleanup` command (rejected)

An alternative to §5.4's self-healing was to keep
`validate_binlog_index` strict and add a separate
`binlog_server cleanup <config>` command that operators must run by
hand after a crashed purge. Rejected because the trigger for §5.4 is
narrow and unambiguous — there is no operational benefit to
forcing a manual step.

### 7.3. Delete payloads first, then rewrite index (rejected)

The inverted order leaves the index referencing nonexistent files on
crash, which today's `validate_binlog_index` also rejects. The chosen
order (§5.1) leaves orphan files instead, which is the easier of the
two states to detect and clean up safely.

## 8. Tests (mtr) — sketch

1. **Happy path** — produce a few binlogs with `pull`, stop it,
   `purge_up_to binlog.000003`, restart `pull` and verify it appends
   to the existing tail (`000004`); index lists only `000004+`; old
   `.json` and payloads are gone.
2. **Purge everything** — `purge_up_to <last_file>` empties the
   storage; subsequent `pull` starts from scratch; `metadata.json` is
   retained; `binlog.index` is present and empty.
3. **Wrong base name / nonexistent file / malformed name** — error
   JSON, exit 1, directory unchanged.
4. **Crash recovery (§5.4)** — kill the purger between steps 2 and 4
   (test harness inserts a `SIGKILL` after `save_binlog_index` returns);
   the next startup auto-cleans the orphans without complaint and
   produces an `info`-level log line. *Phase 2.*
5. **S3 backend refusal** — `purge_up_to` against an `s3://` storage
   returns the error described in §3.1 and exits 1.
