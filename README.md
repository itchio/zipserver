[![test](https://github.com/itchio/zipserver/actions/workflows/test.yml/badge.svg)](https://github.com/itchio/zipserver/actions/workflows/test.yml)

# zipserver

Zipserver is an HTTP service and CLI tool for working with zip files on Google Cloud Storage. It can extract zip files, copy files between storage targets, download URLs to storage, and more.

## Installation

```bash
go install github.com/itchio/zipserver@latest
```

## Configuration

Create a config file `zipserver.json`:

```json
{
  "PrivateKeyPath": "path/to/service/key.pem",
  "ClientEmail": "111111111111@developer.gserviceaccount.com",
  "Bucket": "my-bucket",
  "ExtractPrefix": "extracted"
}
```

More config settings can be found in `zipserver/config.go`.

## Limits

All limits are configured in `zipserver.json` using the names below. For `extract` and `list`, you can override limits per request via HTTP query parameters. CLI overrides are available for `extract` via flags, and `--threads` can override `ExtractionThreads`. A value of `0` means "unbounded" for all limits except `ExtractionThreads` (which uses `GOMAXPROCS`, and is forced to at least 1).

| Config field | Applies to | Description | Default |
| --- | --- | --- | --- |
| `MaxInputZipSize` | list, extract | Maximum size (bytes) of the input zip file (compressed). | `104857600` |
| `MaxFileSize` | extract | Maximum uncompressed size (bytes) for a single file. | `209715200` |
| `MaxTotalSize` | extract | Maximum total uncompressed size (bytes) across all files. | `524288000` |
| `MaxNumFiles` | extract | Maximum number of files in the archive. | `100` |
| `MaxFileNameLength` | extract | Maximum path length for a file name in the archive. | `255` |
| `ExtractionThreads` | extract | Number of worker threads used during extraction. | `4` |
| `MaxListFiles` | list | Maximum number of files returned by list. | `50000` |

## Usage

Zipserver can run as an HTTP server or execute operations directly via CLI commands. Operational commands output JSON; `server`, `testzip`, `dump`, and `version` print human-readable output.

```bash
zipserver --help              # Show all commands
zipserver <command> --help    # Show help for a specific command
```

### Commands

| Command | Description | Storage | HTTP Endpoint |
|---------|-------------|---------|---------------|
| `server` | Start HTTP server (default) | n/a | |
| `extract` | Extract a zip file to storage | Source read, optional target write | `/extract` |
| `copy` | Copy a file to target storage | Source read, target write | `/copy` |
| `delete` | Delete files from storage | Target write | `/delete` |
| `list` | List files in a zip archive | Source read, URL, or local file | `/list` |
| `slurp` | Download a URL and store it | Source write, or optional target write | `/slurp` |
| `testzip` | Extract and serve a local zip file via HTTP for debugging | local only | |
| `dump` | Dump parsed config and exit | n/a | |
| `version` | Print version information | n/a | |

## HTTP Server

Start the server:

```bash
zipserver server --listen 127.0.0.1:8090
```

**Warning:** This HTTP server exposes unauthenticated operations on your storage bucket. It's recommended to avoid public network interfaces.

### Monitoring Endpoints

| Endpoint | Description |
|----------|-------------|
| `/status` | Show currently running tasks (held locks per operation type) |
| `/metrics` | Prometheus-compatible metrics |

**Example `/status` response:**
```json
{
  "copy_locks": [],
  "extract_locks": [{"Key": "s3backup:zips/large.zip", "LockedAt": "...", "LockedSeconds": 12.5}],
  "slurp_locks": [],
  "delete_locks": []
}
```

## Extract

Extract a zip file and upload individual files to a prefix.

**CLI:**
```bash
# Extract from storage key
zipserver extract --key zips/my_file.zip --prefix extracted/

# Extract from local file
zipserver extract --file ./local.zip --prefix extracted/

# With limits
zipserver extract --key zips/my_file.zip --prefix extracted/ \
  --max-file-size 10485760 --max-num-files 100

# Override extraction threads (global flag)
zipserver --threads 8 extract --key zips/my_file.zip --prefix extracted/

# With a target storage and file filter
zipserver extract --key zips/my_file.zip --prefix extracted/ \
  --target s3backup --filter "assets/**/*.png"

# Extract specific files by exact path
zipserver extract --key zips/my_file.zip --prefix extracted/ \
  --only-file "readme.txt" --only-file "images/logo.png"
```

**HTTP API:**
```bash
curl "http://localhost:8090/extract?key=zips/my_file.zip&prefix=extracted"

# With a target storage and file filter
curl "http://localhost:8090/extract?key=zips/my_file.zip&prefix=extracted&target=s3backup&filter=assets/**/*.png"

# Extract specific files by exact path
curl "http://localhost:8090/extract?key=zips/my_file.zip&prefix=extracted&only_files[]=readme.txt&only_files[]=images/logo.png"
```

Note: `--filter` (glob pattern) and `--only-file` (exact paths) are mutually exclusive.

## Copy

Copy a file from primary storage to a target storage (e.g., S3).

**CLI:**
```bash
zipserver copy --key path/to/file.zip --target s3backup
```

**HTTP API:**
```bash
curl "http://localhost:8090/copy?key=path/to/file.zip&target=s3backup&callback=http://example.com/done"
```

## Delete

Delete files from a target storage.

**CLI:**
```bash
zipserver delete --key file1.zip --key file2.zip --target s3backup
```

**HTTP API:**
```bash
curl -X POST "http://localhost:8090/delete" \
  -d "keys[]=file1.zip" \
  -d "keys[]=file2.zip" \
  -d "target=s3backup" \
  -d "callback=http://example.com/done"
```

## List

List files in a zip archive without extracting. Returns JSON with filenames and uncompressed sizes.

**CLI:**
```bash
# From storage (uses efficient range requests - only reads zip metadata)
zipserver list --key zips/my_file.zip

# From URL (downloads entire file)
zipserver list --url https://example.com/file.zip

# From local file
zipserver list --file ./local.zip
```

When using `--key`, zipserver uses HTTP range requests to read only the zip's central directory (typically < 1% of the file size). This significantly reduces bandwidth and storage operation costs for large zip files.

**HTTP API:**
```bash
curl "http://localhost:8090/list?key=zips/my_file.zip"
```

The HTTP API also uses range requests when listing by key.

## Slurp

Download a file from a URL and store it in storage.

**CLI:**
```bash
# Store in primary storage
zipserver slurp --url https://example.com/file.zip --key uploads/file.zip

# Store in a target storage
zipserver slurp --url https://example.com/file.zip --key uploads/file.zip --target s3backup
```

**HTTP API:**
```bash
curl "http://localhost:8090/slurp?url=https://example.com/file.zip&key=uploads/file.zip"

# With target storage
curl "http://localhost:8090/slurp?url=https://example.com/file.zip&key=uploads/file.zip&target=s3backup"
```

## Testzip (Local)

Extract and serve a local zip file via HTTP for debugging:

```bash
zipserver testzip ./my_file.zip

# With filtering
zipserver testzip ./my_file.zip --filter "*.png"
zipserver testzip ./my_file.zip --only-file "readme.txt"
```

## Storage Targets

The top-level storage settings in `zipserver.json` (for example
`PrivateKeyPath`, `ClientEmail`, `Bucket`) define the primary/source storage
used for reads and default writes. You can also configure additional storage
targets (S3 or GCS) for `copy`, `delete`, `extract`, and `slurp` operations.
When a target is specified (for example `--target s3backup` or
`target=s3backup`), reads still come from the primary/source storage and writes
go to the target bucket. Targets marked `Readonly` cannot be written to.

Example target entries:

```json
{
  "StorageTargets": [
    {
      "Name": "s3backup",
      "Type": "S3",
      "S3AccessKeyID": "...",
      "S3SecretKey": "...",
      "S3Endpoint": "s3.amazonaws.com",
      "S3Region": "us-east-1",
      "Bucket": "my-backup-bucket"
    },
    {
      "Name": "gcsbackup",
      "Type": "GCS",
      "GCSPrivateKeyPath": "/path/to/target/key.pem",
      "GCSClientEmail": "target-service@project.iam.gserviceaccount.com",
      "Bucket": "my-gcs-backup-bucket"
    }
  ]
}
```

## Callbacks (Async Mode)

Some HTTP handlers support callbacks to notify your application when long-running operations complete. This allows you to immediately return a response to the client while the operation continues in the background.

### Supported Handlers

| Endpoint | Parameter | Sync Mode Available |
|----------|-----------|---------------------|
| `/extract` | `async` | Yes (omit `async` for sync) |
| `/slurp` | `async` | Yes (omit `async` for sync) |
| `/copy` | `callback` | No (always async) |
| `/delete` | `callback` | No (always async) |

### How It Works

1. Provide a callback URL via the `async` or `callback` parameter
2. The server immediately returns `{"Processing": true, "Async": true}`
3. The operation runs in the background
4. On completion, the server POSTs the result to your callback URL

The callback is sent as a POST request with `Content-Type: application/x-www-form-urlencoded`.

### Callback Response Fields

**On success**, callbacks include `Success=true` plus operation-specific fields:

| Endpoint | Success Fields |
|----------|----------------|
| `/extract` | `ExtractedFiles[N][Key]`, `ExtractedFiles[N][Size]` for each file |
| `/slurp` | (none beyond `Success=true`) |
| `/copy` | `Key`, `Duration`, `Size`, `Md5` |
| `/delete` | `TotalKeys`, `DeletedKeys`, `Errors` (JSON array if any) |

**On error**, callbacks include:

| Endpoint | Error Fields |
|----------|--------------|
| `/extract` | `Type=ExtractError`, `Error=<message>` |
| `/slurp` | `Type=SlurpError`, `Error=<message>` |
| `/copy` | `Success=false`, `Error=<message>` |
| `/delete` | `Success=false`, `Error=<message>` |

### Examples

**Extract with callback:**
```bash
curl "http://localhost:8090/extract?key=zips/my_file.zip&prefix=extracted&async=http://example.com/extract-done"
```

**Slurp with callback:**
```bash
curl "http://localhost:8090/slurp?url=https://example.com/file.zip&key=uploads/file.zip&async=http://example.com/slurp-done"
```

### Notes

- The callback timeout is configurable via `AsyncNotificationTimeout` in the config
- If your callback URL returns a non-200 status, the error is logged but the operation result is not retried
- Operations that are already in progress for the same key return `{"Processing": true}` without the `Async` field

## GCS Authentication and Permissions

The key file in your config should be the PEM-encoded private key for a service
account which has permissions to view and create objects on your chosen GCS
bucket.

The bucket needs correct access settings:

- Public access must be enabled, not prevented.
- Access control should be set to fine-grained ("legacy ACL"), not uniform.
