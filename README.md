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

## Usage

Zipserver can run as an HTTP server or execute operations directly via CLI commands. All CLI commands output JSON.

```bash
zipserver --help              # Show all commands
zipserver <command> --help    # Show help for a specific command
```

### Commands

| Command | Description |
|---------|-------------|
| `server` | Start HTTP server (default) |
| `extract` | Extract a zip file to storage |
| `copy` | Copy a file to target storage |
| `delete` | Delete files from storage |
| `list` | List files in a zip archive |
| `slurp` | Download a URL and store it |
| `serve` | Serve a local zip file via HTTP |
| `dump` | Dump parsed config and exit |
| `version` | Print version information |

## HTTP Server

Start the server:

```bash
zipserver server --listen 0.0.0.0:8090
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
```

**HTTP API:**
```bash
curl "http://localhost:8090/extract?key=zips/my_file.zip&prefix=extracted"
```

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

List files in a zip archive without extracting.

**CLI:**
```bash
# From storage
zipserver list --key zips/my_file.zip

# From URL
zipserver list --url https://example.com/file.zip
```

**HTTP API:**
```bash
curl "http://localhost:8090/list?key=zips/my_file.zip"
```

## Slurp

Download a file from a URL and store it in storage.

**CLI:**
```bash
zipserver slurp --url https://example.com/file.zip --key uploads/file.zip
```

**HTTP API:**
```bash
curl "http://localhost:8090/slurp?url=https://example.com/file.zip&key=uploads/file.zip"
```

## Serve Local Zip

Serve a local zip file via HTTP for testing:

```bash
zipserver serve ./my_file.zip
```

## Storage Targets

You can configure additional storage targets (S3, GCS) for copy and delete operations:

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
    }
  ]
}
```

## GCS Authentication and Permissions

The key file in your config should be the PEM-encoded private key for a service account which has permissions to view and create objects on your chosen GCS bucket.

The bucket needs correct access settings:

- Public access must be enabled, not prevented.
- Access control should be set to fine-grained ("legacy ACL"), not uniform.
