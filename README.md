# zip_server

Zip server is an HTTP service that takes a key to a zip file on Google Cloud
storage, extracts it, then reuploads the individual files to a specified
prefix. It can restrict extraction of the zip file based on individual file
size, total file size, or number of files.


## Usage

Install

```bash
go get github.com/leafo/zip_server
go install github.com/leafo/zip_server/zip_server
```

Create a config file:

`zip_server.json`:

```json
{
	"PrivateKeyPath": "path/to/service/key.pem",
	"ClientEmail": "111111111111@developer.gserviceaccount.com"
}
```

More config settings can be found in `config.go`

Run:

```bash
$GOPATH/bin/zip_server
```

Extract a zip file:

```bash
curl http://localhost:8090/extract?key=zips/my_file.zip&prefix=extracted
```

