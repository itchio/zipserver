[![Build Status](https://travis-ci.org/leafo/zipserver.svg?branch=master)](https://travis-ci.org/leafo/zipserver)

# zipserver

Zip server is an HTTP service that takes a key to a zip file on Google Cloud
storage, extracts it, then reuploads the individual files to a specified
prefix. It can restrict extraction of the zip file based on individual file
size, total file size, or number of files.


## Usage

Install

```bash
go get github.com/leafo/zipserver

zipserver -help
```

Create a config file:

`zipserver.json`:

```json
{
	"PrivateKeyPath": "path/to/service/key.pem",
	"ClientEmail": "111111111111@developer.gserviceaccount.com"
}
```

More config settings can be found in `config.go`

Run:

```bash
$GOPATH/bin/zipserver
```

Extract a zip file:

```bash
curl http://localhost:8090/extract?key=zips/my_file.zip&prefix=extracted
```


## Slurping

You can tell the zip server to download a file from a URL. This can be used to
load a zip file you want to extract later.

```bash
curl http://localhost:8090/slurp?key=myfile.zip&url=http://leafo.net/file.zip
```

