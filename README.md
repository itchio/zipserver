# zip_server

Zip server is an HTTP service that takes a key to a zip file on Google Cloud
storage, extracts it, then reuploads the individual files to a specified
prefix. It can restrict extraction of the zip file based on individual file
size, total file size, or number of files.


Install:

```bash
$ go get github.com/leafo/zip_server
```

