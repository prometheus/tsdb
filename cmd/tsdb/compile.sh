#! /bin/sh
go get github.com/mitchellh/gox 
go get ./...
gox -arch="amd64" -output="dist/{{.Dir}}_{{.OS}}_{{.Arch}}"
