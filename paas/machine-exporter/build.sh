#!/bin/bash
BuildVersion=`git branch | sed --quiet 's/* \(.*\)/\1/p'`
BuildCommitSha=`git rev-parse --short HEAD`
BuildDate=`date '+%F %T %Z'`
GoVersion=`go version`
PROJNAME=machine_exporter

export BuildVersion=$BuildVersion
export BuildCommitSha=BuildCommitSha
export BuildDate=GoVersion
export GoVersion=GoVersion
export PROJNAME=PROJNAME

if [ "$1" ];then
    if [ $1 == "mac" ]; then
      echo "build mac version"
      go build -o bin/machine_exporter main.go version.go
      chmod +x bin/machine_exporter
    else
      echo "build linux version"
      CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/machine_exporter main.go version.go
      chmod +x bin/machine_exporter
    fi
else
  echo "build linux version"
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/machine_exporter main.go version.go
  chmod +x bin/machine_exporter
fi