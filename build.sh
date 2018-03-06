#!/bin/sh

set -ex
# A convienent script for building for both local target and linux

build() {

  go build -ldflags '-s' 

  env GOOS=linux go build -o xtargets/linux/rabbitperf
}

time build
