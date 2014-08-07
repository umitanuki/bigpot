#!/bin/sh

DIR=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)
export GOPATH=$DIR:$GOPATH

export PATH=$DIR/bin:$PATH
