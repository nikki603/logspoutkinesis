#!/bin/sh
cat > ./Dockerfile <<DOCKERFILE
FROM gliderlabs/logspout:v3.1
DOCKERFILE

cat > ./modules.go <<MODULES
package main
import (
  _ "github.com/gliderlabs/logspout/httpstream"
  _ "github.com/gliderlabs/logspout/routesapi"
  _ "github.com/moovel/logspout-kinesis"
)
MODULES


docker build -t moovel/logspout-kinesis .