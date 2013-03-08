#!/usr/bin/env bash
UNIX_PROTO_DIR=src/main/protobuf
JAVA_DIR=src/main/java/
mkdir -p $JAVA_DIR 2> /dev/null
PROTO_DIR=$UNIX_PROTO_DIR
for PROTO_FILE in $UNIX_PROTO_DIR/*.proto
do
protoc -I$PROTO_DIR --java_out=$JAVA_DIR $PROTO_FILE
done