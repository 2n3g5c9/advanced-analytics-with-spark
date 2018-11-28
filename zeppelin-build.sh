#!/usr/bin/env bash
docker build --rm -t zeppelin-fixed:0.8.0 .
docker image prune -f