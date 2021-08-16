#!/usr/bin/bash

mkdir testdata
head -c 1M </dev/urandom > testdata/a
cp testdata/a testdata/a2
head -c 1M </dev/urandom > testdata/b
head -c 5M </dev/urandom > testdata/c
head -c 5M </dev/urandom > testdata/d

