#!/usr/bin/env bash

find /opt/ProductDetailUpdate/ -type f -mtime +3 -execdir rm -- '{}' \;