#!/bin/sh

file=$1

# Exit if no file is provided
if [ -z "$file" ]; then
    echo "Usage: $0 <path-to-file>"
    exit 1
fi

unzip -p "$1" | sed 's/\\u0000//g' | psql postgresql://postgres:pass@localhost:4443/ -c "COPY tweets_jsonb (data) FROM STDIN csv quote e'\x01' delimiter e'\x02';"
