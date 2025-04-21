#!/bin/sh

files=$(find data/*)

echo '================================================================================'
echo 'load pg_denormalized'
echo '================================================================================'
for file in $files; do
    unzip -p $file | sed 's/\\u0000//g' | psql postgresql://postgres:pass@localhost:4444 -c "COPY tweets_jsonb (data) FROM STDIN csv quote E'\x01' delimiter E'\x02';"
done


echo '================================================================================'
echo 'load pg_normalized'
echo '================================================================================'
for file in $files; do
    unzip -p $file | sed 's/\\u0000//g' | psql postgresql://postgres:pass@localhost:4445 -c "COPY tweets_jsonb (data) FROM STDIN csv quote E'\x01' delimiter E'\x02';"
done


echo '================================================================================'
echo 'load pg_normalized_batch'
echo '================================================================================'
for file in $files; do
    unzip -p $file | sed 's/\\u0000//g' | psql postgresql://postgres:pass@localhost:4446 -c "COPY tweets_jsonb (data) FROM STDIN csv quote E'\x01' delimiter E'\x02';"
done

