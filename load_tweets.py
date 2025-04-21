#!/usr/bin/python3

import sqlalchemy
import os
import datetime
import zipfile
import io
import json

################################################################################
# helper functions
################################################################################

def remove_nulls(s):
    r'''
    Postgres doesn't support strings with the null character \x00 in them, but twitter does.
    This helper function replaces the null characters with an escaped version so that they can be loaded into postgres.

    >>> remove_nulls('\x00')
    ''
    >>> remove_nulls('hello\x00 world')
    'hello world'
    '''
    if s is None:
        return None
    else:
        return s.replace('\x00','')


def get_id_urls(url, connection):
    '''
    Given a url, return the corresponding id in the urls table.
    If no row exists for the url, then one is inserted automatically.
    '''
    sql = sqlalchemy.sql.text('''
    INSERT INTO urls
        (url)
        VALUES
        (:url)
    ON CONFLICT DO NOTHING
    RETURNING id_urls
    ''')
    res = connection.execute(sql, {'url': url}).first()

    # If no row was returned, we had a conflict, so we must SELECT the existing row
    if res is None:
        sql = sqlalchemy.sql.text('''
        SELECT id_urls
        FROM urls
        WHERE url = :url
        ''')
        res = connection.execute(sql, {'url': url}).first()

    return res[0]


def insert_tweet(connection, tweet):
    '''
    Insert the tweet into the database (normalized schema).
    '''

    # Wrap all inserts in a single transaction so that
    # a tweet is either fully inserted or not at all.
    with connection.begin() as trans:

        #################################################
        # 1. Skip if this tweet is already inserted
        #################################################
        sql = sqlalchemy.sql.text('''
        SELECT id_tweets
        FROM tweets
        WHERE id_tweets = :id_tweets
        ''')
        res = connection.execute(sql, {'id_tweets': str(tweet['id'])}).first()
        if res is not None:
            # tweet is already in the DB; nothing else to do
            return

        #################################################
        # 2. Insert user
        #################################################
        if tweet['user']['url'] is None:
            user_id_urls = None
        else:
            user_id_urls = get_id_urls(tweet['user']['url'], connection)

        sql = sqlalchemy.sql.text('''
        INSERT INTO users (
            id_users,
            created_at,
            updated_at,
            screen_name,
            name,
            location,
            id_urls,
            description,
            protected,
            verified,
            friends_count,
            listed_count,
            favourites_count,
            statuses_count,
            withheld_in_countries
        )
        VALUES (
            :id_users,
            :created_at,
            :updated_at,
            :screen_name,
            :name,
            :location,
            :id_urls,
            :description,
            :protected,
            :verified,
            :friends_count,
            :listed_count,
            :favourites_count,
            :statuses_count,
            :withheld_in_countries
        )
        ON CONFLICT (id_users) DO NOTHING
        ''')
        connection.execute(sql, {
            'id_users':           str(tweet['user']['id']),
            'created_at':         tweet.get('created_at'),
            'updated_at':         tweet.get('updated_at'),
            'screen_name':        tweet.get('screen_name'),
            'name':               tweet.get('name'),
            'location':           tweet.get('location'),
            'id_urls':            user_id_urls,
            'description':        tweet.get('description'),
            'protected':          tweet.get('protected'),
            'verified':           tweet.get('verified'),
            'friends_count':      tweet.get('friends_count'),
            'listed_count':       tweet.get('listed_count'),
            'favourites_count':   tweet.get('favourites_count'),
            'statuses_count':     tweet.get('statuses_count'),
            'withheld_in_countries': tweet.get('withheld_in_countries'),
        })

        #################################################
        # 3. Prepare tweet‐level fields
        #################################################
        try:
            geo_coords = tweet['geo']['coordinates']
            geo_str = 'POINT'
        except TypeError:
            try:
                geo_coords = '('
                for i, poly in enumerate(tweet['place']['bounding_box']['coordinates']):
                    if i > 0:
                        geo_coords += ','
                    geo_coords += '('
                    for j, point in enumerate(poly):
                        geo_coords += f"{point[0]} {point[1]},"
                    # close the ring
                    geo_coords += f"{poly[0][0]} {poly[0][1]})"
                geo_coords += ')'
                geo_str = 'MULTIPOLYGON'
            except KeyError:
                # user might have geo_enabled, but no place data
                if tweet['user']['geo_enabled']:
                    geo_str = None
                    geo_coords = None

        try:
            text = tweet['extended_tweet']['full_text']
        except KeyError:
            text = tweet['text']

        try:
            country_code = tweet['place']['country_code'].lower()
        except (TypeError, KeyError):
            country_code = None

        if country_code == 'us':
            state_code = tweet['place']['full_name'].split(',')[-1].strip().lower()
            if len(state_code) > 2:
                state_code = None
        else:
            state_code = None

        try:
            place_name = tweet['place']['full_name']
        except (TypeError, KeyError):
            place_name = None

        # Ensure the in_reply_to user is in the DB (unhydrated)
        if tweet.get('in_reply_to_user_id') is not None:
            sql = sqlalchemy.sql.text('''
            INSERT INTO users (id_users)
            VALUES (:id_users)
            ON CONFLICT DO NOTHING
            ''')
            connection.execute(sql, {'id_users': str(tweet['in_reply_to_user_id'])})

        #################################################
        # 4. Insert into tweets table
        #################################################
        sql = sqlalchemy.sql.text('''
        INSERT INTO tweets (
            id_tweets,
            id_users,
            created_at,
            in_reply_to_status_id,
            in_reply_to_user_id,
            quoted_status_id,
            retweet_count,
            favorite_count,
            quote_count,
            withheld_copyright,
            withheld_in_countries,
            source,
            text,
            country_code,
            state_code,
            lang,
            place_name,
            geo
        )
        VALUES (
            :id_tweets,
            :id_users,
            :created_at,
            :in_reply_to_status_id,
            :in_reply_to_user_id,
            :quoted_status_id,
            :retweet_count,
            :favorite_count,
            :quote_count,
            :withheld_copyright,
            :withheld_in_countries,
            :source,
            :text,
            :country_code,
            :state_code,
            :lang,
            :place_name,
            :geo
        )
        ON CONFLICT DO NOTHING
        ''')
        connection.execute(sql, {
            'id_tweets':            str(tweet['id']),
            'id_users':             str(tweet['user']['id']),
            'created_at':           tweet.get('created_at'),
            'in_reply_to_status_id': tweet.get('in_reply_to_status_id'),
            'in_reply_to_user_id':   str(tweet.get('in_reply_to_user_id')) if tweet.get('in_reply_to_user_id') else None,
            'quoted_status_id':      tweet.get('quoted_status_id'),
            'retweet_count':         tweet.get('retweet_count'),
            'favorite_count':        tweet.get('favorite_count'),
            'quote_count':           tweet.get('quote_count'),
            'withheld_copyright':    tweet.get('withheld_copyright'),
            'withheld_in_countries': tweet.get('withheld_in_countries'),
            'source':                remove_nulls(tweet.get('source')),
            'text':                  remove_nulls(text),
            'country_code':          remove_nulls(country_code),
            'state_code':            remove_nulls(state_code),
            'lang':                  remove_nulls(tweet.get('lang')),
            'place_name':            remove_nulls(place_name),
            'geo':                   None
        })

        #################################################
        # 5. tweet_urls
        #################################################
        try:
            urls = tweet['extended_tweet']['entities']['urls']
        except KeyError:
            urls = tweet['entities']['urls']

        for u in urls:
            id_urls = get_id_urls(u['expanded_url'], connection)
            sql = sqlalchemy.sql.text('''
            INSERT INTO tweet_urls (id_tweets, id_urls)
            VALUES (:id_tweets, :id_urls)
            ON CONFLICT DO NOTHING
            ''')
            connection.execute(sql, {
                'id_tweets': tweet['id'],
                'id_urls':   id_urls
            })

        #################################################
        # 6. tweet_mentions
        #################################################
        try:
            mentions = tweet['extended_tweet']['entities']['user_mentions']
        except KeyError:
            mentions = tweet['entities']['user_mentions']

        for mention in mentions:
            # unhydrated user insert
            sql = sqlalchemy.sql.text('''
            INSERT INTO users (id_users)
            VALUES (:id_users)
            ON CONFLICT DO NOTHING
            ''')
            connection.execute(sql, {'id_users': mention['id']})

            # insert mention link
            sql = sqlalchemy.sql.text('''
            INSERT INTO tweet_mentions (id_tweets, id_users)
            VALUES (:id_tweets, :id_users)
            ON CONFLICT DO NOTHING
            ''')
            connection.execute(sql, {
                'id_tweets': tweet['id'],
                'id_users':  mention['id']
            })

        #################################################
        # 7. tweet_tags
        #################################################
        try:
            hashtags = tweet['extended_tweet']['entities']['hashtags']
            cashtags = tweet['extended_tweet']['entities']['symbols']
        except KeyError:
            hashtags = tweet['entities']['hashtags']
            cashtags = tweet['entities']['symbols']

        tags = ['#' + h['text'] for h in hashtags] + ['$' + c['text'] for c in cashtags]
        for tag in tags:
            sql = sqlalchemy.sql.text('''
            INSERT INTO tweet_tags (id_tweets, tag)
            VALUES (:id_tweets, :tag)
            ON CONFLICT DO NOTHING
            ''')
            connection.execute(sql, {
                'id_tweets': tweet['id'],
                'tag':       remove_nulls(tag)
            })

        #################################################
        # 8. tweet_media
        #################################################
        try:
            media = tweet['extended_tweet']['extended_entities']['media']
        except KeyError:
            try:
                media = tweet['extended_entities']['media']
            except KeyError:
                media = []

        for m in media:
            id_urls = get_id_urls(m['media_url'], connection)
            sql = sqlalchemy.sql.text('''
            INSERT INTO tweet_media (id_tweets, id_urls, type)
            VALUES (:id_tweets, :id_urls, :type)
            ON CONFLICT DO NOTHING
            ''')
            connection.execute(sql, {
                'id_tweets': tweet['id'],
                'id_urls':   id_urls,
                'type':      remove_nulls(m['type'])
            })


################################################################################
# main
################################################################################

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    parser.add_argument('--inputs', nargs='+', required=True)
    parser.add_argument('--print_every', type=int, default=1000)
    args = parser.parse_args()

    # create database connection
    engine = sqlalchemy.create_engine(args.db, connect_args={
        'application_name': 'load_tweets.py',
    })
    connection = engine.connect()

    # loop through the input files
    for filename in sorted(args.inputs, reverse=True):
        with zipfile.ZipFile(filename, 'r') as archive:
            print(datetime.datetime.now(), filename)
            for subfilename in sorted(archive.namelist(), reverse=True):
                with io.TextIOWrapper(archive.open(subfilename)) as f:
                    for i, line in enumerate(f):
                        tweet = json.loads(line)
                        insert_tweet(connection, tweet)

                        if i % args.print_every == 0:
                            print(datetime.datetime.now(),
                                  filename, subfilename,
                                  'i=', i, 'id=', tweet['id'])

