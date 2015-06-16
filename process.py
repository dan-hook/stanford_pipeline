# -*- encoding=utf-8 -*-

import os
import sys
import glob
import parser
import logging
import datetime
import argparse
from pymongo import MongoClient
from ConfigParser import ConfigParser
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, F

def make_conn(db_auth, db_user, db_pass, db_host=None, elasticsearch=False):
    """
    Function to establish a connection to a local MonoDB instance.

    Parameters
    ----------

    db_auth: String.
                MongoDB database that should be used for user authentication.

    db_user: String.
                Username for MongoDB authentication.

    db_user: String.
                Password for MongoDB authentication.


    Returns
    -------

    collection: pymongo.collection.Collection.
                Collection within MongoDB that holds the scraped news stories.

    """
    if not elasticsearch:
        if db_host:
            client = MongoClient(db_host)
        else:
            client = MongoClient()
        if db_auth:
            client[db_auth].authenticate(db_user, db_pass)
        database = client.event_scrape
        collection = database['stories']
    else:
        collection=Elasticsearch()

    return collection


def query_date(collection, date, num_days, elasticsearch, index):
    """
    Function to query the MongoDB instance and obtain results for the desired
    date range. Pulls stories that aren't Stanford parsed yet
    (``"stanford: 0"``) and that were added within the last day.

    Parameters
    ----------

    collection: pymongo.collection.Collection.
                Collection within MongoDB that holds the scraped news stories.

    date: String.
            Current date that the program is running.

    Returns
    -------

    posts: pymongo.cursor.Cursor.
            Results from the MongoDB query.

    """

    logger = logging.getLogger('stanford')
    gt_date = date - datetime.timedelta(days=num_days)
    if not elasticsearch:
        posts = collection.find({"$and": [{"date_added": {"$lte": date}},
                                          {"date_added": {"$gt": gt_date}},
                                          {"stanford": 0}]})
        logger.info('Returning {} total stories.'.format(posts.count()))
    else:
        #Do a date range query and filter out the documents where stanford != 0.
        lte_time = date.strftime('%Y-%m-%dT%X.%f%z')
        gt_time = gt_date.strftime('%Y-%m-%dT%X.%f%z')
        s = Search(using=collection,index=index,doc_type="news")\
            .filter("range",published_date={"lte": lte_time, "gt": gt_time})\
            .filter("or", [F("term", stanford=0), F("missing", field="stanford")])

        page = s[0:100].execute()
        total = page.hits.total
        current_count = 100
        posts=page.hits
        while current_count < total:
            page = s[current_count+1:current_count+101].execute()

            posts.extend(page.hits)
            current_count += 100

    return posts


def _parse_config(cparser):
    try:
        stanford_dir = cparser.get('StanfordNLP', 'stanford_dir')
        if 'Logging' in cparser.sections():
            log_dir = cparser.get('Logging', 'log_file')
        else:
            log_dir = ''
        if 'Auth' in cparser.sections():
            auth_db = cparser.get('Auth', 'auth_db')
            auth_user = cparser.get('Auth', 'auth_user')
            auth_pass = cparser.get('Auth', 'auth_pass')
            db_host = cparser.get('Auth', 'db_host')
        else:
            auth_db = ''
            auth_user = ''
            auth_pass = ''
            db_host = os.getenv('MONGO_HOST')
        return stanford_dir, log_dir, auth_db, auth_user, auth_pass, db_host
    except Exception, e:
        print 'There was an error parsing the config file. {}'.format(e)
        raise


def parse_config():
    """Function to parse the config file."""
    config_file = glob.glob('config.ini')
    cparser = ConfigParser()
    if config_file:
        cparser.read(config_file)
    else:
        cwd = os.path.abspath(os.path.dirname(__file__))
        config_file = os.path.join(cwd, 'default_config.ini')
        cparser.read(config_file)
    return _parse_config(cparser)


def run(run_date,num_days,elasticsearch,index):
    stanford_dir, log_dir, db_auth, db_user, db_pass, db_host = parse_config()
    # Setup the logging
    logger = logging.getLogger('stanford')
    logger.setLevel(logging.INFO)

    if log_dir:
        fh = logging.FileHandler(log_dir, 'a')
    else:
        fh = logging.FileHandler('stanford.log', 'a')
    formatter = logging.Formatter('%(levelname)s %(asctime)s: %(message)s')
    fh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.info('Running.')

    if not run_date:
      run_date = datetime.datetime.utcnow()
    else:
        try:
            run_date = datetime.datetime.strptime(run_date,'%Y%m%d')
        except ValueError:
            print('Bad run date')
            raise SystemExit

    coll = make_conn(db_auth, db_user, db_pass, db_host, elasticsearch)
    stories = query_date(coll, run_date,num_days,elasticsearch,index)
    parser.stanford_parse(coll, stories, stanford_dir,elasticsearch,index)


if __name__ == '__main__':
    # Grab command line options.
    argumentParser = argparse.ArgumentParser(description='Grab run_date.')
    argumentParser.add_argument('--run_date', type=str, default='',
                        help='enter date in YYYYMMDD format')
    argumentParser.add_argument('--num_days', type=int, default=1,
                                help='number of days before run_date to query')
    argumentParser.add_argument('--es', dest='elasticsearch', action='store_true',
                                help='Use Elasticsearch on localhost')
    argumentParser.set_defaults(elasticsearch=False)
    argumentParser.add_argument('--index', type=str, default='stories-index',
                                help='the elasticsearch index containing the stories')
    args = argumentParser.parse_args()

    run(args.run_date,args.num_days,args.elasticsearch,args.index)

