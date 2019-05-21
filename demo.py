"""
Usefull shorcuts:

    java -Doracle.kv.security=/home/ankh/nosql/kv-18.1.19/kvroot/security/user.security -jar lib/kvstore.jar runadmin -host localhost -port 5000

"""

from nosqldb import ConnectionException
from nosqldb import IllegalArgumentException
from nosqldb import Factory
from nosqldb import StoreConfig
from nosqldb import ProxyConfig
from nosqldb import Row

import logging
import sys
import uuid

storehost = "localhost:5000"
proxy = "localhost:7010"

# set logging level to debug and log to stdout
def setup_logging():
    rootLogger = logging.getLogger("nosqldb")
    rootLogger.setLevel(logging.DEBUG)

    logger = logging.StreamHandler(sys.stdout)
    logger.setLevel(logging.DEBUG)

    errlogger = logging.StreamHandler(sys.stderr)
    errlogger.setLevel(logging.ERROR)

    formatter = logging.Formatter('\t%(levelname)s - %(message)s')
    logger.setFormatter(formatter)
    errlogger.setFormatter(formatter)

    rootLogger.addHandler(logger)
    rootLogger.addHandler(errlogger)

# configure and open the store
def open_store():
    try:
        kvproxyconfig = ProxyConfig()
        kvproxyconfig.set_security_props_file('kv-18.1.19/kvroot/security/user.security')
        kvstoreconfig = StoreConfig('kvstore', [storehost])
        kvstoreconfig.set_user("admin")
        return Factory.open(proxy, kvstoreconfig,kvproxyconfig)
    except ConnectionException, ce:
        logging.error("Store connection failed.")
        logging.error(ce.message)
        sys.exit(-1)

def do_store_insert(store):

    row_d = {  'uuid' : str(uuid.uuid1()),
               'desc' : "Hex head, stainless",
               'count' : 5,
               'percentage' : 0.2173913}
    row = Row(row_d)
    try:
        store.put("Users2", row)
        logging.debug("Store write succeeded.")
    except IllegalArgumentException, iae:
        logging.error("Could not write table.")
        logging.error(iae.message)

def do_store_ops(store):
    ### Drop the table if it exists
    try:
        ddl = """DROP TABLE IF EXISTS Users2"""
        store.execute_sync(ddl)
        logging.debug("Table drop succeeded")
    except IllegalArgumentException, iae:
        logging.error("DDL failed.")
        logging.error(iae.message)
        return

    ### Create a table
    try:
        ddl = """CREATE TABLE Users2 (
            id INTEGER CHECK(id <= 00000 and id < 99999),
            firstName STRING,
            lastName STRING,
            runner ENUM(slow,fast) DEFAULT slow,
            myRec RECORD(a STRING),
            myMap MAP(INTEGER CHECK(ELEMENTOF(myMap) > 500)),
            myArray ARRAY(INTEGER),
            myBool BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (SHARD(id, firstName), lastName)
        )"""
        ddl = """CREATE TABLE Users2 ( uuid STRING, desc STRING, count INTEGER, percentage FLOAT, PRIMARY KEY (uuid) )"""
        ### ddl = """CREATE TABLE Users2 ( zip INTEGER CHECK(zip<300), PRIMARY KEY (zip))"""
        store.execute_sync(ddl)
        logging.debug("Table creation succeeded")
    except IllegalArgumentException, iae:
        logging.error("DDL failed.")
        logging.error(iae.message)

def iter_fn(err, iterator):
    print err, iterator

if __name__ == '__main__':
    print '*** NOSQL Python test driver (c) 2019'
    setup_logging()
    store = open_store()
    print '*** We are in business ...'

    res = store.execute_sync("show tables")
    print 'execute : ', res['is_done'], res['error_message']

    #do_store_ops(store)

    for i in range(0,100000):
        do_store_insert(store)

    print store.execute_sync("SELECT uuid, desc FROM Users2")

    rows = store.table_iterator("Users2", {}, False)
    for elt in rows:
        print elt['uuid']

    store.close()
    print '*** Store is now close ...'
