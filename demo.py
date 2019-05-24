"""
Usefull shorcuts:

    export PYTHONPATH=$PWD/lib/python2.7/site-packages
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
import datetime

def ddn():
    return datetime.datetime.now()

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


    rootLogger = logging.getLogger()
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

def do_store_insert_request(store, aDict):

    _dict = aDict
    row_d = {
        'body'        : _dict['body'],
        'clustername' : _dict['clustername'],
        'cmdtype'     : _dict['cmdtype'],
        'data'        : _dict['data'],
        'endtime'     : _dict['endtime'],
        'error'       : _dict['error'],
        'error_str'   : _dict['error_str'],
        'lock'        : _dict['lock'],
        'params'      : _dict['params'],
        'starttime'   : _dict['starttime'],
        'staus'       : _dict['status'],
        'statusinfo'  : _dict['statusinfo'],
        'uuid'        : _dict['uuid'],
        'xml'         : _dict['xml']
    }
    row = Row(row_d)
    try:
        store.put("requests", row)
        logging.debug("Store write succeeded.")
    except IllegalArgumentException, iae:
        logging.error("Could not write table.")
        logging.error(iae.message)

def do_store_create_request(store):

    try:
        ddl = """DROP TABLE IF EXISTS requests"""
        store.execute_sync(ddl)
        logging.debug("Table drop succeeded")
    except IllegalArgumentException, iae:
        logging.error("DDL failed.")
        logging.error(iae.message)
        return

    _ddl = """CREATE TABLE requests (
                uuid STRING,
                status STRING,
                starttime STRING,
                endtime STRING,
                cmdtype STRING,
                params STRING,
                error STRING,
                error_str STRING,
                body STRING,
                xml STRING,
                statusinfo STRING,
                clustername STRING,
                lock STRING,
                data STRING,
                PRIMARY KEY (uuid) )"""  # type: str
    try:
        store.execute_sync(_ddl)
        logging.debug("Table creation succeeded")
    except IllegalArgumentException, iae:
        logging.error("DDL failed.")
        logging.error(iae.message)

def do_store_insert_big(store, aString):

    row_d = {  'uuid' : str(uuid.uuid1()),
               'desc' : aString,
               'desc2' : aString,
               'count' : 5,
               'percentage' : 0.2173913}
    row = Row(row_d)
    try:
        store.put("Users2", row)
        logging.debug("Store write succeeded.")
    except IllegalArgumentException, iae:
        logging.error("Could not write table.")
        logging.error(iae.message)

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
        # Users2
        ddl = """CREATE TABLE Users2 ( uuid STRING, desc STRING, count INTEGER, percentage FLOAT, PRIMARY KEY (uuid) )"""
        ### ddl = """CREATE TABLE Users2 ( zip INTEGER CHECK(zip<300), PRIMARY KEY (zip))"""
        store.execute_sync(ddl)
        logging.debug("Table creation succeeded")
    except IllegalArgumentException, iae:
        logging.error("DDL failed.")
        logging.error(iae.message)

def do_store_update(self):
    pass

def do_store_delete(store):

    _uuid  = str(uuid.uuid1())
    _table = "Users2"
    _pkey  =  {"uuid" : _uuid }

    try:
        rc = store.delete(_table, _pkey)
        if rc[0]:
            logging.debug("Row deletion succeeded.")
        else:
            logging.debug("Row deletion failed.")
    except IllegalArgumentException, iae:
        logging.error("Row deletion failed (except).")
        logging.error(iae.message)

def do_store_create_index(store):

    store.execute_sync("CREATE INDEX IF NOT EXISTS UUIDX ON Users2 (uuid)")

def iter_fn(err, iterator):
    print err, iterator

if __name__ == '__main__':

    recreate_default_table = False
    perform_insert_default_table = False
    dump_content_default_table = False
    perform_insert_big_data_default_table = False

    setup_logging()
    logging.debug('*** NOSQL Python test driver (c) 2019')
    store = open_store()
    print '*** We are in business ...'

    res = store.execute_sync("show tables")
    print 'execute : ', res['is_done'], res['error_message']

    if recreate_default_table:
        do_store_ops(store)

    if perform_insert_default_table:
        for i in range(0,100000):
            do_store_insert(store)

    #rootLogger = logging.getLogger("nosqldb")
    #rootLogger.setLevel(logging.INFO)

    if perform_insert_big_data_default_table:
        _str = 'Simple string which gotta grow big !!! ...'
        for i in range(0,10):
            do_store_insert_big(store, _str)
            _str = _str + _str
            print 'LEN:', len(_str)

    if False:
        res = store.execute_sync("SELECT uuid, desc FROM Users2")
        print type(res), res

    if False:
        do_store_delete(store)

    if False:
        do_store_create_index(store)
        row_list = store.index_iterator("Users2", "UUIDX", {}, False)
        if row_list:
            for row in row_list:
                print row['uuid']

    if False:
        if dump_content_default_table:
            rows = store.table_iterator("Users2", {}, False)
            for elt in rows:
                print elt['uuid']

    if False:
        do_store_create_request(store)
        store.execute_sync("CREATE INDEX IF NOT EXISTS UUIDX2 ON requests (uuid)")

    #
    # Load requests table
    #
    if False:
        f=open('requests.db.txt')
        d=f.read()
        f.close()
        _x = eval(d)
        for _entry in _x:
            print '*** ENTRY:', _entry['uuid']
            do_store_insert_request(store,_entry)
    #
    # Dump requests table
    #
    if True:
        rows = store.table_iterator("requests", {}, False)
        print '*** ROWS:', rows
        c1=ddn()
        for elt in rows:
            continue
            print 'ITER:', elt['uuid']
        c2=ddn()
        et=c2-c1
        print et.seconds, et.microseconds

    if True:
        row_list = store.index_iterator("requests", "UUIDX2", {}, False)
        c1=ddn()
        if row_list:
            for row in row_list:
                continue
                print 'INDEX:', row['uuid']
        c2 = ddn()
        et = c2 - c1
        print et.seconds, et.microseconds

    store.close()
    print '*** Store is now close ...'
