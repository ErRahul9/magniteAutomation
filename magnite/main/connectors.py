import os

import psycopg2
from redis.cluster import RedisCluster
from redis.cluster import ClusterNode

def connectToCache(host, port, mapping, key,action,insertType):
    ROOTDIR = os.path.realpath(os.path.join(os.path.dirname(__file__), "..","resources"))
    startup_nodes = [ClusterNode(host, port)]
    rc = RedisCluster(startup_nodes=startup_nodes, decode_responses=True, skip_full_coverage_check=True)
    getValue = ""
    if "insert" in action:
        if "set" in insertType:
            for data in mapping:
                rc.set(key, mapping.get(data))
                getValue = rc.get(key)
        elif "sadd" in insertType:
            vals = [data for data in mapping.get(key)]
            for value in vals:
                rc.sadd(key,value)
            getValue = rc.smembers(key)

        elif "hm" in insertType:
            rc.hmset(name=key,mapping=mapping)
            getValue = rc.hgetall(key)
    elif "delete" in action:
        rc.flushall()
    return getValue


def connectToPostgres(dburl,user,passd,port,query):
    conn = psycopg2.connect(database="qacoredb", user=user, password=passd, host= dburl, port=port)
    conn.autocommit = True
    cursor = conn.cursor()
    # print("inserting data into postgres for {0}".format(query))
    cursor.execute(query)
    # print("insert complete")
    conn.commit()
    conn.close()
    return cursor




