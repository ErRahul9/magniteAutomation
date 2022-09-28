import os

import psycopg2
from rediscluster import RedisCluster

def connectToCache(host, port, mapping, key,action,insertType):
    ROOTDIR = os.path.realpath(os.path.join(os.path.dirname(__file__), "..","resources"))
    startup_nodes = [{"host": host, "port": port}]
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


'''
integration-dev.crvrygavls2u.us-west-2.rds.amazonaws.com
user: qacore 
5432
qa#core07#19


'''
def connectToPostgres(dburl,user,passd,port,query):
    conn = psycopg2.connect(database="qacoredb", user=user, password=passd, host= dburl, port=port)
    conn.autocommit = True
    cursor = conn.cursor()
    print("inserting data into postgres")
    cursor.execute(query)
    print("insert complete")
    conn.commit()
    conn.close()
    return cursor




# if __name__ == '__main__':
    # conn = connectToPostgres("integration-dev.crvrygavls2u.us-west-2.rds.amazonaws.com","qacore","qa#core07#19",5432)
    # conn.execute


