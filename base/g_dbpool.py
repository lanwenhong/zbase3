#coding: utf-8

import os, sys, time

from nebula3.Config import SessionPoolConfig
from nebula3.gclient.net.SessionPool import SessionPool
from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
from nebula3.data.DataObject import Node
from nebula3.gclient.net import Connection

from nebula3.data.DataObject import Value, ValueWrapper
from nebula3.data.ResultSet import ResultSet
import prettytable
import logging, traceback
from contextlib import contextmanager

log = logging.getLogger()
gdbpool = None


def timeit(func):
    def _(*args, **kwargs):
        st = time.time()
        try:
            resp = func(*args, **kwargs)
            log.debug("resp: %s", resp)
            return resp
        except Exception as e:
            raise
        finally:
            et = time.time()
            #gdbpool = args[0]
            se = args[0]
            nql = args[1]
            log.info('server=%s|user=%s|space=%s|nql=%s|ret=%s|time=%d', se.addrs, se.user, se.space, nql, str(resp), int((et-st)*1000000))
    return _

class GraphClientPool():
     def __init__(self, ip_list=[], min_conn_p_size=0, 
        max_conn_p_size=10, timeout=0, 
        idle_time = 0, interval_check = -1, dbcf={}):

        self.dbcf = dbcf
        self.addrs = ip_list
        self.configs = Config()
        self.configs.min_connection_pool_size = min_conn_p_size
        self.configs.max_connection_pool_size = max_conn_p_size
        self.configs.idle_time = idle_time
        self.configs.interval_check = interval_check
        self.configs.timeout = timeout
        self.pool = ConnectionPool()
        assert self.pool.init(ip_list, self.configs)
    
@timeit
def format_execute(self, nql):
    resp = self.execute(nql)
    assert resp.is_succeeded(), resp.error_msg()
    return resp

@timeit
def format_query(self, nql):
    class Format:
        cast_as = {
            Value.NVAL: "as_null",
            Value.__EMPTY__: "as_empty",
            Value.BVAL: "as_bool",
            Value.IVAL: "as_int",
            Value.FVAL: "as_double",
            Value.SVAL: "as_string",
            Value.LVAL: "as_list",
            Value.UVAL: "as_set",
            Value.MVAL: "as_map",
            Value.TVAL: "as_time",
            Value.DVAL: "as_date",
            Value.DTVAL: "as_datetime",
            Value.VVAL: "as_node",
            Value.EVAL: "as_relationship",
            Value.PVAL: "as_path",
            Value.GGVAL: "as_geography",
            Value.DUVAL: "as_duration",
        }

        def __init__(self):
            pass

        def customized_cast_with_dict(self, val: ValueWrapper):
            _type = val._value.getType()
            method = self.cast_as.get(_type)
            if method is not None:
                return getattr(val, method, lambda *args, **kwargs: None)() 
            raise KeyError("No such key: {}".format(_type))

        def print_resp(self, resp: ResultSet):
            assert resp.is_succeeded()
            output_table = prettytable.PrettyTable()
            output_table.field_names = resp.keys()
            for recode in resp:
                value_list = []
                for col in recode:
                    val = self.customized_cast_with_dict(col)
                    value_list.append(val)
                output_table.add_row(value_list)
            log.debug(output_table)
        
    fm = Format()
    resp = self.execute(nql)
    assert resp.is_succeeded(), resp.error_msg()
    fm.print_resp(resp)
    rows = []
    for recode in resp:
        value_r = {}
        i = 0
        for col in recode:
            val = fm.customized_cast_with_dict(col)
            key = resp.keys()[i]
            value_r[key] = val
            i += 1
        rows.append(value_r)
    log.debug("rows: %s", rows)
    return rows

@contextmanager
def get_session(token):
    try:
        dbcf = gdbpool.dbcf.get(token, None)
        user = dbcf.get('user', 'root')
        pwd = dbcf.get('pwd', '')
        space = dbcf.get('space', '')
        session = gdbpool.pool.get_session(user, pwd)
        session.user = user
        session.space = space
        session.addrs = gdbpool.addrs
        import types
        session.format_execute = types.MethodType(format_execute, session)
        session.format_query = types.MethodType(format_query, session)
        yield session
    except:
        log.error("error=%s", traceback.format_exc())
        raise
    finally:
        if session:
            session.release()

def g_install(ip_list=[], min_conn_p_size=0, 
        max_conn_p_size=10, timeout=0, 
        idle_time = 0, interval_check = -1, dbcf={}):
        
        global gdbpool
        if gdbpool:
            return gdbpool
        gdbpool =  GraphClientPool(ip_list=ip_list, min_conn_p_size=min_conn_p_size,
                        max_conn_p_size=max_conn_p_size, timeout=timeout,
                        idle_time=idle_time, interval_check=interval_check, dbcf=dbcf)
    
        log.debug("dbcf: %s", gdbpool.dbcf)
        return gdbpool

def test1():
    dbcf = {
        'test1': {
            'space': 'basketballplayer2',
            'user': 'root',
            'pwd': 'nebula',
        }
    }

    g_install(ip_list=[('127.0.0.1', 9669 ), ], dbcf=dbcf)
    with get_session('test1') as session:
        nql = 'drop space basketballplayer2'
        session.execute(nql)

def test2():
    dbcf = {
        'test1': {
            'space': 'basketballplayer2',
            'user': 'root',
            'pwd': 'nebula',
        }
    }
    g_install(ip_list=[('127.0.0.1', 9669 ), ], dbcf=dbcf)
    with get_session('test1') as session:
        nql = 'CREATE SPACE basketballplayer2(partition_num=15, replica_factor=1, vid_type=fixed_string(30));'
        session.format_execute(nql)

def test3():
    dbcf = {
        'test1': {
            'space': 'basketballplayer2',
            'user': 'root',
            'pwd': 'nebula',
        }
    }
    g_install(ip_list=[('127.0.0.1', 9669 ), ], dbcf=dbcf)
    with get_session('test1') as session:
        nql = '''
            USE basketballplayer2;
            CREATE TAG player(name string, age int);
            CREATE TAG team(name string);
            CREATE EDGE follow(degree int);
            CREATE EDGE serve(start_year int, end_year int);
        '''
        session.format_execute(nql)

def test4():
    dbcf = {
        'test1': {
            'space': 'basketballplayer2',
            'user': 'lanwenhong',
            'pwd': 'baozou123!@#',
        },
        'test2': {
            'space': 'basketballplayer',
            'user': 'lanwenhong1',
            'pwd': 'baozou123',
        }
    }
    g_install(ip_list=[('127.0.0.1', 9669 ), ], dbcf=dbcf)
    with get_session('test1') as session:
        nql = 'USE basketballplayer2;'
        session.format_execute(nql)
        nql = 'INSERT VERTEX player(name, age) VALUES "{}":("{}", {});'.format('player100', "Tim Duncan", 42)
        session.format_execute(nql)
        nql = 'INSERT VERTEX player(name, age) VALUES "{}":("{}", {});'.format('player101', "Tony Parker", 36)
        session.format_execute(nql)
        nql = 'INSERT VERTEX player(name, age) VALUES "{}":("{}", {});'.format('player102', "LaMarcus Aldridge", 33)
        session.format_execute(nql)

        nql = 'INSERT VERTEX team(name) VALUES "{}":("{}"), "{}":("{}")'.format('team203', 'Trail Blazers', 'team204', 'Spurs')
        session.format_execute(nql)
        nql = 'INSERT EDGE follow(degree) VALUES "{}" -> "{}":({});'.format('player101', 'player100', 95)
        session.format_execute(nql)
        nql = 'INSERT EDGE follow(degree) VALUES "{}" -> "{}":({});'.format('player101', 'player102', 90)
        session.format_execute(nql)
        nql = 'INSERT EDGE follow(degree) VALUES "{}" -> "{}":({});'.format('player102', 'player100', 75)
        session.format_execute(nql)
        nsql = 'INSERT EDGE serve(start_year, end_year) VALUES "{}" -> "{}":({}, {}),"{}" -> "{}":({},  {});'.format('player101', 'team204', 1999, 2018, 'player102', 'team203', 2006, 2015)
        session.format_execute(nql)

def test6():
    dbcf = {
        'test1': {
            'space': 'basketballplayer2',
            'user': 'lanwenhong',
            'pwd': 'baozou123!@#',
        },
        'test2': {
            'space': 'basketballplayer',
            'user': 'lanwenhong1',
            'pwd': 'baozou123',
        }
    }
    g_install(ip_list=[('127.0.0.1', 9669 ), ], dbcf=dbcf)
    with get_session('test1') as session:
        session.format_execute('use basketballplayer2;')
        nql = 'INSERT VERTEX player(name, age) VALUES "{}":("{}", {});'.format('player100', "Tim Duncan", 42)
        session.format_execute(nql)
        nql = 'INSERT VERTEX player(name, age) VALUES "{}":("{}", {});'.format('player101', "Tony Parker", 36)
        session.format_execute(nql)

def test7():
    dbcf = {
        'test1': {
            'space': 'basketballplayer2',
            'user': 'lanwenhong',
            'pwd': 'baozou123!@#',
        },
        'test2': {
            'space': 'basketballplayer',
            'user': 'lanwenhong1',
            'pwd': 'baozou123',
        }
    }
    g_install(ip_list=[('127.0.0.1', 9669 ), ], dbcf=dbcf)
    with get_session('test1') as session:
        session.format_execute('use basketballplayer2;')
        nql = 'GO FROM "player101" OVER follow YIELD id($$);'
        session.format_query(nql)

def test8():
    dbcf = {
        'test1': {
            'space': 'basketballplayer2',
            'user': 'lanwenhong',
            'pwd': 'baozou123!@#',
        },
        'test2': {
            'space': 'basketballplayer',
            'user': 'lanwenhong1',
            'pwd': 'baozou123',
        }
    }
    g_install(ip_list=[('127.0.0.1', 9669 ), ], dbcf=dbcf)
    with get_session('test1') as session:
        session.format_execute('use basketballplayer2;')

        nql = '''GO FROM "player101" OVER follow WHERE properties($$).age >= 35 \
        YIELD properties($$).name AS Teammate, properties($$).age AS Age;'''
        
        session.format_query(nql)
        
def test9():
    dbcf = {
        'test1': {
            'space': 'basketballplayer2',
            'user': 'lanwenhong',
            'pwd': 'baozou123!@#',
        },
        'test2': {
            'space': 'basketballplayer',
            'user': 'lanwenhong1',
            'pwd': 'baozou123',
        }
    }
    g_install(ip_list=[('127.0.0.1', 9669 ), ], dbcf=dbcf)
    with get_session('test1') as session:
        session.format_execute('use basketballplayer2;')
        nql = '''FETCH PROP ON follow "player101" -> "player100" YIELD properties(edge);'''
        session.format_query(nql)

def test10():
    dbcf = {
        'test1': {
            'space': 'basketballplayer2',
            'user': 'lanwenhong',
            'pwd': 'baozou123!@#',
        },
        'test2': {
            'space': 'basketballplayer',
            'user': 'lanwenhong1',
            'pwd': 'baozou123',
        }
    }
    g_install(ip_list=[('127.0.0.1', 9669 ), ], dbcf=dbcf)
    with get_session('test1') as session:
        session.format_execute("use basketballplayer2;")  
        nql = '''
            match (v1:player)-[e:follow]->(v2:player) \
            return v1, e, v2
            limit 10;
        '''
        rows = session.format_query(nql)
        row = rows[0]
        v1 = row['v1']
        log.debug("v1 id: %s tag: %s properties: %s", v1.get_id(), v1.tags(), v1.properties('player'))
        e = row['e']
        log.debug("e name: %s ranking: %s properties: %s", e.edge_name(), e.ranking(), e.properties())
        v2 = row['v2']

def test11():
    dbcf = {
        'test1': {
            'space': 'basketballplayer2',
            'user': 'lanwenhong',
            'pwd': 'baozou123!@#',
        },
        'test2': {
            'space': 'basketballplayer',
            'user': 'lanwenhong1',
            'pwd': 'baozou123',
        }
    }
    g_install(ip_list=[('127.0.0.1', 9669 ), ], dbcf=dbcf)
    with get_session('test1') as session:
        session.format_execute("use basketballplayer2;")  
        nql = '''
            MATCH (v:player { name: 'Tim Duncan' })--(v2) \
            WHERE id(v2) IN ["player101", "player102"] \
            RETURN v2;
        '''
        rows = session.format_query(nql)
        for row in rows:
            v2 = row['v2']
            log.debug("v2 id: %s tag: %s properties: %s", v2.get_id(), v2.tags(), v2.properties('player'))

def test12():
    dbcf = {
        'test1': {
            'space': 'basketballplayer2',
            'user': 'lanwenhong',
            'pwd': 'baozou123!@#',
        },
        'test2': {
            'space': 'basketballplayer',
            'user': 'lanwenhong1',
            'pwd': 'baozou123',
        }
    }
    g_install(ip_list=[('127.0.0.1', 9669 ), ], dbcf=dbcf)
    with get_session('test1') as session:
        session.format_execute("use basketballplayer2;")  
        nql = '''
            MATCH (v:player{name:"LaMarcus Aldridge"})-->()<--(v3) \
            RETURN v3.player.name AS Name;
        '''
        rows = session.format_query(nql)
        for row in rows:
            n = row['Name']
            log.debug('name: %s', n)

if __name__ == '__main__':
    from zbase3.base import logger
    logger.install('stdout')
    #test1()
    #test2()
    #test3()
    #test4()
    #test6()
    #test7()
    #test8()
    #test9()
    #test10()
    #test11()
    test12()
