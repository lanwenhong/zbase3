# coding: utf-8
import os, sys
from elasticsearch import Elasticsearch
import pprint
import logging, traceback
import time

log = logging.getLogger()

edbpool = None

'''
包装后的ret
{
    'succ': True or False
    'fail_reason: xxx
    'data': {}
}
'''


def pack_ret(func):
    def _(*args, **kwargs):
        starttm = time.time()
        ret = {
            'succ': True,
            'fail_reason': '',
            'data': {}
        }

        try:
            result = func(*args, **kwargs)
            ret['data'] = result
        except Exception as e:
            # log.warn(str(e))
            traceback.print_exc()
            ret['succ'] = False
            ret['fail_reason'] = str(e)
        finally:
            endtm = time.time()
            conn = args[0]
            log.info('server=%s|func=%s|index=%s|dsl=%s|time=%d|ret=%s', conn.host, func.__name__, conn.index, conn.dsl,
                     int((endtm - starttm) * 1000000), ret)
            return ret

    return _


def query_pack_ret(func):
    def _(*args, **kwargs):
        starttm = time.time()
        ret = {
            'succ': True,
            'fail_reason': '',
            'data': {}
        }
        try:
            # TODO 处理查询结果
            result = func(*args, **kwargs)
            ret['data'] = result
        except Exception as e:
            # log.warn(str(e))
            traceback.print_exc()
            ret['succ'] = False
            ret['fail_reason'] = str(e)
        finally:
            endtm = time.time()
            conn = args[0]
            log.info('server=%s|func=%s|index=%s|dsl=%s|time=%d|ret=%s', conn.host, func.__name__, conn.index, conn.dsl,
                     int((endtm - starttm) * 1000000), ret)
            return ret

    return _


class ESquery(object):
    def __init__(self, host=None, timeout=1000):
        if host is None:
            host = []
        self.host = host
        self.timeout = timeout
        self.es = Elasticsearch(host, timeout=timeout)
        self.dsl = None
        self.index = None

    @pack_ret
    def es_create_index(self, index_name, ignore=None):
        if ignore is None:
            ignore = []
        self.index = index_name
        result = self.es.indices.create(index=index_name, ignore=ignore)
        return result

    @pack_ret
    def es_del_index(self, index_name):
        self.index = index_name
        result = self.es.indices.delete(index_name)
        return result

    @pack_ret
    def es_modify_mapping(self, index_name, mapping_dsl):
        self.index = index_name
        self.dsl = mapping_dsl
        result = self.es.indices.put_mapping(index=index_name, body=mapping_dsl)
        return result

    @pack_ret
    def es_get_mapping(self, index_name):
        self.index = index_name
        result = self.es.indices.get_mapping(index=index_name)
        return result

    @pack_ret
    def es_create_data(self, index_name, data, id=None):
        self.index = index_name
        self.dsl = data
        if id:
            result = self.es.index(index=index_name, id=id, body=data)
        else:
            result = self.es.index(index=index_name, body=data)
        return result

    '''
        基于ID修改某条数据
        dsl = { 
                'script': "ctx._source.file_name = '000000'; ctx._source.url = 'xxx'" 
            }   
    '''

    @pack_ret
    def es_update_data(self, index_name, id, dsl):
        self.index = index_name
        self.dsl = dsl
        result = self.es.update(index=index_name, id=id, body=dsl)
        return result

    '''
    基于查询修改某些数据
    query = {
        'bool': {
            'must': [
                {
                    'term': {
                        'total': 100,
                    },
                }
            ] 
        }
    }
    script = {
        "inline": "ctx._source.status = params.status",
        "params": {
            "status": 2 
        },
        "lang": "painless",
    }
    '''

    @pack_ret
    def es_update_by_query(self, index_name, q_dsl, script_dsl):
        dsl = {
            "query": q_dsl,
            "script": script_dsl,
        }

        self.index = index_name
        self.dsl = dsl
        log.debug("dsl: %s", dsl)
        result = self.es.update_by_query(index=index_name, body=dsl)
        return result

    def __query_build(self, dsl, qfrom, qsize, includes, excludes, sort, highlight):
        dsl["_source"] = {
            "includes": includes,
            "excludes": excludes,
        }
        if qfrom is not None and qsize is not None:
            dsl['size'] = qsize
            dsl['from'] = qfrom

        if sort:
            dsl['sort'] = sort

        if highlight:
            dsl['highlight'] = highlight

    def es_query(self, index_name, dsl, qfrom, qsize, includes, excludes, sort, highlight=None):
        self.__query_build(dsl, qfrom, qsize, includes, excludes, sort, highlight)
        self.index = index_name
        self.dsl = dsl
        result = self.es.search(index=index_name, body=dsl)
        return result

    '''
    term查询: {"item_name": xxxx"}
    '''

    @query_pack_ret
    def es_query_term(self, index_name, dsl_term, qfrom=None, qsize=None, includes=[], excludes=[], sort=None):
        dsl = {
            "query": {
                "term": dsl_term
            }
        }
        return self.es_query(index_name, dsl, qfrom, qsize, includes, excludes, sort)

    '''
    terms查询:
    qterms = {
        "filename": [
            "66666666", "666666",
        ]
    }
    '''

    @query_pack_ret
    def es_query_terms(self, index_name, dsl_terms, qfrom=None, qsize=None, includes=[], excludes=[], sort=None):
        dsl = {
            "query": {
                "terms": dsl_terms
            }
        }
        return self.es_query(index_name, dsl, qfrom, qsize, includes, excludes, sort)

    '''
    ids查询
    qids = {
        "values": [
            'S6EjfoQB0vOEyrc-xgeN',
            'SqEjfoQB0vOEyrc-xgeH',
            'SaEjfoQB0vOEyrc-xgd9'
        ]
    }
    '''

    @query_pack_ret
    def es_query_ids(self, index_name, dsl_ids, qfrom=None, qsize=None, includes=[], excludes=[], sort=None):
        dsl = {"query": {
            "ids": dsl_ids
        }
        }
        return self.es_query(index_name, dsl, qfrom, qsize, includes, excludes, sort)

    @query_pack_ret
    def es_query_match_all(self, index_name, qfrom=None, qsize=None, includes=[], excludes=[], sort=None):
        dsl = {
            "query": {
                "match_all": {
                }
            }
        }
        return self.es_query(index_name, dsl, qfrom, qsize, includes, excludes, sort)

    '''
    match查询
    dsl_match = {
        "title": "四川宜宾"
    }
    '''

    @query_pack_ret
    def es_query_match(self, index_name, dsl_match, qfrom=None, qsize=None, includes=[], excludes=[], sort=None):
        dsl = {
            "query": {
                "match": dsl_match
            }
        }
        return self.es_query(index_name, dsl, qfrom, qsize, includes, excludes, sort)

    '''
    multi_match查询
    mul_match = {
        "query": "酒都宜宾",
        "fields": ["titile", "msg"],
    }
    '''

    @query_pack_ret
    def es_query_match_multi(self, index_name, dsl_mu, qfrom=None, qsize=None, includes=[], excludes=[], sort=None):
        dsl = {
            "query": {
                "multi_match": dsl_mu
            }
        }
        return self.es_query(index_name, dsl, qfrom, qsize, includes, excludes, sort)

    '''
    bool查询
    key = must or should or must_not
    cond = [
        {
            "term": {
                "gender": 0,
            }
        },
        {
            "term": {
                "status": 2,
            }
        },
    ]
    '''

    @query_pack_ret
    def es_query_match_bool(self, index_name, key='must', cond=[], qfrom=None, qsize=None, includes=[], excludes=[],
                            sort=None, highlight=None):
        dsl = {
            "query": {
                "bool": {
                    key: cond,
                }
            }
        }
        return self.es_query(index_name, dsl, qfrom, qsize, includes, excludes, sort, highlight)


def e_install(ip_list=[], timeout=1000):
    global edbpool
    edbpool = ESquery(ip_list, timeout)
    return edbpool


def test1():
    host = [{'host': '127.0.0.1', 'port': 9200}, {'host': '127.0.0.1', 'port': 9200}]
    timeout = 3600
    es = ESquery(host, timeout)
    log.info('es init succ')


def test2():
    host = [{'host': '127.0.0.1', 'port': 9200}]
    timeout = 3600
    eq = ESquery(host, timeout)
    ret = eq.es_create_index(index_name='ex_test1')
    log.debug("ret: %s", ret)


def test3():
    host = [{'host': '127.0.0.1', 'port': 9200}]
    timeout = 3600
    eq = ESquery(host, timeout)

    ret = eq.es_create_index(index_name='ex_test2')
    log.debug("ret: %s", ret)
    ret = eq.es_del_index(index_name='ex_test2')
    log.debug("ret: %s", ret)


def test4():
    host = [{'host': '127.0.0.1', 'port': 9200}]
    timeout = 3600
    eq = ESquery(host, timeout)
    mapping = {
        "properties": {
            "filename": {"type": "keyword"},
            "url": {"type": "text"},
            "status": {"type": "integer"},
            "date": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "county": {"type": "text"},
            "total": {"type": "integer"},
            "gender": {"type": "integer"},
            "agelow": {"type": "integer"},
            "agehigh": {"type": "integer"}
        }
    }
    ret = eq.es_create_index(index_name='ex_test4')
    log.debug('ret: %s', ret)
    ret = eq.es_modify_mapping(index_name='ex_test4', mapping_dsl=mapping)
    log.debug('ret: %s', ret)
    ret = eq.es_get_mapping(index_name='ex_test4')
    log.debug('ret: %s', ret)

    dsl = {
        "properties": {
            "test_add": {
                "type": "keyword",
            }
        }
    }
    ret = eq.es_modify_mapping(index_name='ex_test4', mapping_dsl=dsl)
    log.debug('ret: %s', ret)

    ret = eq.es_get_mapping(index_name='ex_test4')
    log.debug('ret: %s', ret)
    ret = eq.es_del_index(index_name='ex_test4')
    log.debug('ret: %s', ret)


def test5():
    host = [{'host': '127.0.0.1', 'port': 9200}]
    timeout = 3600
    eq = ESquery(host, timeout)
    data = {
        "filename": '1233445',
        "url": '/root',
        "status": 0,
        "date": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        # "date": datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'),
        "total": 100,
        "county": '111111 111112',
        "gender": 0,
        "agelow": 12,
        "agehigh": 18
    }
    ret = eq.es_create_index(index_name='ex_test5')
    mapping = {
        "properties": {
            "filename": {"type": "keyword"},
            "url": {"type": "text"},
            "status": {"type": "integer"},
            "date": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "county": {"type": "text"},
            "total": {"type": "integer"},
            "gender": {"type": "integer"},
            "agelow": {"type": "integer"},
            "agehigh": {"type": "integer"}
        }
    }
    ret = eq.es_modify_mapping(index_name='ex_test5', mapping_dsl=mapping)
    log.debug('ret: %s', ret)
    ret = eq.es_create_data('ex_test5', data, id='123456')
    log.debug('id: %s ret: %s', id, ret)
    ret = eq.es_del_index(index_name='ex_test5')
    log.debug('ret: %s', ret)


def test6():
    host = [{'host': '127.0.0.1', 'port': 9200}]
    timeout = 3600
    eq = ESquery(host, timeout)
    data = {
        "filename": '1233445',
        "url": '/root',
        "status": 0,
        "date": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        # "date": datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'),
        "total": 100,
        "county": '111111 111112',
        "gender": 0,
        "agelow": 12,
        "agehigh": 18
    }
    ret = eq.es_create_index(index_name='ex_test5')
    mapping = {
        "properties": {
            "filename": {"type": "keyword"},
            "url": {"type": "text"},
            "status": {"type": "integer"},
            "date": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "county": {"type": "text"},
            "total": {"type": "integer"},
            "gender": {"type": "integer"},
            "agelow": {"type": "integer"},
            "agehigh": {"type": "integer"}
        }
    }
    ret = eq.es_modify_mapping(index_name='ex_test5', mapping_dsl=mapping)
    log.debug('ret: %s', ret)
    ret = eq.es_create_data('ex_test5', data, id='123456')
    log.debug("ret: %s", ret)
    dsl = {
        'script': "ctx._source.file_name = '000000'; ctx._source.url = 'xxx'"
    }
    ret = eq.es_update_data('ex_test5', id='123456', dsl=dsl)
    log.debug("ret: %s", ret)
    ret = eq.es_del_index(index_name='ex_test5')
    log.debug('ret: %s', ret)


def test7():
    host = [{'host': '127.0.0.1', 'port': 9200}]
    timeout = 3600
    eq = ESquery(host, timeout)
    data = {
        "filename": '1233445',
        "url": '/root',
        "status": 0,
        "date": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        # "date": datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'),
        "total": 100,
        "county": '111111 111112',
        "gender": 0,
        "agelow": 12,
        "agehigh": 18
    }
    ret = eq.es_create_index(index_name='ex_test5')
    mapping = {
        "properties": {
            "filename": {"type": "keyword"},
            "url": {"type": "text"},
            "status": {"type": "integer"},
            "date": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "county": {"type": "text"},
            "total": {"type": "integer"},
            "gender": {"type": "integer"},
            "agelow": {"type": "integer"},
            "agehigh": {"type": "integer"}
        }
    }
    ret = eq.es_modify_mapping(index_name='ex_test5', mapping_dsl=mapping)
    log.debug('ret: %s', ret)
    ret = eq.es_create_data('ex_test5', data)
    log.debug("ret: %s", ret)
    ret = eq.es_create_data('ex_test5', data)
    log.debug("ret: %s", ret)
    ret = eq.es_create_data('ex_test5', data)
    log.debug("ret: %s", ret)
    # query = {
    #    'bool': {
    #        'must': [
    #            {
    #                'term': {
    #                    'total': 100,
    #                },
    #            }
    #        ] 
    #    }
    # }
    # script = {
    #    "inline": "ctx._source.status = params.status",
    #    "params": {
    #        "status": 2 
    #    },
    #    "lang": "painless",
    # }
    # ret = eq.es_update_by_query(index_name='ex_test5', q_dsl=query, script_dsl=script)
    # log.debug("ret: %s", ret)
    #
    # ret = eq.es_del_index(index_name='ex_test5')
    # log.debug('ret: %s', ret)


def test8():
    host = [{'host': '127.0.0.1', 'port': 9200}]
    timeout = 3600
    eq = ESquery(host, timeout)
    query = {
        'bool': {
            'must': [
                {
                    'term': {
                        'total': 100,
                    },
                }
            ]
        }
    }
    script = {
        "inline": "ctx._source.filename = params.filename;ctx._source.url=params.url",
        "params": {
            "filename": "66666666",
            "url": "www.sohu.com",
        },
        "lang": "painless",
    }
    ret = eq.es_update_by_query(index_name='ex_test5', q_dsl=query, script_dsl=script)
    log.debug("ret: %s", ret)

    # ret = eq.es_del_index(index_name='ex_test5')
    # log.debug('ret: %s', ret)


def test9():
    host = [{'host': '127.0.0.1', 'port': 9200}]
    timeout = 3600
    eq = ESquery(host, timeout)
    qterm = {"filename": "66666666"}
    # ret = eq.es_query_term("ex_test5", qterm, "1", "3", includes=["date", "county"], excludes=["agehigh"])
    ret = eq.es_query_term("ex_test5", qterm, "1", "3", includes=[], excludes=[])
    log.debug(ret)


def test10():
    host = [{'host': '127.0.0.1', 'port': 9200}]
    timeout = 3600
    eq = ESquery(host, timeout)
    qterms = {
        "filename": [
            "66666666", "666666",
        ]
    }
    ret = eq.es_query_terms("ex_test5", qterms, "1", "3", includes=[], excludes=[])
    log.debug(ret)


def test11():
    host = [{'host': '127.0.0.1', 'port': 9200}]
    timeout = 3600
    eq = ESquery(host, timeout)

    qids = {
        "values": [
            'S6EjfoQB0vOEyrc-xgeN',
            'SqEjfoQB0vOEyrc-xgeH',
            'SaEjfoQB0vOEyrc-xgd9',
            'ZqFhf4QB0vOEyrc-ZgdE',
        ]
    }

    sort = {
        "date": {
            "order": "desc",
        }
    }
    ret = eq.es_query_ids("ex_test5", qids, "0", "4", includes=[], excludes=[], sort=sort)
    log.debug(ret)


def test12():
    host = [{'host': '127.0.0.1', 'port': 9200}]
    timeout = 3600
    eq = ESquery(host, timeout)
    qmatch = {
        "msg": "酒都宜宾"
    }
    ret = eq.es_query_match("ex_test6", qmatch)
    log.debug(ret)


def test13():
    host = [{'host': '127.0.0.1', 'port': 9200}]
    timeout = 3600
    eq = ESquery(host, timeout)
    data1 = {
        "date": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "title": '五粮液，四川省宜宾市特产，中国国家地理标志产品。',
        "msg": "以五粮液为代表的中国白酒，有着4000多年的酿造历史，堪称世界最古老、最具神秘特色的食品制造产业之一。五粮液酒产自世界十大烈酒产区之一、中国酒都宜宾，以高粱、大米、糯米、小麦、玉米五种谷物为原料，以古法工艺配方酿造而成，是世界上率先采用五种粮食进行酿造的烈性酒，其多粮固态酿造历史传承逾千年。"
    }

    data2 = {
        "date": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "title": "茅台酒，贵州省遵义市仁怀市茅台镇特产，中国国家地理标志产品。",
        "msg": "茅台酒是中国的传统特产酒。与苏格兰威士忌、法国科涅克白兰地齐名的世界三大蒸馏名酒之一，同时是中国三大名酒“茅五剑”之一。也是大曲酱香型白酒的鼻祖，已有800多年的历史。",

    }
    ret = eq.es_create_index(index_name='ex_test6')
    mapping = {
        "properties": {
            "title": {
                "type": "text",
                "analyzer": "ik_max_word",
                "search_analyzer": "ik_max_word",
            },
            "msg": {
                "type": "text",
                "analyzer": "ik_max_word",
                "search_analyzer": "ik_max_word",
                "term_vector": "no",
                "store": "false",
            },
            "date": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
        }
    }
    ret = eq.es_modify_mapping(index_name='ex_test6', mapping_dsl=mapping)
    log.debug('ret: %s', ret)
    ret = eq.es_create_data('ex_test6', data1)
    ret = eq.es_create_data('ex_test6', data2)


def test14():
    host = [{'host': '127.0.0.1', 'port': 9200}]
    timeout = 3600
    eq = ESquery(host, timeout)
    mul_match = {
        "query": "酱香",
        "fields": ["titile", "msg"],
    }
    ret = eq.es_query_match_multi("ex_test6", mul_match)
    log.debug(ret)


def test15():
    host = [{'host': '127.0.0.1', 'port': 9200}]
    timeout = 3600
    eq = ESquery(host, timeout)
    ret = eq.es_query_match_all("ex_test6")
    log.debug(ret)


def test16():
    host = [{'host': '127.0.0.1', 'port': 9200}]
    timeout = 3600
    eq = ESquery(host, timeout)

    cond = [
        {
            "term": {
                "gender": 0,
            }
        },
        {
            "term": {
                "status": 2,
            }
        },
    ]
    ret = eq.es_query_match_bool("ex_test5", cond=cond)
    log.debug(ret)


def test17():
    e_install(ip_list=[{'host': '127.0.0.1', 'port': 9200}], timeout=3600)
    ret = edbpool.es_query_match_all("ex_test6")
    log.debug(ret)


if __name__ == '__main__':
    from zbase3.base import logger

    logger.install('stdout')
    # test1()
    # test2()
    # test3()
    # test4()
    # test5()
    # test6()
    # test7()
    # test8()
    # test9()
    # test10()
    # test11()
    # test12()
    # test13()
    # test14()
    # test15()
    # test16()
    test17()
