# -*- coding: utf-8 -*-

from tornado.options import options


def _get_es():
    if not getattr(_get_es, '_es', None):
        from mimas.es.context import init_context
        from mimas.es import ElasticsearchEngine
        context = init_context('mcluster', options.es_hosts)
        es = ElasticsearchEngine.init_by_context(context)
        _get_es._es = es
    return _get_es._es

es = _get_es()
