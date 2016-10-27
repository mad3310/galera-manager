#!/usr/bin/env python
# -*- coding: utf-8 -*-

__import__('framework').init()

from tornado.options import options

from mimas.es.context import init_context
from mimas.es import ElasticsearchEngine

from src.api.common.appdefine import mclusterManagerDefine  #flake8: noqa
from src.api.libs.es.store import es

MAPPING = {
    "wsrep_status": {
        "properties": {
            "timestamp": {"type": "date", "index": "not_analyzed"},
            "message": {"type": "string", "index": "not_analyzed"},
            "timeout_num": {"type": "long", "index": "not_analyzed"},
            "alarm": {"type": "string", "index": "not_analyzed"},
            "node_name": {"type": "string", "index": "not_analyzed"},
            "error_record": {"type": "object", "index": "not_analyzed"}
        }
    }
}

TEMPLATE = {
    "template": "mcl_status*",
    "mappings": MAPPING,
}

def put_template(template_name, template):
    context = init_context('mcluster', servers=options.es_hosts)
    es = ElasticsearchEngine.init_by_context(context)
    r = es.put_template(template_name, template)
    print r
    return r.get('acknowledged')


if __name__ == "__main__":
    put_template('mcluster_status', TEMPLATE)
