# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals

import os
from .datautils import access, import_function
from elasticsearch import Elasticsearch
import os

ES_SERVER = os.getenv('ES_SERVER', 'http://localhost:9200')
ES_USER = os.getenv('ES_USER', 'elastic')
ES_PASSWORD = os.getenv('ES_PASSWORD', 'elastic')


class ESStore():
    index_name = None
    index_mapping = None

    def __init__(self, server=ES_SERVER, user=ES_USER, password=ES_PASSWORD):
        self.es = Elasticsearch(server, basic_auth=(user, password))

    def create_index(self):
        if self.es.indices.exists(index=self.index_name):
            return
        self.es.indices.create(index=self.index_name)

    def index(self, d):
        return self.es.index(index=self.index_name, id=d['id'], body=d)

    def count(self, **kwargs):
        return self.es.count(index=self.index_name, **kwargs)['count']

    def search(self, kwargs):
        return self.es.search(
            index=self.index_name,
            body=dict(
                query=kwargs
            )
        )

    def match_search(self, kwargs):
        return self.search(dict(match=kwargs))

    def nested_search(self, kwargs):
        path = kwargs.pop('path')
        return self.search(
            dict(
                nested=dict(
                    path=path,
                    query=dict(
                        match=kwargs
                    )
                )
            )
        )

        # for hit in response['hits']['hits']:
        #     yield(hit["_source"])
