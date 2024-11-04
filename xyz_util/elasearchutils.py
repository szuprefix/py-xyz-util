# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals
from elasticsearch import Elasticsearch
import os

ES_SERVER = os.getenv('ES_SERVER', 'http://localhost:9200')
ES_APIKEY = os.getenv('ES_APIKEY')

class ESStore():
    index_name = None
    index_mapping = None

    def __init__(
            self,
            index_name=None,
            server=ES_SERVER,
            **kwargs
    ):
        # print('ES_SERVER:', server)
        if index_name:
            self.index_name = index_name
        config = dict(**kwargs)
        if ES_APIKEY and 'api_key' not in config:
            config['api_key'] = ES_APIKEY
        self.es = Elasticsearch(server, **config)
        self.create_index()

    def create_index(self):
        if self.es.indices.exists(index=self.index_name):
            return
        self.es.indices.create(index=self.index_name)

    def index(self, d):
        return self.es.index(index=self.index_name, id=d['id'], body=d)

    def get(self, id):
        return self.es.get(
            index=self.index_name,
            id=id
        )

    def count(self, **kwargs):
        return self.es.count(index=self.index_name, **kwargs)['count']

    def search(self, query, **kwargs):
        return self.es.search(
            index=self.index_name,
            query=query,
            **kwargs
        )

    def yield_hits(self, rs):
        for hit in rs['hits']['hits']:
            yield(hit["_source"])


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
