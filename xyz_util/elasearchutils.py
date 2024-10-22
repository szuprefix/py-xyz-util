# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals
from elasticsearch import Elasticsearch
import os

ES_SERVER = os.getenv('ES_SERVER', 'http://localhost:9200')
ES_USER = os.getenv('ES_USER', 'elastic')
ES_PASSWORD = os.getenv('ES_PASSWORD', 'elastic')


class ESStore():
    index_name = None
    index_mapping = None

    def __init__(
            self,
            index_name=None,
            server=ES_SERVER,
            user=ES_USER,
            password=ES_PASSWORD
    ):
        # print('ES_SERVER:', server)
        if index_name:
            self.index_name = index_name
        self.es = Elasticsearch(server, basic_auth=(user, password))
        self.create_index()

    def create_index(self):
        if self.es.indices.exists(index=self.index_name):
            return
        self.es.indices.create(index=self.index_name)

    def index(self, d):
        return self.es.index(index=self.index_name, id=d['id'], body=d)

    def get(self, id):
        return self.es.get(
            index=self.name,
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
