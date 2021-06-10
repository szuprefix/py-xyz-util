# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals
import requests
from django.core.files.base import ContentFile

def read_table(f):
    import pdfplumber
    if f.startswith('http://') or  f.startswith('https://'):
        pdf = pdfplumber.load(ContentFile(requests.get(f).content))
    else:
        pdf = pdfplumber.open(f)
    ds = []
    for page in pdf.pages:
        try:
            ds.extend(page.extract_table())
        except:
            pass
    return ds