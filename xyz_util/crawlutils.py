# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals
import re
from requests.exceptions import ProxyError, ConnectionError
from scrapy.http import HtmlResponse
import requests
from django.conf import settings
from datetime import datetime
import hashlib
import logging

log = logging.getLogger('django')

UA_MOBILE = 'Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1'
UA_PC = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36'

PROXY = getattr(settings, 'HTTP_PROXY', None)


def md5(s):
    return hashlib.md5(s.encode('utf8')).hexdigest()


def import_function(s):
    import importlib
    ps = s.split(':')
    try:
        m = importlib.import_module(ps[0])
        func = getattr(m, ps[1])
        return func
    except:
        return s


if PROXY:
    PROXY = import_function(PROXY)


def get_redirect_url(url):
    r = requests.get(url, allow_redirects=False)
    return r.headers['Location']


def http_get(url, **kwargs):
    return http_request(url, **kwargs)


def http_post(url, **kwargs):
    return http_request(url, **kwargs)


def http_request(url, data=None, mobile_mode=True, cookies='', referer=None, extra_headers={}, timeout=(20, 20),
                 proxy=True, allow_redirects=True):
    headers = {
        "User-Agent": UA_MOBILE if mobile_mode else UA_PC,
        "Accept-Encoding": "gzip"
    }
    headers.update(extra_headers)
    if referer:
        headers['Referer'] = referer
    if proxy is True:
        proxy = PROXY
    from inspect import isgeneratorfunction
    if isgeneratorfunction(proxy):
        ps = proxy()
    else:
        ps = [proxy]
    log.info('http_get: %s', url)
    for p in ps:
        btime = datetime.now()
        try:
            if callable(p):
                p = p()
            proxies = {'http': 'http://' + p, 'https': 'http://' + p} if p else None
            if data:
                r = requests.post(url, data, headers=headers, timeout=timeout, proxies=proxies, cookies=cookies, allow_redirects=allow_redirects)
            else:
                r = requests.get(url, headers=headers, timeout=timeout, proxies=proxies, cookies=cookies, allow_redirects=allow_redirects)
            if 'charset' not in r.headers.get('Content-Type', ''):
                r.encoding = 'utf8'
            if p:
                log.info('proxy %s visit %s spent %s seconds', p, url, (datetime.now() - btime).seconds)
            return r
        except (ProxyError, ConnectionError) as e:
            import traceback
            log.warn('proxy %s by %s spent %s seconds error: %s', url, p, (datetime.now() - btime).seconds, traceback.format_exc())
    if proxy:
        raise ProxyError('all proxies failed')


def ScrapyResponse(url, **kwargs):
    encoding = kwargs.pop('encoding', None)
    r = http_get(url, **kwargs)
    hr = HtmlResponse(url=r.url, encoding=encoding or 'utf8', body=r.content)
    setattr(hr, 'r', r)
    return hr


def extract_url(s):
    import re
    s = s.replace('\xc2\xa0', ' ')
    r = re.compile(r'\s')
    ps = r.split(s)
    for a in ps:
        for sc in ['http://', 'https://']:
            if sc in a:
                return a[a.index(sc):]


def trim_url(s):
    import re
    return re.compile(r'http[s]?://[^\s]*?(\s|$)').sub('', s)


def trim_text(s):
    s = trim_url(s).replace(u'\u200b', '').strip()
    s = re.compile(r'\n{2,}').sub('\n', s)
    return s


def ensure_url_schema(url):
    return ('https:' + url) if (url and url.startswith('//')) else url


def extract_between(s, a, b):
    ps = s.split(a)
    if len(ps) < 2:
        return None
    return ps[1].split(b)[0]
