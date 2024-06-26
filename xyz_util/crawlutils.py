# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals
import re
from requests.exceptions import ProxyError, ConnectionError
import requests
from datetime import datetime
import hashlib
import logging
from time import sleep
from six import text_type

log = logging.getLogger('django')

UA_MOBILE = 'Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1'
UA_PC = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36'

try:
    from django.conf import settings

    PROXY = getattr(settings, 'HTTP_PROXY', None)
except:
    PROXY = None


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
                 proxy=True, allow_redirects=True, requests=requests.session(), no_agent=False):
    headers = {
        "User-Agent": UA_MOBILE if mobile_mode else UA_PC,
        "Accept-Encoding": "gzip"
    }
    if no_agent:
        del headers['User-Agent']

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
                r = requests.post(url, data, headers=headers, timeout=timeout, proxies=proxies, cookies=cookies,
                                  allow_redirects=allow_redirects)
            else:
                r = requests.get(url, headers=headers, timeout=timeout, proxies=proxies, cookies=cookies,
                                 allow_redirects=allow_redirects)
            if 'charset' not in r.headers.get('Content-Type', ''):
                r.encoding = 'utf8'
            if p:
                log.info('proxy %s visit %s spent %s seconds', p, url, (datetime.now() - btime).seconds)
            return r
        except (ProxyError, ConnectionError) as e:
            import traceback
            log.warn('proxy %s by %s spent %s seconds error: %s', url, p, (datetime.now() - btime).seconds,
                     traceback.format_exc())
    if proxy:
        raise ProxyError('all proxies failed')


def ScrapyResponse(url, **kwargs):
    from scrapy.http import HtmlResponse
    encoding = kwargs.pop('encoding', None)
    r = http_get(url, **kwargs)
    hr = HtmlResponse(url=r.url, encoding=encoding or 'utf8', body=r.content)
    setattr(hr, 'r', r)
    maintain_cookies(r, kwargs.get('cookies'))
    return hr


def maintain_cookies(response, cookies):
    if cookies is None:
        return
    cookies.update(response.cookies)


def extract_url(s):
    import re
    s = s.replace('\xc2\xa0', ' ')
    r = re.compile(r'\s')
    ps = r.split(s)
    for a in ps:
        for sc in ['http://', 'https://']:
            if sc in a:
                return a[a.index(sc):]


def is_url(url):
    if not url or not isinstance(url, text_type):
        return False
    import re
    RE_URL = re.compile('^https?://')
    return bool(url and RE_URL.match(url))


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


MOBILE_EMULATION = {
    "deviceMetrics": {"width": 360, "height": 640, "pixelRatio": 3.0},  # 定义设备高宽，像素比
    "userAgent": UA_MOBILE
}


class Browser(object):

    def __init__(self, mobile_mode=False):
        self.mobile_mode = mobile_mode
        self.extracted_data = {}
        self.reload()

    def reload(self, url=False):
        if hasattr(self, 'driver'):
            try:
                if url is True:
                    url = self.driver.current_url
                self.driver.close()
            except:
                pass
        try:
            import undetected_chromedriver as webdriver
            print('#######using undetected_chromedriver#######')
        except:
            from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        options = Options()
        if self.mobile_mode:
            options.add_experimental_option('mobileEmulation', MOBILE_EMULATION)
        else:
            # options.add_experimental_option("excludeSwitches", ["enable-automation"])
            # options.add_experimental_option('useAutomationExtension', False)
            # options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("user-agent=%s" % UA_PC)
        self.driver = webdriver.Chrome(options=options)
        if isinstance(url, text_type):
            self.driver.get(url)

    def element(self, css):
        from selenium.webdriver.support.wait import WebDriverWait
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        return WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, css)))

    def get(self, url):
        self.driver.get(url)

    def get_bs_root(self):
        from bs4 import BeautifulSoup
        return BeautifulSoup(self.driver.page_source, 'html.parser')

    def extract(self, key, ele_path, attribute=None):
        e = self.element(ele_path)
        self.extracted_data[key] = e.get_attribute(attribute) if attribute else e.text

    def get_element_value(self, element, path, limit=1):
        ps = path.split("::")
        es = element.select(ps[0], limit=limit)
        from .datautils import access
        rs = [access(e, ps[1]) if len(ps) > 1 else e for e in es]
        if limit == 1:
            return None if not rs else rs[0]
        return rs

    def start_extract(self):
        self.extracted_data = {}

    def element_to_bs(self, e):
        from bs4 import BeautifulSoup
        return BeautifulSoup(e.get_attribute('outerHTML'), 'html.parser')

    def switch_iframe(self, frame_id):
        self.element("#%s" % frame_id)
        self.driver.switch_to.frame(frame_id)

    def backspace_clean(self, element, deviation=3):
        """
        退格原理清除输入框中的内容
        :param element: 需要操作的元素
        :param deviation: 退格数偏差,默认会多输入3个以确保可靠度
        """
        if isinstance(element, text_type):
            element = self.element(element)
        from selenium.webdriver.common.keys import Keys
        quantity = len(element.get_attribute("value")) + deviation
        for _ in range(quantity):
            element.send_keys(Keys.BACKSPACE)

    def clean_with_send(self, element, text, **kwargs):
        """
        清空输入框并且输入内容
        :param element: 需要操作的元素
        :param text: 输入的内容
        """
        self.backspace_clean(element, **kwargs)
        element.send_keys(text)

    def run_script(self, script):
        for l in script.split('\n'):
            s = l.strip()
            if not s:
                continue
            eval('self.%s' % s)

    def extract_data(self, conf, element=None):
        if not element:
            element = self.get_bs_root()
        if not conf:
            return dict(html=self.element('html').get_attribute('outerHTML'))
        if isinstance(conf, text_type):
            return self.get_element_value(element, conf)
        if isinstance(conf, (list, tuple)):
            ls = []
            for a in conf:
                ls += self.extract_data(a, element)
            return ls
        ls = []
        ep = conf.get('$', None)
        es = [element]
        if ep:
            es = self.get_element_value(element, ep, limit=None)
        for e in es:
            d = {}
            for k, v in conf.items():
                if k == '$':
                    continue
                d[k] = self.extract_data(v, e)
            ls.append(d)
        return ls if ep else ls[0]


def retry(func, times=3, interval=10):
    ts = times
    while True:
        try:
            return func()
        except:
            import traceback
            ts -= 1
            if ts > 0:
                traceback.print_exc()
                print('retrying...')
                sleep(interval)
                continue
            raise Exception('retry failed')


def readability_summary(html, html_partial=True):
    if is_url(html):
        html = ScrapyResponse(html).text
    from readability import Document
    doc = Document(html)
    return doc.summary(html_partial=html_partial)


def html2text(text):
    return re.sub('<.*?>', '', text, flags=re.M | re.S) \
        .replace('&quot;', '"') \
        .replace('&copy;', '©') \
        .replace('&lt;', '<') \
        .replace('&gt;', '>') \
        .replace('&amp;', '&') \
        .replace('&#39;', "'") \
        .replace('&apos;', "'")


def html_get_text(html):
    import bs4
    return bs4.BeautifulSoup(html, 'html.parser').get_text().strip()


def clean_html(html):
    if not html:
        return ''
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')

    for tag in soup.find_all(True):
        fs = list(tag.attrs.keys())
        for f in fs:
            if f == 'href' and tag.name == 'a' and 'javascript:' not in tag.attrs[f]:
                continue
            if f == 'src' and tag.name == 'img':
                continue
            del tag[f]
        if tag.name == 'a':
            tag.attrs['target'] = '_blank'
    return str(soup)


class Reader:

    def __init__(self, main_css=None):
        self.main_css = main_css

    def read_images(self, r):
        for e in r.css('img'):
            yield dict(
                src=e.css('::attr("src")').get(),
                alt=e.css('::attr("alt")').get()
            )

    def read_meta(self, r, fs=['title', 'description', 'updated_time', 'image'], prefix='og:'):
        return dict([(f, r.css(f'meta[property="{prefix}{f}"]::attr(content)').get()) for f in fs])

    def read(self, r):
        if is_url(r):
            r = ScrapyResponse(r)
        if self.main_css:
            h = r.css(self.main_css).get()
        else:
            h = readability_summary(r.text)
        from scrapy.http import HtmlResponse
        pr = HtmlResponse(url=r.url, encoding='utf8', body=h)
        d = self.read_meta(r)
        d.update(
            url=r.url,
            title=r.css('head title::text').get().strip(),
            text=html_get_text(h),
            html=h,
            images=list(self.read_images(pr))
        )
        return d

