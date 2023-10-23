# -*- coding:utf-8 -*-
from __future__ import unicode_literals
import re
from decimal import Decimal
from six import text_type
from django.core.serializers.json import DjangoJSONEncoder
from datetime import date, datetime
from collections import OrderedDict


class JSONEncoder(DjangoJSONEncoder):
    def default(self, o):
        from django.db.models.fields.files import FieldFile
        from django.db.models import Model, QuerySet
        if isinstance(o, (FieldFile,)):
            return o.name
        if isinstance(o, Model):
            return o.pk
        if isinstance(o, QuerySet):
            return [self.default(a) for a in o]
        return super(JSONEncoder, self).default(o)


def jsonSpecialFormat(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, datetime):
        return v.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(v, Model):
        return v.pk
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, (FieldFile,)):
        return {'name:': v.name, 'url': v.url}
    return v


def model2dict(model, fields=[], exclude=[]):
    return dict([(attr, jsonSpecialFormat(getattr(model, attr)))
                 for attr in [f.name for f in model._meta.fields]
                 if not (fields and attr not in fields or exclude and attr in exclude)])


def queryset2dictlist(qset, fields=[], exclude=[]):
    return [model2dict(m, fields, exclude) for m in qset]


def queryset2dictdict(qset, fields=[], exclude=[]):
    return dict([(m.pk, model2dict(m, fields, exclude)) for m in qset])


def node2dict(node):
    children = node.getchildren()
    if not children:
        return node.text
    d = {}
    for n in children:
        d[n.tag] = node2dict(n)
    return d


def str2int(s):
    try:
        return int(s)
    except ValueError:
        try:
            return int(re.finditer(r'\d+', s).next().group())
        except:
            return None


def xml2dict(xml):
    from lxml import etree
    return node2dict(etree.fromstring(xml))


def dict2xml(d):
    ks = d.keys()
    ks.sort()
    return "\n".join(["<%s><![CDATA[%s]]></%s>" % (k, d[k], k) for k in ks if d[k]])


def dictlist2arraylist(dict_data, field_names):
    return [[row[fn] for fn in field_names] for row in dict_data]


def node2model(node, model, timestamp_fields=[], name_format_func=None):
    if name_format_func == None:
        name_format_func = lambda x: x[0].lower() + x[1:]
    for cnode in node.getchildren():
        tg = name_format_func(cnode.tag)
        tx = cnode.text
        if tg in timestamp_fields:
            tx = datetime.fromtimestamp(int(tx))
        setattr(model, tg, tx)


def xml2model(xml, model):
    from lxml import etree
    return node2model(etree.fromstring(xml))


def re_group_split(r, s):
    g = []
    p = 0
    t = None
    for m in r.finditer(s):
        g.append((t, s[p:m.start()]))
        p = m.end()
        t = m.groups()
    g.append((t, s[p:]))
    return g


def group_by(data, groups):
    res = {}
    if isinstance(groups, text_type):
        groups = [groups]
    gc = len(groups)
    for d in data:
        # for i in range(gc):
        if gc > 1:
            k = tuple([d.get(g) for g in groups])
        else:
            k = d.get(groups[0])
        res.setdefault(k, []).append(d)
    return res


def count_group_by(data, groups):
    """

      dl = [
         {"f1":"a1","f2":"b1","f3":"c1"},
         {"f1":"a1","f2":"b2","f3":"c1"},
         {"f1":"a1","f2":"b2","f3":"c2"},
         {"f1":"a1","f2":"b2","f3":"c2"},
         {"f1":"a2","f2":"b1","f3":"c2"},
         {"f1":"a2","f2":"b1","f3":"c2"},
         {"f1":"a2","f2":"b1","f3":"c2"},
         {"f1":"a2","f2":"b2","f3":"c2"},
         {"f1":"a2","f2":"b2","f3":"c1"},
      ]

    >>> count_group_by(dl,["f2"])
    {('b1',): 4, ('b2',): 5}

    >>> count_group_by(dl,["f2","f3"])
    {('b1', 'c2'): 3, ('b1',): 4, ('b1', 'c1'): 1, ('b2',): 5, ('b2', 'c1'): 2, ('b2', 'c2'): 3}

    >>> count_group_by(dl,["f1","f2"])
    {('a1', 'b2'): 3, ('a1',): 4, ('a1', 'b1'): 1, ('a2', 'b2'): 2, ('a2',): 5, ('a2', 'b1'): 3}

    >>> count_group_by(dl,["f1","f2","f3"])
    {('a1', 'b2', 'c1'): 1, ('a2', 'b2', 'c1'): 1, ('a2', 'b1', 'c2'): 3, ('a2',): 5, ('a2', 'b2', 'c2'): 1, ('a1', 'b2', 'c2'): 2, ('a1', 'b2'): 3, ('a1',): 4, ('a1', 'b1'): 1, ('a2', 'b2'): 2, ('a1', 'b1', 'c1'): 1, ('a2', 'b1'): 3}


    >>> cr = count_group_by(dl,["f1","f2","f3"])
    >>> for k in sorted(cr.keys()):
    ...     print("\t"*len(k),k[-1], cr[k])
    ...
    """
    res = {}
    gc = len(groups)
    for d in data:
        for i in range(gc):
            k = tuple([d.get(g) for g in groups[:i + 1]])
            res.setdefault(k, 0)
            res[k] += 1
    return res


def str2dict(s, line_spliter='\n', key_spliter=':'):
    d = OrderedDict()
    s = s.strip()
    if not s:
        return d
    for a in s.split(line_spliter):
        a = a.strip()
        if not a:
            continue
        p = a.find(key_spliter)
        if p == -1:
            d[a] = ""
        else:
            d[a[:p].strip()] = a[p + 1:].strip()
    return d


def str2list(s, line_spliter='\n', field_spliter='\t'):
    for l in s.split(line_spliter):
        yield l.split(field_spliter)


def dict2str(d, line_spliter='\n', key_spliter=':'):
    return line_spliter.join(["%s%s%s" % (k, key_spliter, v) for k, v in d.items()])


def not_float(d):
    if isinstance(d, (float,)):
        return int(d)
    return d


def phonemask(value):
    if not value:
        return value
    l = list(value.replace(" ", ""))
    l[3:7] = "****"
    return "".join(l)


def strQ2B(ustring):
    """全角转半角"""
    if not ustring:
        return ustring
    rstring = ""
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code == 12288 or inside_code == 8197:  # 全角空格直接转换
            inside_code = 32
        if inside_code == 12290:  # 。 -> .
            inside_code = 46
        elif (inside_code >= 65281 and inside_code <= 65374):  # 全角字符（除空格）根据关系转化
            inside_code -= 65248

        rstring += unichr(inside_code)
    return rstring


def strB2Q(ustring):
    """半角转全角"""
    if not ustring:
        return ustring
    rstring = ""
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code == 32:  # 半角空格直接转化
            inside_code = 12288
        elif inside_code >= 32 and inside_code <= 126:  # 半角字符（除空格）根据关系转化
            inside_code += 65248

        rstring += unichr(inside_code)
    return rstring


def list2csv(data, line_spliter="\n", field_spliter="\t"):
    s = []
    for line in data:
        s.append(field_spliter.join([text_type(v) for v in line]))
    return line_spliter.join(s)


def csv2list(csv, line_spliter="\n", field_spliter="\t"):
    return [l.split(field_spliter) for l in csv.split(line_spliter)]


def csv2dictlist(csv, line_spliter="\n", field_spliter="\t"):
    ls = csv.split(line_spliter)
    fns = ls[0].split(field_spliter)
    rs = []
    for l in ls[1:]:
        d = dict([(fns[i], a) for i, a in enumerate(l.split(field_spliter))])
        rs.append(d)
    return rs


def exclude_dict_keys(d, *args):
    return dict([(k, v) for k, v in d.items() if k not in args])


def choices_map_reverse(choices):
    return dict([(v, k) for k, v in choices])


def clear_dict_keys(d, *args):
    for a in args:
        if a in d:
            d.pop(a)


def gen_sub_dict(d, prefix=None):
    nd = {}
    p = len(prefix)
    for k, v in d.items():
        if k.startswith(prefix):
            fn = k[p:].lower()
            nd[fn] = v
    return nd


common_used_numerals = {'零': 0, '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,
                        '十': 10, '百': 100, '千': 1000, '万': 10000, '亿': 100000000}


def cn2digits(uchars_cn):
    s = uchars_cn
    if not s:
        return 0
    for i in ['亿', '万', '千', '百', '十']:
        if i in s:
            ps = s.split(i)
            lp = cn2digits(ps[0])
            if lp == 0:
                lp = 1
            rp = cn2digits(ps[1])
            # print i,s,lp,rp
            return lp * common_used_numerals.get(i, 0) + rp
    return common_used_numerals.get(s[-1], 0)


def digits2cn(number):
    _MAPPING = (
        '零', '一', '二', '三', '四', '五', '六', '七', '八', '九', '十', '十一', '十二', '十三', '十四', '十五', '十六', '十七',
        '十八', '十九')
    _P0 = ('', '十', '百', '千',)
    _S4 = 10 ** 4
    _PP = ('', '万', '亿')

    def _to_chinese4(num):
        assert (0 <= num and num < _S4)
        if num < 20:
            return _MAPPING[num]
        else:
            lst = []
            while num >= 10:
                lst.append(num % 10)
                num = num / 10
            lst.append(num)
            c = len(lst)  # 位数
            result = ''
            for idx, val in enumerate(lst):
                val = int(val)
                if val != 0:
                    result += _P0[idx] + _MAPPING[val]
                    if idx < c - 1 and lst[idx + 1] == 0:
                        result += '零'
            return result[::-1]

    if number < _S4:
        return _to_chinese4(number)
    if number < _S4 ** 2:
        return '%s万%s' % (_to_chinese4(number // _S4), _to_chinese4(number % _S4))
    return '%s亿%s万%s' % (
        _to_chinese4(number // _S4 // _S4), _to_chinese4(number // _S4 % _S4), _to_chinese4(number % _S4))


def auto_code(n):
    from xpinyin import Pinyin
    p = Pinyin()
    replace_chars = lambda s: re.sub(r"""[\(\["'\]\)]""", ' ', s)
    return replace_chars(p.get_initials(n, ''))


def try_numeric_dict(d):
    for k, v in d.items():
        try:
            d[k] = int(v)
        except ValueError:
            try:
                d[k] = float(v)
            except ValueError:
                pass
    return d


def split_test_case(s):
    cs = []
    rs = []
    for l in s.split("\n"):
        ps = l.split("=>")
        a = try_numeric_dict(str2dict(ps[0], line_spliter=" "))
        b = len(ps) > 1 and try_numeric_dict(str2dict(ps[1], line_spliter=" ")) or {}
        rs.append(a)
        cs.append(b)
    return rs, cs


RE_SPACES = re.compile(r"\s+")


def space_split(s):
    ss = RE_SPACES.split(s)
    if ss[0] == '':
        del ss[0]
    if len(ss) > 0 and ss[-1] == '':
        del ss[-1]
    return ss


def ischildof(obj, cls):
    try:
        for i in obj.__bases__:
            if i is cls or isinstance(i, cls):
                return True
        for i in obj.__bases__:
            if ischildof(i, cls):
                return True
    except AttributeError:
        return ischildof(obj.__class__, cls)
    return False


def filter_emoji(desstr, restr=''):
    '''
    过滤表情
    '''
    try:
        co = re.compile('[\U00010000-\U0010ffff]')
    except re.error:
        co = re.compile('[\uD800-\uDBFF][\uDC00-\uDFFF]')
    return co.sub(restr, text_type(desstr))


def snake_case(name):
    import re
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


class Accessor(str):
    '''
    A string describing a path from one object to another via attribute/index
    accesses. For convenience, the class has an alias `.A` to allow for more concise code.

    Relations are separated by a ``.`` character.
    '''
    SEPARATOR = '.'

    def resolve(self, context, safe=True, quiet=False):
        '''
        Return an object described by the accessor by traversing the attributes
        of *context*.

        Lookups are attempted in the following order:

         - dictionary (e.g. ``obj[related]``)
         - attribute (e.g. ``obj.related``)
         - list-index lookup (e.g. ``obj[int(related)]``)

        Callable objects are called, and their result is used, before
        proceeding with the resolving.

        Example::

            >>> x = Accessor('__len__')
            >>> x.resolve('brad')
            4
            >>> x = Accessor('0.upper')
            >>> x.resolve('brad')
            'B'

        Arguments:
            context (object): The root/first object to traverse.
            safe (bool): Don't call anything with `alters_data = True`
            quiet (bool): Smother all exceptions and instead return `None`

        Returns:
            target object

        Raises:
            TypeError`, `AttributeError`, `KeyError`, `ValueError`
            (unless `quiet` == `True`)
        '''
        try:
            current = context
            for bit in self.bits:
                try:  # dictionary lookup
                    current = current[bit]
                except (TypeError, AttributeError, KeyError):
                    try:  # attribute lookup
                        current = getattr(current, bit)
                    except (TypeError, AttributeError):
                        try:  # list-index lookup
                            current = current[int(bit)]
                        except (IndexError,  # list index out of range
                                ValueError,  # invalid literal for int()
                                KeyError,  # dict without `int(bit)` key
                                TypeError,  # unsubscriptable object
                                ):
                            raise ValueError('Failed lookup for key [%s] in %r'
                                             ', when resolving the accessor %s' % (bit, current, self)
                                             )
                if callable(current):
                    if safe and getattr(current, 'alters_data', False):
                        raise ValueError('refusing to call %s() because `.alters_data = True`'
                                         % repr(current))
                    if not getattr(current, 'do_not_call_in_templates', False):
                        current = current()
                # important that we break in None case, or a relationship
                # spanning across a null-key will raise an exception in the
                # next iteration, instead of defaulting.
                if current is None:
                    break
            return current
        except:
            if not quiet:
                raise

    @property
    def bits(self):
        if self == '':
            return ()
        return self.split(self.SEPARATOR)

    def get_field(self, model):
        '''Return the django model field for model in context, following relations'''
        if not hasattr(model, '_meta'):
            return

        field = None
        from django.core.exceptions import FieldDoesNotExist
        for bit in self.bits:
            try:
                field = model._meta.get_field(bit)
            except FieldDoesNotExist:
                break
            if hasattr(field, 'rel') and hasattr(field.rel, 'to'):
                model = field.rel.to
                continue

        return field

    def penultimate(self, context, quiet=True):
        '''
        Split the accessor on the right-most dot '.', return a tuple with:
         - the resolved left part.
         - the remainder

        Example::

            >>> Accessor('a.b.c').penultimate({'a': {'a': 1, 'b': {'c': 2, 'd': 4}}})
            ({'c': 2, 'd': 4}, 'c')

        '''
        path, _, remainder = self.rpartition('.')
        return A(path).resolve(context, quiet=quiet), remainder


A = Accessor  # alias


def access(obj, path, quiet=True):
    return Accessor(path).resolve(obj, quiet=quiet)


def import_function(s):
    import importlib
    ps = s.split(':')
    m = importlib.import_module(ps[0])
    func = getattr(m, ps[1])
    return func



def list_dict(l):
    d = {}
    for k, v in l:
        d.setdefault(k, []).append(v)
    return d


def trim_by_length(s, ml, charset='utf8'):
    l = 0
    rs = ''
    for c in s:
        l += len(c.encode(charset))
        if l > ml:
            return rs
        rs += c
        if l == ml:
            return rs
    return rs


def reorder(dl, new_orders):
    for a in new_orders:
        if a not in dl:
            continue
        yield a
    for a in dl:
        if a in new_orders:
            continue
        yield a
