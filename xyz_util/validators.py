# -*- coding:utf-8 -*-
from __future__ import unicode_literals
from django.core.validators import validate_email, validate_slug

__author__ = 'denishuang'

from six import text_type,string_types

from . import datautils
from django.core.validators import RegexValidator

import re

RE_NOT_ALPHA = re.compile(r"[^0-9a-zA-Z]")
RE_SPACE = re.compile(r"\s")

def format_strip(v):
    if isinstance(v, string_types):
        return v.strip()
    return v

def format_half_year(v):
    if v>0 and v<70:
        return 2000+v
    elif v>=70 and v<100:
        return 1900+v
    return v


def format_not_require(v):
    if v == "":
        return None
    return v


def format_split_by_bracket(v):
    return text_type(v).split("(")[0]


def format_banjiao(value):
    if not isinstance(value, string_types):
        value = text_type(value)
    return datautils.strQ2B(value)


def format_upper(value):
    return value.upper()

format_not_float = datautils.not_float


def format_str_without_space(value):
    if value is not None:
        return RE_SPACE.sub("", text_type(value))


def format_alpha(value):
    return RE_NOT_ALPHA.sub("", value)


class IDCardValidator(RegexValidator):
    message = "身份证格式不对"
    regex = "^[1-9]\d{5}(19|2\d)\d{2}((0[1-9])|(1[0-2]))(([0|1|2]\d)|3[0-1])\d{3}([0-9]|X)$"


valid_idcard = IDCardValidator()


class WeixinIDValidator(RegexValidator):
    message = "微信号格式不对"
    regex = "^[a-zA-Z][-_\w]+$"


valid_weixinid = WeixinIDValidator()


class MobileValidator(RegexValidator):
    message = "手机格式不对"
    regex = "^1[3-9]\\d{9}$"


valid_mobile = MobileValidator()


class HanNameValidator(RegexValidator):
    regex = "^[·\u4e00-\u9fa5]{2,}$"
    message = "姓名格式不对"


valid_han_name = HanNameValidator()

valid_position = HanNameValidator(message="职位格式不对")


class QQValidator(RegexValidator):
    regex = "^\d{4,16}$"
    message = "QQ格式不对"


valid_qq = QQValidator()


class BaseField(object):
    default_validators = []
    default_formaters = [format_strip]
    name = "undefined"
    default_synonyms = []
    clear_space = True
    ignore_invalid = False
    no_duplicate = False

    def __init__(self, name=None, synonyms=[], formaters=[], validators=[]):
        self.name = name or self.name
        self._synonyms = self.default_synonyms + synonyms
        self._validators = self.default_validators + validators
        self._formaters = self.default_formaters + formaters

        if self.clear_space:
            self._formaters.append(format_str_without_space)

    def get_formaters(self):
        return self._formaters

    def get_validators(self):
        return self._validators

    def get_synonyms(self):
        return self._synonyms

    def _format(self, value):
        for f in self.get_formaters():
            value = f(value)
        return value

    def _validate(self, value):
        errors = []
        for f in self.get_validators():
            try:
                f(value)
            except Exception as e:
                errors.append(e.message)
        return errors

    def __call__(self, value):
        value = self._format(value)
        errors = self._validate(value)
        if errors and self.ignore_invalid:
            errors = []
            value = None
        return self.name, value, errors


class MobileField(BaseField):
    name = "手机"
    default_synonyms = ["手机号", "手机号码", "联系电话"]
    default_validators = [valid_mobile]
    default_formaters = [format_not_float, format_banjiao]
    no_duplicate = True


field_mobile = MobileField()


class IDCardField(BaseField):
    name = "身份证"
    default_synonyms = ["身份证号", "身份证号码"]
    default_validators = [valid_idcard]
    default_formaters = [format_not_float, format_banjiao, format_upper]
    ignore_invalid = True
    no_duplicate = True


field_idcard = IDCardField()


class WeixinIDField(BaseField):
    name = "微信号"
    default_synonyms = ["微信ID"]
    default_validators = [valid_weixinid]
    default_formaters = [format_not_float, format_banjiao]
    ignore_invalid = True
    no_duplicate = True


field_weixinid = WeixinIDField()


class HanNameField(BaseField):
    name = "姓名"
    default_validators = [valid_han_name]
    default_formaters = [format_banjiao, format_split_by_bracket]


field_han_name = HanNameField()


class PositionField(BaseField):
    name = "职位"
    default_validators = []


field_position = PositionField()


class QQField(BaseField):
    name = "QQ"
    default_synonyms = ["扣扣", "QQ号"]
    default_validators = [valid_qq]
    default_formaters = [format_not_float, format_banjiao]
    ignore_invalid = True
    no_duplicate = True


field_qq = QQField()


class EmailField(BaseField):
    name = "邮箱"
    default_synonyms = ["EMAIL", "电子邮箱", "MAIL"]
    default_validators = [validate_email]
    default_formaters = [format_banjiao]
    ignore_invalid = True
    no_duplicate = True


field_email = EmailField()


class UseridField(BaseField):
    name = "帐号"
    default_synonyms = ["学号", "工号"]
    default_validators = [validate_slug]
    default_formaters = [format_not_float, format_banjiao, format_str_without_space]


field_userid = UseridField()
