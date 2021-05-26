# -*- coding:utf-8 -*-
from collections import OrderedDict

from django.urls import reverse
from django.db.models import Q
from django.forms import model_to_dict
from django.http import JsonResponse, HttpResponse
from django.views.generic import View
from six import text_type

from . import modelutils, formutils, dateutils, datautils


class ContextJsonDumpsMixin(object):
    json_contexts = {}

    def get_json_contexts(self, context):
        res = {}
        res.update(self.json_contexts)
        return res

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(
            dict(code=0,
                 msg='ok',
                 data=self.get_json_contexts(context)
                 ),
            encoder=datautils.JSONEncoder)


class FormResponseJsonMixin(object):
    def form_save(self, form):
        self.object = form.save()

    def form_valid(self, form):
        self.form_save(form)
        data = {}
        if hasattr(self, 'object') and self.object and hasattr(form, '_meta'):
            data['object'] = model_to_dict(self.object, form._meta.fields, form._meta.exclude)
            m = self.object._meta
            data['model'] = "%s.%s" % (m.app_label, m.model_name)
        return JsonResponse(dict(code=0, msg='ok', data=data), encoder=datautils.JSONEncoder)

    def form_invalid(self, form):
        return JsonResponse(dict(code=1, msg=u'表单检验不通过', data=dict(errors=form.errors)))

    def get_json_contexts(self, context):
        ctx = super(FormResponseJsonMixin, self).get_json_contexts(context)
        form = context['form']
        ctx['form'] = formutils.form2dict(form)
        if hasattr(form, 'instance'):
            instance = form.instance
            ctx['title'] = u"%s%s" % (instance.id is None and u'新建' or u'修改', instance)
        if hasattr(self, 'object') and self.object and hasattr(form, '_meta'):
            ctx['object'] = model_to_dict(self.object, fields=form._meta.fields, exclude=form._meta.exclude)
        return ctx


class TableResponseJsonMixin(object):
    def get_json_contexts(self, context):
        ctx = super(TableResponseJsonMixin, self).get_json_contexts(context)
        ctx['table'] = tableutils.table2dict(context['table'])
        return ctx


class ObjectResponseJsonMixin(object):
    fields_display = ['id']

    def get_fields_display(self):
        return self.fields_display

    def get_json_contexts(self, context):
        ctx = super(ObjectResponseJsonMixin, self).get_json_contexts(context)
        ctx['object'] = modelutils.object2dict4display(context['object'], self.get_fields_display())
        ctx['title'] = u"%s详情" % context['object']
        return ctx


class SearchFormMixin(object):
    """

    """
    search_form_fields = ["q"]
    search_form_extra_fields = {}
    search_fields = ["name"]
    search_date_range_field = None
    search_number_range_field = None

    def add_search_filters(self, qset):
        d = {}
        qdt = self.request.GET
        self.search_form_values = OrderedDict()
        for fn in self.get_search_form_fields():
            sv = qdt.get(fn, "")
            if sv:
                if fn == "q":
                    qset = self.get_search_results(qset, sv)
                else:
                    d[fn] = sv
            self.search_form_values[fn] = sv
        if self.search_date_range_field:
            date_begin = qdt.get("date_begin")
            date_end = qdt.get("date_end")
            if date_begin:
                d["%s__gte" % self.search_date_range_field] = dateutils.format_the_date(date_begin)
            if date_end:
                d["%s__lt" % self.search_date_range_field] = dateutils.get_next_date(date_end)
        if self.search_number_range_field:
            number_begin = qdt.get("number_begin")
            number_end = qdt.get("number_end")
            if number_begin:
                d["%s__gte" % self.search_number_range_field] = number_begin
            if number_end:
                d["%s__lte" % self.search_number_range_field] = number_end

        if d:
            qset = qset.filter(**d)
        return qset

    def get_queryset(self):
        return self.add_search_filters(super(SearchFormMixin, self).get_queryset())

    def get_search_fields(self):
        return self.search_fields

    def get_search_form_fields(self):
        return self.search_form_fields

    def get_search_fields_label(self):
        cf = lambda f: f[0] in ['^', '@', '='] and f[1:] or f
        return u"搜索" + ",".join(
            [modelutils.get_related_field_verbose_name(self.model, cf(f)) for f in self.search_fields])

    def get_search_results(self, queryset, search_term):
        """
        此代码改编自django.contrib.admin.options.ModelAdmin.get_search_results
        """
        import operator

        def construct_search(field_name):
            if field_name.startswith('^'):
                return "%s__istartswith" % field_name[1:]
            elif field_name.startswith('='):
                return "%s__iexact" % field_name[1:]
            elif field_name.startswith('@'):
                return "%s__search" % field_name[1:]
            else:
                return "%s__icontains" % field_name

        search_fields = self.get_search_fields()

        if search_fields and search_term:
            orm_lookups = [construct_search(str(search_field))
                           for search_field in search_fields]
            for bit in search_term.split():
                or_queries = [Q(**{orm_lookup: bit})
                              for orm_lookup in orm_lookups]
                queryset = queryset.filter(reduce(operator.or_, or_queries))

        return queryset

    def get_search_limit_choices(self):
        d = {}
        for f in self.get_search_form_fields():
            if f == "q":
                continue
            func_name = "search_choices_for_%s" % f
            if hasattr(self, func_name):
                choices = getattr(self, func_name)()
            else:
                choices = modelutils.get_related_field(self.model, f).choices
            d[f] = [(text_type(a), text_type(b)) for a, b in choices]
        return d

    def get_verbose_names(self):
        d = {}
        meta = self.model._meta
        for f in self.get_search_form_fields():
            if f == "q":
                continue
            vn = modelutils.get_related_field(self.model, f).verbose_name
            d[f] = vn
        return d

    def get_search_field_dependences(self):
        return {}

    def get_context_data(self, **kwargs):
        ctx = super(SearchFormMixin, self).get_context_data(**kwargs)
        ctx["search_form"] = dict(
            values=self.search_form_values,
            choices=self.get_search_limit_choices(),
            verbose_names=self.get_verbose_names(),
            extra_fields=self.search_form_extra_fields,
            search_fields_label=self.get_search_fields_label(),
            dependences=self.get_search_field_dependences()
        )
        return ctx


class SearchFormResponseJsonMixin(object):
    def get_json_contexts(self, context):
        ctx = super(SearchFormResponseJsonMixin, self).get_json_contexts(context)
        ctx['search_form'] = context['search_form']
        return ctx


def csrf_token(request):
    from django.middleware.csrf import get_token
    get_token(request)
    return JsonResponse(dict(code=0, msg="ok"))


class LoginRequiredJsView(View):
    def get(self, request, *args, **kwargs):
        from django.middleware.csrf import get_token
        get_token(request)
        if request.user.is_authenticated():
            return HttpResponse("")
        else:
            return HttpResponse("window.location.href = '%s'" % reverse("accounts:login"))
