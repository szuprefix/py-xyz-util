import os, logging, json

REGIONS = [
    'cn-hangzhou',
    'cn-shanghai',
    'cn-qingdao',
    'cn-beijing',
    'cn-zhangjiakou',
    'cn-shenzhen',
    'cn-chengdu',
    'cn-hongkong',
    'cn-wulanchabu',
    'cn-huhehaote',
    'ap-southeast-1',
    'ap-southeast-3',
    'ap-southeast-5',
    'ap-southeast-7',
    'ap-northeast-1',
    'ap-northeast-2',
    'eu-central-1',
    'eu-west-1',
    'us-west-1',
    'us-east-1'
]
CN_REGIONS = [a for a in REGIONS if a.startswith('cn-')]
NCN_REGIONS = [a for a in REGIONS if not a.startswith('cn-')]

class FC():

    def __init__(self,
                 key_id=os.getenv('ALIBABA_CLOUD_ACCESS_KEY_ID'),
                 key_secret=os.getenv('ALIBABA_CLOUD_ACCESS_KEY_SECRET'),
                 token=os.getenv('ALIBABA_CLOUD_SECURITY_TOKEN'),
                 account_id=os.getenv('FC_ACCOUNT_ID'),
                 region=os.getenv('FC_REGION', 'us-west-1'),
                 timeout=int(os.getenv('FC_TIMEOUT', 10000))
                 ):
        from alibabacloud_fc20230330.client import Client
        from alibabacloud_tea_openapi.models import Config

        config = Config(
            access_key_id=key_id,
            access_key_secret=key_secret,
            security_token=token,
            endpoint=f"{account_id}.{region}.fc.aliyuncs.com",
            read_timeout=1000*timeout,
            connect_timeout=1000*timeout
        )

        self.client = Client(config)

    def invoke(self, function_name=os.getenv('FC_FUNCTION_NAME'), type='Sync', **kwargs):
        from alibabacloud_fc20230330.models import InvokeFunctionRequest, InvokeFunctionHeaders
        from alibabacloud_tea_util.models import RuntimeOptions
        req = InvokeFunctionRequest(**kwargs)
        headers = InvokeFunctionHeaders(x_fc_invocation_type=type)
        return self.client.invoke_function_with_options(function_name, req, headers, RuntimeOptions())

    def post_async(self, event, data={}, **kwargs):
        return self.invoke(type='Async', body=json.dumps(dict(event=event, kwargs=data)), **kwargs)

    def post(self, event, data, **kwargs):
        rs = self.invoke(body=json.dumps(dict(event=event, kwargs=data)), **kwargs)
        s = rs.body.read()
        if isinstance(s, bytes):
            s = s.decode()
        try:
            return json.loads(s)
        except:
            return s

    def get_http_trigger_url(self, function_name, **kwargs):
        from alibabacloud_fc20230330.models import ListTriggersRequest
        req = ListTriggersRequest(**kwargs)
        return self.client.list_triggers(function_name, req)

    def get_async_task(self, tid, function_name=os.getenv('FC_FUNCTION_NAME'), **kwargs):
        from alibabacloud_fc20230330.models import GetAsyncTaskRequest
        req = GetAsyncTaskRequest(**kwargs)
        return self.client.get_async_task(function_name, tid, req)

    @classmethod
    def get_instance(cls):
        instance = getattr(cls, '_instance', None)
        if not instance:
            logging.info('new instance')
            instance = cls()
            setattr(cls, '_instance', instance)
        return instance


