# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals
import sys, subprocess, os


def cmd_call(cmd, without_output=False):
    if sys.version_info[0] > 2:
        res = subprocess.getstatusoutput(cmd)
        if not without_output and res and len(res):
            return res[1]
    elif os.name == 'posix':
        import commands
        return commands.getoutput(cmd)
    else:
        if without_output:
            DEVNULL = open(os.devnull, 'wb')
            subprocess.Popen(cmd, stdout=DEVNULL, stderr=DEVNULL)
        else:
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            return p.stdout.read()
