# -*- coding: utf-8 -*-

import os
import time
import signal
import subprocess
import datetime


class InvokeCommand():

    @classmethod
    def run_with_syn(cls, cmd):
        """
        同步执行：等待执行完毕即可返回
        """
        p = subprocess.Popen(cmd, shell=True,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT,
                             preexec_fn=os.setsid)
        stdout, stderr = p.communicate()
        p.wait()

        try:
            os.killpg(p.pid, signal.SIGTERM)
        except OSError:
            pass

        return stdout.strip()

    @classmethod
    def run_with_asyn(cls, cmd):
        """
        异步执行：立即返回，根据p.poll()的值判断是否执行完毕
        p.poll() == None 正在执行
        p.poll() == 0    执行成功
        """
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        return p

    @classmethod
    def kill_process_with_timeout(cls, process, timeout):
        """异步进程清理工作"""
        start = datetime.datetime.now()
        while process.poll() is None:
            time.sleep(0.1)
            now = datetime.datetime.now()
            if (now - start).seconds > timeout:
                try:
                    process.terminate()
                except Exception, e:
                    return e
                return None
        if process.stdin:
            process.stdin.close()
        if process.stdout:
            process.stdout.close()
        if process.stderr:
            process.stderr.close()

        try:
            process.kill()
        except OSError:
            pass
