# -*- coding: utf-8 -*-

import os


class File(object):
    '''
    文件操作
    '''

    def get_values(self, path):
        """获取文件数据
           返回类型: dict
        """
        with open(path, 'r') as f:
            lines = f.readlines()
            if not lines:
                return {}

            values = {}
            for line in lines:
                value_list = line.strip('\n').split('=')
                values[value_list[0]] = value_list[1]

            return values

    def write_values(self, path, values):
        """写数据到文件
           输入类型: dict
        """
        with open(path, 'w') as f:
            lines = f.readlines()

            content = []
            for line in lines:
                key = line.strip('\n').split("=")[0]
                if key in values:
                    content.append(key + '=' + values[key] + '\n')
            f.writelines(content)
            os.chmod(path, os.stat(path).st_mode)

    def get_fulltext(self, path):
        """写文本到文件
           输入类型: text
        """
        with open(path) as f:
            lines = f.readlines()

            text = ''
            for line in lines:
                text += line
            return text

    def write_fulltext(self, path, text):
        """写文本到文件
           输入类型: text
        """
        with open(path, 'w') as f:
            f.write(text)
