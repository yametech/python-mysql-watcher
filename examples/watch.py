#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 27/1/2021 18:32
from pymysqlreplication.watcher import Watcher

if __name__ == '__main__':
    def handle(item): print(item)


    MYSQL_SETTINGS = {
        "host": "10.200.100.200",
        "port": 3306,
        "user": "root",
        "passwd": "Abc12345"
    }

    # results = select * from test.a where name = '222'
    # for item in results: handle(item)

    Watcher(MYSQL_SETTINGS, 'test', 'a', handle, dict()).run()
