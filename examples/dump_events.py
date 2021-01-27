#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Dump all replication events from a remote mysql server
#

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import EventHandle
from pymysqlreplication.row_event import RowsEvent, WriteRowsEvent
import time

from typing import TypeVar, Generic
from abc import ABCMeta, abstractmethod, ABC

MYSQL_SETTINGS = {
    "host": "10.200.100.200",
    "port": 3306,
    "user": "root",
    "passwd": "Abc12345"
}


def handle(item: RowsEvent): [print(row.get("values", "")) for row in item.rows]


handleEvent = EventHandle(handle)


def main():
    # server_id is your slave identifier, it should be unique.
    # set blocking to True if you want to block and wait for the next event at
    # the end of the stream
    try:
        stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
                                    server_id=3,
                                    only_schemas=['test'],
                                    only_tables=['a'],
                                    only_events=[RowsEvent, WriteRowsEvent],
                                    blocking=True,
                                    skip_to_timestamp=time.time(),
                                    )

        for binlogevent in stream:
            binlogevent.dump(handleEvent)
    finally:
        stream.close()


if __name__ == "__main__":
    main()
