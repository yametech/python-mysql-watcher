# -*- coding: utf-8 -*-
import queue
import random
import time
from threading import Thread
from typing import Callable, Dict

from pymysqlreplication.row_event import RowsEvent, WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
from pymysqlreplication.binlogstream import BinLogStreamReader
from pymysqlreplication.event import EventHandle


def localAsync(f):
    def wrapper(*args, **kwargs):
        thr = Thread(target=f, args=args, kwargs=kwargs)
        thr.start()

    return wrapper


class Watcher:
    def __init__(self, db_settings: Dict[str, any], schema: str, table: str, _handle: Callable[[dict], Exception],
                 _filter: Dict[str, any]):
        self.q = queue.Queue()
        self.handle = _handle
        self.filter = _filter
        self.tasks = []
        self.id = random.randint(20, 1000)
        self.stream = BinLogStreamReader(connection_settings=db_settings,
                                         server_id=self.id,
                                         only_schemas=[schema],
                                         only_tables=[table],
                                         only_events=[RowsEvent, WriteRowsEvent, UpdateRowsEvent],
                                         blocking=True,
                                         skip_to_timestamp=time.time(),
                                         )

    @localAsync
    def dump(self):
        def _eventhandle(item: RowsEvent):
            for row in item.rows:
                needPut = True
                value = row.get("values", {})
                for k, v in self.filter.items():

                    if k not in value.keys():
                        needPut = False
                        break
                    if value[k] != v:
                        needPut = False
                        break

                if needPut:
                    self.q.put(value)

        handleEvent = EventHandle(_eventhandle)

        for binlogevent in self.stream:
            binlogevent.dump(handleEvent)

    def run(self):
        self.dump()
        try:
            while True:
                exception = self.handle(self.q.get())
                if exception is not None:
                    raise exception
        finally:
            self.stream.close()


