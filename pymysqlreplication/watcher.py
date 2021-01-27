# -*- coding: utf-8 -*-
import asyncio
import queue
import random
from datetime import datetime
from typing import Callable, Dict

from .row_event import RowsEvent, WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
from .binlogstream import BinLogStreamReader


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
                                         only_events=[RowsEvent, WriteRowsEvent, UpdateRowsEvent,
                                                      DeleteRowsEvent],
                                         blocking=True,
                                         skip_to_timestamp=datetime.now(),
                                         )

        def _eventhandle(item: RowsEvent): [self.q.put(row) for row in item.rows]

        def dump():
            for binlogevent in self.stream:
                binlogevent.dump(_eventhandle)

        self.stream.close()

    def run(self):
        while True:
            exception = self.handle(self.q.get())
            if exception is not None:
                raise exception


def _handle(item): print(item)


if __name__ == '__main__':
    Watcher('test', 'a', _handle, dict(status=True)).run()
