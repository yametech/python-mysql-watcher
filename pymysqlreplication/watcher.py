# -*- coding: utf-8 -*-
import queue
import random
import time
import ctypes
import inspect

from threading import Thread
from typing import Callable, Dict

from pymysqlreplication.row_event import RowsEvent, WriteRowsEvent, UpdateRowsEvent
from pymysqlreplication.binlogstream import BinLogStreamReader
from pymysqlreplication.event import EventHandle


def _async_raise(tid, exctype):
    """raises the exception, performs cleanup if needed"""
    print("async raise error")
    tid = ctypes.c_long(tid)
    if not inspect.isclass(exctype):
        exctype = type(exctype)
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
    if res == 0:
        raise ValueError("invalid thread id")
    elif res != 1:
        # """if it returns a number greater than one, you're in trouble,
        # and you should call it again with exc=NULL to revert the effect"""
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
        raise SystemError("PyThreadState_SetAsyncExc failed")


def stop_thread(thread):
    _async_raise(thread.ident, SystemExit)


def _async_create(f):
    def wrapper(*args, **kwargs):
        t = Thread(target=f, args=args, kwargs=kwargs)
        t.setDaemon(True)
        t.start()

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

    @_async_create
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
        try:
            for binlogevent in self.stream:
                binlogevent.dump(handleEvent)
        except Exception:
            return
        finally:
            self.stream.close()

    def run(self):
        self.dump()
        try:
            while True:
                self.handle(self.q.get())
                self.q.task_done()
        except KeyboardInterrupt:
            return
        finally:
            self.q.join()
