#!/usr/bin/env python
"""A class to manage the receive export items, keep the status of them"""

from export_manager.mq_server import MqServer
from export_manager.mq_client import MqClient
from telegram_export.__main__ import main
import sqlite3
import asyncio
import json
import time
from threading import Thread
from contextlib import suppress
import traceback


def mq_server_callback(body):
    print("customer callback")
    print(body)


def start_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def start_exporter(account, loop):
    config = account.get("config")
    try:
        await main(loop, config)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(e)
        print(traceback.format_exc())


class Manager(object):

    def __init__(self, mq_host="192.168.3.113", queue="rcp_queue_web"):
        self._mq_host = mq_host
        self._mq_queue = queue
        self._dumper = None
        self._runtime_task_list = []
        self._loop = asyncio.new_event_loop()
        self._loop_thread = None
        self._mq_server_thread = None
        self._mq_client = None
        self.start_loop_thread()

    def start_loop_thread(self):
        if self._loop_thread is None:
            self._loop_thread = Thread(target=start_loop, args=(self._loop,))
            self._loop_thread.start()

    def rcp_call(self, json_str):
        if not self._mq_client:
            self._mq_client = MqClient(self._mq_host, queue=self._mq_queue)
        self._mq_client.call(json_str)

    def start_runtime_task(self, account):
        """
        account: {
            id: 1,
            config: {},
        },
        task:{
            task_id: 1,
            task_runtime_future: future_obj,
        }
        """
        task_id = account.get("id")
        runtime_task = list(filter(lambda item: item.get("task_id") == task_id, self._runtime_task_list))
        if not runtime_task:
            future_obj = asyncio.run_coroutine_threadsafe(start_exporter(account, self._loop), self._loop)
            self._runtime_task_list.append({"task_id": task_id, "task_runtime_future": future_obj})
        else:
            future_obj = runtime_task[0].get("task_runtime_future")
            if future_obj is None or future_obj._state == "FINISHED":
                new_future_obj = asyncio.run_coroutine_threadsafe(start_exporter(account, self._loop), self._loop)
                self._runtime_task_list = list(filter(lambda item: item.get("task_id") != task_id, self._runtime_task_list))
                self._runtime_task_list.append({"task_id": task_id, "task_runtime_future": new_future_obj})

    def get_runtime_task_list(self):
        return self._runtime_task_list

    def get_task_list(self):
        return self._runtime_task_list

    def add_task(self, account):
        self.start_runtime_task(account)
        return True

    def set_task(self, account):
        task_id = account.get("id")
        runtime_task = list(filter(lambda item: item.get("task_id") == task_id, self._runtime_task_list))
        if not runtime_task:
            future_obj = asyncio.run_coroutine_threadsafe(start_exporter(account, self._loop), self._loop)
            self._runtime_task_list.append({"task_id": task_id, "task_runtime_future": future_obj})
        else:
            future_obj = runtime_task[0].get("task_runtime_future")
            if future_obj:
                future_obj.cancel()
                new_future_obj = asyncio.run_coroutine_threadsafe(start_exporter(account, self._loop), self._loop)
                # self._runtime_task_list = list(filter(lambda item: item.get("task_id") != task_id, self._runtime_task_list))
                self._runtime_task_list.append({"task_id": task_id, "task_runtime_future": new_future_obj})
        return True

    def get_chat_list(self):
        return []

    def get_chat_history(self, chat_id):
        return []

    def close(self):
        for task in asyncio.Task.all_tasks():
            task.cancel()
            with suppress(asyncio.CancelledError):
                self._loop.run_until_complete(task)
        self._loop.stop()
        self._loop.close()


def start_server(mq_host, local_queue="rpc_queue_mgr", remote_queue="rpc_queue_web"):

    mgr = Manager(mq_host=mq_host, queue=remote_queue)

    def server_callback(body):
        # json_obj = {
        #     "action": "add_task",
        #     "args": {
        #         "account": {
        #             "id": "1",
        #             "config": {},
        #         }
        #     },
        #     "message": "adding new task"
        # }
        # mgr.rcp_call(json.dumps(json_obj))
        try:
            json_obj = json.loads(body)
            action = json_obj.get("action")
            return_data = {"status": "error", "data": None, "msg": ""}

            if action == "add_task":
                if json_obj.get("args") and json_obj.get("args", {}).get("account", None):
                    return_data["data"] = mgr.add_task(json_obj.get("args", {}).get("account"))
                    return_data["status"] = "success"
                    return return_data
                else:
                    return_data["data"] = None
                    return_data["status"] = "error"
                    return_data["msg"] = "account args required!"
                    return return_data

            elif action == "set_task":
                if json_obj.get("args") and json_obj.get("args", {}).get("account", None):
                    return_data["data"] = mgr.set_task(json_obj.get("args", {}).get("account"))
                    return_data["status"] = "success"
                    return return_data
                else:
                    return_data["data"] = None
                    return_data["status"] = "error"
                    return_data["msg"] = "account args required!"
                    return return_data

            elif action == "get_task_list":
                return_data["data"] = mgr.get_task_list()
                return_data["status"] = "success"
                return return_data

            elif action == "get_chat_list":
                return_data["data"] = mgr.get_chat_list()
                return_data["status"] = "success"
                return return_data

            elif action == "get_chat_history":
                chat_id = json_obj.get("args", {}).get("chat_id", None)
                if chat_id:
                    return_data["data"] = mgr.get_chat_history(chat_id=chat_id)
                    return_data["status"] = "success"
                else:
                    return_data["data"] = None
                    return_data["status"] = "error"
                return return_data
            else:
                return_data["data"] = None
                return_data["status"] = "error"
                return_data["msg"] = "action not allowed!"
                return return_data

        except Exception as e:
            return_data["data"] = None
            return_data["status"] = "error"
            return_data["msg"] = str(e)
            return return_data

    try:
        mq_server = MqServer(mq_host, queue=local_queue, callback=server_callback)
        mq_server.start_rpc_consuming()
    except KeyboardInterrupt:
        pass
    finally:
        mgr.close()
        mq_server.close()


if __name__ == "__main__":
    # start_server("192.168.3.113")
    start_server("localhost")
