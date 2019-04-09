#!/usr/bin/env python
""" rabitmq server """

import pika
import uuid


class MqClient(object):

    def __init__(self, host="192.168.3.113", queue="rpc_queue_web", queue_send="queue_web"):
        self._queue = queue
        self._queue_send = queue_send
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host))

        self.channel = self.connection.channel()
        self.channel_send = self.connection.channel()

        result = self.channel.queue_declare('', exclusive=True)
        self.callback_queue = result.method.queue
        print(result)

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def send(self, message):
        self.channel_send.basic_publish(exchange='',
                                        routing_key=self._queue_send,
                                        body=message)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, json_str):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key=self._queue,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=json_str)
        while self.response is None:
            self.connection.process_data_events()
        return self.response


if __name__ == "__main__":
    import json
    # mq_client = MqClient(host="192.168.3.113", queue="rpc_queue_mgr")
    mq_client = MqClient(host="localhost", queue="rpc_queue_mgr")
    body = {
        "action": "set_task",
        "args": {
            "account": {
                "id": "1",
                "config": {
                    "TelegramAPI": {
                        "ApiId": 492396,
                        "ApiHash": "xxx",
                        "PhoneNumber": "+8520000",
                        "SecondFactorPassword": "",
                        "SessionName": "export",
                    },
                    "Dumper": {
                        "OutputDirectory": ".",
                        'MediaWhitelist': 'chatphoto, photo, sticker, document, video, audio, voice',
                        'MaxSize': 2 * 1024 ** 2,
                        'LogLevel': 'INFO',
                        'DBFileName': 'export_8520000',
                        'InvalidationTime': 7200 * 60,
                        'ChunkSize': 100,
                        'MaxChunks': 0,
                        'LibraryLogLevel': 'WARNING',
                        'MediaFilenameFmt': 'media-8520000/{name}-{context_id}/{type}-{filename}',
                        # 'Whitelist': '-390498386',
                        # 'Blacklist': 'more t',
                        'Proxy': 'http://127.0.0.1:1080',
                    }
                }
            }
        }
    }
    response = mq_client.call(json.dumps(body))
    print(" [.] Got %r" % response)
    body = {
        "action": "get_task_list",
        "args": {
        }
    }
    response = mq_client.call(json.dumps(body))
    print(" [.] Got %r" % response)
