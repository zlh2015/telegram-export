#!/usr/bin/env python
""" rabitmq server """

import pika


class MqServer(object):

    def __init__(self, host="192.168.3.113", queue="rpc_queue", callback=None):
        self._host = host
        self._queue = queue
        self.connection = None
        self._callback = callback
        self.init_conn()

    def init_conn(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host))

    def start_rpc_consuming(self):
        channel = self.connection.channel()
        channel.queue_declare(queue=self._queue)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=self._queue, on_message_callback=self.on_request)
        print(" [x] Awaiting RPC requests")
        channel.start_consuming()

    def on_request(self, ch, method, props, body):
        print(" [x] got request body{}".format(body))
        if self._callback:
            response = self._callback(body)
        else:
            response = "true"
        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id=props.correlation_id),
                         body=str(response))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def close(self):
        self.connection.close()


if __name__ == "__main__":
    mq_server = MqServer()
    mq_server.start_rpc_consuming()


