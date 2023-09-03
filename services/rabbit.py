import json
from os import environ
from typing import Union

from pika import adapters, PlainCredentials, ConnectionParameters, BlockingConnection


def converting_obj(obj: Union[list, dict]) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=4)


class RabbitHandler:
    __connection: BlockingConnection
    __channel: adapters.blocking_connection

    def __init__(self):
        RABBITMQ_DEFAULT_USER = environ.get('RABBITMQ_DEFAULT_USER')
        RABBITMQ_DEFAULT_PASS = environ.get('RABBITMQ_DEFAULT_PASS')

        credentials = PlainCredentials(RABBITMQ_DEFAULT_USER, RABBITMQ_DEFAULT_PASS)
        self.__connection = BlockingConnection(ConnectionParameters(
            host='localhost', port=5673, credentials=credentials))
        self.__channel = self.__connection.channel()

    def create_queue(self, name: str):
        self.__channel.queue_declare(queue=name, passive=True)

    def delete_queue(self, name: str):
        self.__channel.queue_delete(queue=name)

    def list_queue(self):
        queues = self.__channel.queue_declare(queue='', passive=True)
        return queues

    def send_message(self, msg: str, rk: str):
        self.__channel.basic_publish(exchange='', routing_key=rk, body=msg.encode("utf-8"))
