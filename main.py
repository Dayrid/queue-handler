import json

from services.rabbit import RabbitHandler


class QueueHandler(RabbitHandler):
    def __init__(self):
        super().__init__()

    @staticmethod
    def callback(ch, method, properties, body: bytes):
        obj = json.loads(body.decode("utf-8"))
        print(obj)

    def handler_init(self, queue):
        self.__channel.basic_consume(queue=queue, on_message_callback=self.callback, auto_ack=True)


if __name__ == '__main__':
    qh = QueueHandler()
    qh.handler_init('suppliers')
