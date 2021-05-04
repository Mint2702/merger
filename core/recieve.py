import pika
from functools import wraps
import time


def connect(func):
    @wraps(func)
    def wrapper(*args, **kwargs):

        global channel

        credentials = pika.PlainCredentials("danil", "passw0rd")
        parameters = pika.ConnectionParameters("host1.miem.vmnet.top", 5672, "/", credentials)

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        result = func(*args, **kwargs)

        connection.close()

        return result

    return wrapper


def callback(ch, method, properties, body: str):
    body = str(body)
    print(f" [x] Received {body}")
    time.sleep(int(body.count(".")))
    print(" [x] Done")
    channel.basic_ack(delivery_tag=method.delivery_tag)


@connect
def recieve():
    channel.queue_declare(queue="hello", durable=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="hello", on_message_callback=callback)
    print(" [*] Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()


recieve()
