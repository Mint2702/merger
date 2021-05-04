import pika
import sys
from functools import wraps


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


@connect
def send_message():
    channel.queue_declare(queue="hello", durable=True)

    message = " ".join(sys.argv[1:]) or "Hello World!"

    channel.basic_publish(
        exchange="",
        routing_key="hello",
        body=message,
        properties=pika.BasicProperties(delivery_mode=2),
    )
    print(f" [x] Sent '{message}'")


send_message()