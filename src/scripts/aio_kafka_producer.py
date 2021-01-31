import asyncio

from aiokafka import AIOKafkaProducer
from aiokafka.helpers import create_ssl_context

context = create_ssl_context(
    cafile="ca.pem",  # CA used to sign certificate.
    # `CARoot` of JKS store container
    certfile="service.cert",  # Signed certificate
    keyfile="service.key",  # Private Key file of `certfile` certificate
)
server = "kafka-aiven-site-checker-drednout-7f62.aivencloud.com:14798"


async def produce_msg(loop):
    # Produce
    producer = AIOKafkaProducer(
        loop=loop, bootstrap_servers=server, security_protocol="SSL", ssl_context=context
    )

    await producer.start()
    try:
        msg = await producer.send_and_wait("demo-topic", b"Super Message", partition=0)
    finally:
        await producer.stop()

    return msg


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    task = loop.create_task(produce_msg(loop))
    try:
        res_msg = loop.run_until_complete(task)
        print("msg is {}".format(res_msg))
    finally:
        loop.run_until_complete(asyncio.sleep(0, loop=loop))
        task.cancel()
        try:
            loop.run_until_complete(task)
        except asyncio.CancelledError:
            pass
