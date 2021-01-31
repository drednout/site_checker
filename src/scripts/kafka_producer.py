from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="kafka-aiven-site-checker-drednout-7f62.aivencloud.com:14798",
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
)

for i in range(1, 4):
    message = "message number {}".format(i)
    print("Sending: {}".format(message))
    producer.send("demo-topic", message.encode("utf-8"))

# Force sending of all messages

producer.flush()
