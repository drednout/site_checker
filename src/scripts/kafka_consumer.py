from kafka import KafkaConsumer

consumer = KafkaConsumer(
    # "demo-topic",
    "site-checker",
    auto_offset_reset="earliest",
    bootstrap_servers="kafka-aiven-site-checker-drednout-7f62.aivencloud.com:14798",
    client_id="demo-client-1",
    group_id="demo-group",
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
)


for _ in range(2):
    raw_msgs = consumer.poll(timeout_ms=1000)
    for tp, msgs in raw_msgs.items():
        for msg in msgs:
            print("Received: {}".format(msg.value))

consumer.commit()
