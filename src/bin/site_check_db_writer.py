#!/usr/bin/env python

import asyncio
import datetime
import ssl
import json
import argparse
import logging

import aiohttp
import asyncpg
from aiokafka import AIOKafkaConsumer
from aiokafka.helpers import create_ssl_context
import certifi
import yaml

from site_checker.log import LOG_LEVELS
from site_checker.error import ErrorStopMsgLimit


async def run_kafka_consumer(context):
    # TODO: move SQL code to dataclass in model
    insert_sql = """
        INSERT INTO 
            check_result
            (site_info_id, check_status, check_error_code, response_time, latency_time,
             http_status, regexp_check, check_timestamp)
        VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING 
            id
    """
    db_pool = context["db_pool"]
    kafka_consumer = context["kafka_consumer"]
    max_check_count = context["args"].max_check_count
    conf = context["conf"]
    while True:
        async with db_pool.acquire() as conn:
            data = await kafka_consumer.getmany(max_records=100)
            for part_id, messages in data.items():
                for raw_msg in messages:
                    logging.debug("received raw_msg {}".format(raw_msg))
                    try:
                        msg = json.loads(raw_msg.value)
                    except ValueError as e:
                        logging.info(
                            f"Unable to unpack msg from Kafka {raw_msg} as JSON, error: {e}. Skip it"
                        )
                        continue

                    try:
                        check_timestamp_as_dt = datetime.datetime.fromisoformat(
                            msg["check_timestamp"]
                        )
                    except ValueError as e:
                        msg = "Unable to parse check_timestamp for msg {} , error: {}. Skip it"
                        logging.info(msg.format(msg, e))
                        continue

                    logging.info("about to insert msg {}".format(msg))
                    res = await conn.fetch(
                        insert_sql,
                        msg["site_info_id"],
                        msg["check_status"],
                        msg["check_error_code"],
                        msg["response_time"],
                        msg["latency_time"],
                        msg["http_status"],
                        msg["regexp_check"],
                        check_timestamp_as_dt,
                    )
                    context["check_count"] += 1
                    if max_check_count and context["check_count"] >= max_check_count:
                        raise ErrorStopMsgLimit
                    logging.info(
                        "Write to DB msg for site={} id={}".format(msg["site_info_id"], res[0][0])
                    )

        await asyncio.sleep(conf["site_check_db_writer"]["check_interval"])


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    default_conf = "site_checker.yaml"
    parser.add_argument(
        "--config-path",
        "-c",
        default=default_conf,
        help="specify path to site checker YAML config",
    )
    parser.add_argument(
        "--log-level",
        "-l",
        default="info",
        choices=("info", "debug", "error", "warning"),
    )
    parser.add_argument(
        "--max-check-count",
        type=int,
        default=None,
        help="specify number of checks to read from kafka before exit(for tests)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=LOG_LEVELS[args.log_level],
        format="%(levelname)s, %(asctime)s, %(filename)s +%(lineno)s, %(message)s",
    )

    conf = yaml.safe_load(open(args.config_path))

    db_conf = {
        "database": conf["database"]["name"],
        "host": conf["database"]["host"],
        "port": conf["database"]["port"],
        "user": conf["database"]["user"],
        "password": conf["database"]["password"],
    }
    if conf["database"]["ssl"]:
        db_ssl_ctx = ssl.create_default_context(
            cafile=conf["database"]["cafile"], capath=certifi.where()
        )
        db_conf["ssl"] = db_ssl_ctx

    db_pool = await asyncpg.create_pool(
        min_size=conf["database"]["pool_min_size"],
        max_size=conf["database"]["pool_max_size"],
        **db_conf,
    )

    kafka_server = "{}:{}".format(conf["kafka"]["host"], conf["kafka"]["port"])
    kafka_ssl_context = create_ssl_context(
        cafile=conf["kafka"]["cafile"],  # CA used to sign certificate.
        # `CARoot` of JKS store container
        certfile=conf["kafka"]["certfile"],  # Signed certificate
        keyfile=conf["kafka"]["keyfile"],  # Private Key file of `certfile` certificate
    )

    kafka_consumer = AIOKafkaConsumer(
        conf["kafka"]["topic"],
        bootstrap_servers=kafka_server,
        security_protocol="SSL",
        ssl_context=kafka_ssl_context,
    )
    await kafka_consumer.start()

    context = {
        "args": args,
        "conf": conf,
        "db_pool": db_pool,
        "kafka_consumer": kafka_consumer,
        "check_count": 0,
    }
    try:
        await run_kafka_consumer(context)
    except ErrorStopMsgLimit:
        logging.info("Limit of messages is reached. Stop.")
    finally:
        await kafka_consumer.stop()
        db_pool.terminate()


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logging.info("Received CTRL+C signal, exiting")
