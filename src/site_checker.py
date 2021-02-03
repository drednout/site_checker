import asyncio
import datetime
import ssl
import json
import argparse
import logging
import re

import aiohttp
import aiohttp.client_exceptions
import asyncpg
from aiokafka import AIOKafkaProducer
from aiokafka.helpers import create_ssl_context
import certifi
import yaml


LOG_LEVELS = {
    "info": logging.INFO,
    "debug": logging.DEBUG,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


class ErrorStopMsgLimit(Exception):
    pass


async def send_msg(context, topic, msg, partition=0):
    producer = context["kafka_producer"]
    await producer.send_and_wait(topic, msg, partition=partition)


class CheckStats:
    def __init__(self, total_limit):
        self.ok = 0
        self.error = 0
        self.total = 0
        self.total_limit = total_limit

    def incr_ok(self, ok_count=1):
        self.ok += ok_count
        self.total += ok_count

    def incr_error(self, error_count=1):
        self.error += error_count
        self.total += error_count

    def check_for_limit(self):
        if self.total_limit and self.total >= self.total_limit:
            raise ErrorStopMsgLimit


class CheckResult:
    OK = "ok"
    ERROR = "error"

    ERROR_CODE_TIMEOUT = "timeout"
    ERROR_CODE_HTTP_CLIENT = "http_client"
    ERROR_CODE_EXCEPTION = "exception"

    def __init__(
        self,
        site_info_id,
        check_status,
        check_error_code=None,
        response_time=None,
        latency_time=None,
        regexp_check=None,
        http_status=None,
        check_timestamp=None,
    ):
        self.site_info_id = site_info_id
        self.check_status = check_status
        self.check_error_code = check_error_code
        self.response_time = response_time
        self.latency_time = latency_time
        self.regexp_check = regexp_check
        self.http_status = http_status
        if check_timestamp is None:
            self.check_timestamp = datetime.datetime.now()

    def as_dict(self):
        msg = {
            "site_info_id": self.site_info_id,
            "check_status": self.check_status,
            "check_error_code": self.check_error_code,
            "response_time": self.response_time,
            "latency_time": self.latency_time,
            "regexp_check": self.regexp_check,
            "http_status": self.http_status,
            "check_timestamp": self.check_timestamp,
        }
        return msg

    def as_json(self, encoding="utf8"):
        msg = self.as_dict()
        msg["check_timestamp"] = msg["check_timestamp"].isoformat()
        return json.dumps(msg).encode(encoding)


async def do_request(context, session, site_info):
    event_loop = context["event_loop"]
    start_time = event_loop.time()
    async with session.get(site_info["url"]) as response:
        latency_time = event_loop.time() - start_time
        logging.info("Check with id={} has status={}".format(site_info["id"], response.status))
        text = await response.text()
        regexp_check = None
        if site_info["regexp"]:
            if site_info["regexp"].search(text):
                regexp_check = True
            else:
                regexp_check = False
        response_time = event_loop.time() - start_time
        check_result = CheckResult(
            site_info_id=site_info["id"],
            check_status="ok",
            response_time=response_time,
            latency_time=latency_time,
            regexp_check=regexp_check,
            http_status=response.status,
        )
        return check_result


async def do_site_check(context, site_info):
    conf = context["conf"]

    timeout = aiohttp.ClientTimeout(total=conf["site_checker"]["http_timeout"])
    while True:
        logging.info(
            "Run check with id={}, name=`{}`".format(site_info["id"], site_info["site_name"])
        )

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                check_result = await do_request(context, session, site_info)
                context["check_stats"].incr_ok()
        except aiohttp.client_exceptions.ClientError as e:
            logging.warning(
                "Check with id={}, name=`{}` failed due to aiohttp error {}".format(
                    site_info["id"], site_info["site_name"], e
                )
            )
            check_result = CheckResult(
                site_info_id=site_info["id"],
                check_status="error",
                check_error_code=CheckResult.ERROR_CODE_HTTP_CLIENT,
            )
            context["check_stats"].incr_error()

        except asyncio.TimeoutError:
            logging.warning(
                "Check with id={}, name=`{}` failed due to asyncio timeout".format(
                    site_info["id"], site_info["site_name"]
                )
            )
            check_result = CheckResult(
                site_info_id=site_info["id"],
                check_status="error",
                check_error_code=CheckResult.ERROR_CODE_TIMEOUT,
            )
            context["check_stats"].incr_error()

        await send_msg(context, conf["kafka"]["topic"], check_result.as_json())
        context["check_stats"].check_for_limit()

        await asyncio.sleep(conf["site_checker"]["check_interval"])


def load_regexp(site_id, regexp):
    compiled_regexp = None
    try:
        compiled_regexp = re.compile(regexp)
    except re.error as e:
        logging.error(f"Unable to compile regexp for site_id={site_id}, reason={e}")

    return compiled_regexp


async def run_checks(context):
    db_pool = context["db_pool"]
    conf = context["conf"]
    args = context["args"]
    active_checks = {}
    last_timestamp = datetime.datetime.min
    stop_checks = False

    sql = """
        SELECT 
            id, updated, site_name, url, data, http_method, regexp 
        FROM 
            check_site_info
        WHERE
            updated > $1
    """
    if args.check_site_id:
        sql += " AND id=$2"

    sql += """
        ORDER BY 
            updated
    """

    def task_finish_callback(finished_task):
        logging.info(
            "Task {} was finished with result={}".format(finished_task.get_name(), finished_task)
        )
        site_info_id = int(finished_task.get_name())
        check_result = CheckResult(
            site_info_id=site_info_id,
            check_status=CheckResult.ERROR,
            check_error_code=CheckResult.ERROR_CODE_EXCEPTION,
        )
        try:
            send_msg(context, conf["kafka"]["topic"], check_result.as_json())
            context["check_stats"].incr_error()
            # raise exception, if it's in result
            finished_task.result()
        except ErrorStopMsgLimit:
            logging.info("Limit of messages is reached. Stop.")
            nonlocal stop_checks
            stop_checks = True
        except asyncio.exceptions.CancelledError:
            # ok, we did it when updating the task
            pass

    while True:
        async with db_pool.acquire() as conn:
            sql_args = [last_timestamp]
            if args.check_site_id:
                sql_args.append(args.check_site_id)
            site_check_info = await conn.fetch(sql, *sql_args)
            logging.debug("site_check_info is {}".format(site_check_info))
            for db_site_info in site_check_info:
                site_id = db_site_info[0]
                updated = db_site_info[1]
                site_name = db_site_info[2]
                url = db_site_info[3]
                data = db_site_info[4]
                http_method = db_site_info[5]
                regexp = db_site_info[6]
                site_info = {
                    "id": site_id,
                    "updated": updated,
                    "site_name": site_name,
                    "url": url,
                    "data": data,
                    "http_method": http_method,
                    "regexp": regexp,
                }
                if regexp is not None:
                    compiled_regexp = load_regexp(site_id, regexp)
                    site_info["regexp"] = compiled_regexp

                if site_id in active_checks:
                    logging.info(f"Cancel old check with id={site_id}")
                    # cancel check, run updated
                    old_task = active_checks.pop(site_id)
                    old_task.cancel()

                    logging.info(f"Schedule updated check {site_info}")
                    task = context["event_loop"].create_task(
                        do_site_check(context, site_info), name=site_id
                    )
                    task.add_done_callback(task_finish_callback)
                    active_checks[site_id] = task
                    pass
                else:
                    logging.info(f"Schedule new check {site_info}")
                    # run new check
                    task = context["event_loop"].create_task(
                        do_site_check(context, site_info), name=site_id
                    )
                    task.add_done_callback(task_finish_callback)
                    active_checks[site_id] = task

                last_timestamp = updated

        logging.debug("active_checks are {}".format(active_checks))
        await asyncio.sleep(conf["site_checker"]["check_interval"])
        if stop_checks:
            for name, task in active_checks.items():
                if not task.done():
                    logging.info(f"Cancel active task {name}")
                    task.cancel()
            break


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
        "--check-site-id",
        type=int,
        default=None,
        help="specify check_site_info.id for check(for tests)",
    )
    parser.add_argument(
        "--max-check-count",
        type=int,
        default=None,
        help="specify maximium number of checks to run(for tests)",
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

    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=kafka_server,
        security_protocol="SSL",
        ssl_context=kafka_ssl_context,
    )
    await kafka_producer.start()

    context = {
        "args": args,
        "conf": conf,
        "db_pool": db_pool,
        "kafka_producer": kafka_producer,
        "event_loop": asyncio.get_event_loop(),
        "check_stats": CheckStats(total_limit=args.max_check_count),
    }
    try:
        await run_checks(context)
    finally:
        await kafka_producer.stop()
        db_pool.terminate()


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logging.info("Received CTRL+C signal, exiting")
