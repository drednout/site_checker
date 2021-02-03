import logging
import sys
import os
import asyncio
import argparse
import ssl
import subprocess
import datetime

import yaml
import certifi
import asyncpg


LOG_LEVELS = {
    "info": logging.INFO,
    "debug": logging.DEBUG,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


async def prepare_tests(context):
    db_pool = context["db_pool"]
    conf = context["conf"]

    # drop tables in database
    logging.info("Drop all tables in database")
    async with db_pool.acquire() as conn:
        await conn.fetch("DROP OWNED BY {}".format(conf["database"]["user"]))

    # run pg_migrate and apply migrations
    logging.info("Apply fresh DB migration via pgmigrate")
    ret = os.system("pgmigrate -d site_checker_db  -t 1 migrate")
    if ret != 0:
        logging.error("Unable to apply DB migrations.")
        sys.exit(1)

    # todo: create or clean kafka topic automatically


async def get_check_results(db_pool, check_site_id):
    sql = """
        SELECT 
            id, check_status, check_error_code, latency_time, response_time, http_status, 
            check_timestamp, regexp_check
        FROM 
            check_result
        WHERE
            site_info_id=$1
        ORDER BY 
            id
    """
    check_results = []
    async with db_pool.acquire() as conn:
        raw_check_results = await conn.fetch(sql, check_site_id)
        for row in raw_check_results:
            res = {
                "id": row[0],
                "check_status": row[1],
                "check_error_code": row[2],
                "latency_time": row[3],
                "response_time": row[4],
                "http_status": row[5],
                "check_timestamp": row[6],
                "regexp_check": row[7],
            }
            check_results.append(res)

    return check_results


def print_msg(msg):
    sys.stdout.write(msg)


def fuzzy_datetime_compare(dt1, dt2, dt=datetime.timedelta(seconds=10)):
    if dt2 - dt <= dt1 <= dt2 + dt:
        return True
    else:
        return False


async def test_positive_5_checks(context):
    args = context["args"]
    print_msg("Running test_positive_5_checks ... ")
    check_site_id = 2
    db_writer = subprocess.Popen(
        "python site_check_db_writer.py  --max-check-count 5 -c {} -l error".format(args.config_path), shell=True
    )
    # wait some time is needed, because site_check_db_writer need some tome for initializing SSL context
    # for Kafka/PostgreSQL and other stuff
    await asyncio.sleep(2)
    site_checker = subprocess.Popen(
        "python site_checker.py --check-site-id {} --max-check-count 5 -c {} -l error".format(
            check_site_id, args.config_path),
        shell=True,
    )
    for child in [db_writer, site_checker]:
        child.wait(timeout=10)

    check_results = await get_check_results(db_pool=context["db_pool"], check_site_id=check_site_id)
    assert len(check_results) == 5, "We should have exactly 5 checks in DB"

    current_timestamp = datetime.datetime.now()
    for res in check_results:
        assert res["http_status"] == 200, "We should have 200 http in response"
        assert res["check_status"] == "ok", "Check should be ok"
        assert res["regexp_check"] is True, "Regexp check should be true"
        assert res["check_error_code"] is None, "Some error in ok check"
        assert res["response_time"] >= res["latency_time"], "Response time should be greater than latency"
        assert fuzzy_datetime_compare(res["check_timestamp"], current_timestamp), "Checks are too old"

    print_msg("OK\n")


async def test_not_found_5_checks(context):
    args = context["args"]
    print_msg("Running test_not_found_5_checks ... ")
    check_site_id = 3
    db_writer = subprocess.Popen(
        "python site_check_db_writer.py  --max-check-count 5 -c {} -l error".format(args.config_path), shell=True
    )
    # wait some time is needed, because site_check_db_writer need some tome for initializing SSL context
    # for Kafka/PostgreSQL and other stuff
    await asyncio.sleep(2)
    site_checker = subprocess.Popen(
        "python site_checker.py --check-site-id {} --max-check-count 5 -c {} -l error".format(
            check_site_id, args.config_path),
        shell=True,
    )
    for child in [db_writer, site_checker]:
        child.wait(timeout=10)

    check_results = await get_check_results(db_pool=context["db_pool"], check_site_id=check_site_id)
    assert len(check_results) == 5, "We should have exactly 5 checks in DB"

    current_timestamp = datetime.datetime.now()
    for res in check_results:
        assert res["http_status"] == 404, "We should have 404 http in response"
        assert res["check_status"] == "ok", "Check should be ok"
        assert res["regexp_check"] is None, "Regexp check should be true"
        assert res["check_error_code"] is None, "Some error in ok check"
        assert res["response_time"] >= res["latency_time"], "Response time should be greater than latency"
        assert fuzzy_datetime_compare(res["check_timestamp"], current_timestamp), "Checks are too old"

    print_msg("OK\n")


async def test_negative_http_client(context):
    args = context["args"]
    print_msg("Running test_negative_http_client ... ")
    check_site_id = 4
    db_writer = subprocess.Popen(
        "python site_check_db_writer.py  --max-check-count 1 -c {} -l error".format(args.config_path), shell=True
    )
    # wait some time is needed, because site_check_db_writer need some tome for initializing SSL context
    # for Kafka/PostgreSQL and other stuff
    await asyncio.sleep(2)
    site_checker = subprocess.Popen(
        "python site_checker.py --check-site-id {} --max-check-count 1 -c {} -l error".format(
            check_site_id, args.config_path),
        shell=True,
    )
    for child in [db_writer, site_checker]:
        child.wait(timeout=10)

    check_results = await get_check_results(db_pool=context["db_pool"], check_site_id=check_site_id)
    assert len(check_results) == 1, "We should have exactly 1 check in DB"

    current_timestamp = datetime.datetime.now()
    for res in check_results:
        assert res["http_status"] is None, "We should not have http status in response"
        assert res["check_status"] == "error", "Check should be not OK"
        assert res["regexp_check"] is None, "Regexp check should be None"
        assert res["check_error_code"] == "http_client", "Error should be http_client"
        assert fuzzy_datetime_compare(res["check_timestamp"], current_timestamp), "Checks are too old"

    print_msg("OK\n")


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

    context = {"db_pool": db_pool, "conf": conf, "args": args}

    # prepare tests
    await prepare_tests(context)

    # run tests
    tests = [
        test_positive_5_checks,
        test_not_found_5_checks,
        test_negative_http_client
    ]
    for test in tests:
        await test(context)


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logging.info("Received CTRL+C signal, exiting")
