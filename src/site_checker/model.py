import re
import datetime
import json

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


def load_regexp(site_id, regexp):
    compiled_regexp = None
    try:
        compiled_regexp = re.compile(regexp)
    except re.error as e:
        logging.error(f"Unable to compile regexp for site_id={site_id}, reason={e}")

    return compiled_regexp

async def get_site_check_by_id(db_pool, id):
    sql = """
        SELECT
            id, updated, site_name, url, data, http_method, regexp
        FROM
            check_site_info
        WHERE
            id=$1
    """
    async with db_pool.acquire() as conn:
        site_check_info = await conn.fetch(sql, id)
        if len(site_check_info) == 0:
            return None
        db_site_info = site_check_info[0] # id is PK
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

    return site_info


async def get_last_site_checks(db_pool, updated):
    sql = """
        SELECT
            id, updated, site_name, url, data, http_method, regexp
        FROM
            check_site_info
        WHERE
            updated > $1
    """
    check_list = []
    async with db_pool.acquire() as conn:
        site_check_info = await conn.fetch(sql, updated)
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
            check_list.append(site_info)

    return check_list
