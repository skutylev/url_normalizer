from dataclasses import dataclass
from typing import List, Tuple, Any
from datetime import datetime
from config import connection_string, logger
from urllib.parse import urlparse
import re
import pyodbc


@dataclass
class QuerySidDto:
    company_id: int
    full_address_url: str
    website_url: str
    refreshed_website_url: str


@dataclass
class QueryCwdDto(QuerySidDto):
    company_name: str
    session_id: str
    page_count: int


""" Dictionary containing SQL queries statements"""
queries = dict(
    get_vendors=r"SELECT vendor_id FROM minionscraper_task_vendors WHERE task_id='%s';",
    get_countries=r"SELECT country_id FROM minionscraper_task_countries WHERE task_id='%s';",
    get_in_progress_tasks=r"SELECT * FROM minionscraper_task WHERE minionscraper_task.status=2 ORDER BY id ASC;",
    get_task_by_id=r"SELECT * FROM minionscraper_task WHERE minionscraper_task.id='%s';",
    get_tasks=r"SELECT {args} FROM minionscraper_task WHERE status IN ({statuses}) ORDER BY id ASC;",
    set_task_status=r"UPDATE minionscraper_task SET status='%s' WHERE id='%s'",
    set_task_status_started=r"UPDATE minionscraper_task SET status='%s', started_at='%s' WHERE id='%s'",
    set_task_status_finished=r"UPDATE minionscraper_task SET status='%s', finished_at='%s' WHERE id='%s'",
    update_refreshed_website1=r"UPDATE {table_name} SET HasHomepageText='%s' WHERE id='%s'",
    update_has_homepage_text=r"UPDATE {table_name} SET HasHomepageText='%s' WHERE id='%s'",
    get_sid_cmp=r"SELECT DISTINCT FullAddress, Website1, RefreshedWebsite1, CompanyId FROM ScrapingInputDataCmp WHERE ProjectId = {0} AND CountryCode IN {1} AND VendorId IN {2}",
    get_sid_cmd=r"SELECT DISTINCT FullAddress, Website1, RefreshedWebsite1, CompanyId FROM ScrapingInputDataCmd WHERE ProjectId = {0} AND CountryCode IN {1}",
    )


def execute_sql_query_and_fetch_all(query: str) -> List[object]:
    """Method executes sql query with select statement and returns list of rows"""
    with pyodbc.connect(connection_string) as connection:
        with connection.cursor() as cursor:
            logger.info(query)
            cursor.execute(query)
            result = cursor.fetchall()
            return result


def execute_sql_query_and_fetch_one(query: str) -> object:
    """Method executes sql query with select statement and returns one row"""
    with pyodbc.connect(connection_string) as connection:
        with connection.cursor() as cursor:
            logger.info(query)
            cursor.execute(query)
            result = cursor.fetchone()
            return result


def execute_sql_query_and_write(query: str) -> None:
    """Method executes sql query with update statement and commits changes in database"""
    with pyodbc.connect(connection_string) as connection:
        with connection.cursor() as cursor:
            logger.info(query)
            cursor.execute(query)
    return None


def get_task(to_run: bool = False) -> object:
    query = queries["get_tasks"].format("TOP 2 *", "1, 2, 4, 8")
    tasks = execute_sql_query_and_fetch_all(query)
    for task in tasks:
        if to_run:
            query_in_progress = queries["get_tasks"].format("TOP 1 *", "2, 4")
            tasks_in_progress = execute_sql_query_and_fetch_all(query_in_progress)
            if task.status == 1 or task.status == 8 and not tasks_in_progress:
                return task
        else:
            if task.status == 4:
                return task
    return None


def _get_vendors(task_id: int) -> Tuple[Any]:
    query = queries["get_vendors"].format(task_id)
    rows = execute_sql_query_and_fetch_all(query)
    result = []
    for row in rows:
        result.append(row.vendor_id)
    return tuple(result)


def _get_counties(task_id: int) -> Tuple[Any]:
    query = queries["get_countries"].format(task_id)
    rows = execute_sql_query_and_fetch_all(query)
    result = []
    for row in rows:
        result.append(row.country_id)
    return tuple(result)


def get_in_progress_tasks() -> List[object]:
    query = queries["get_in_progress_tasks"]
    rows = execute_sql_query_and_fetch_all(query)
    return rows


def get_task_by_id(task_id: int) -> object:
    query = queries["get_task_by_id"].format(task_id)
    row = execute_sql_query_and_fetch_one(query)
    return row


def set_task_status(task_id: int, status: int) -> None:
    now = datetime.now()
    if status == 2:
        query = queries["set_task_status_started"].format(status, now, task_id)
    elif status == 3:
        query = queries["set_task_status_finished"].format(status, now, task_id)
    else:
        query = queries["set_task_status"].format(status, task_id)
    execute_sql_query_and_write(query)
    return None


def update_refreshed_website1(company_id: int, original_url: str, master_data_type: str) -> None:
    if master_data_type == "CMP":
        query = queries["update_refreshed_website1"].format("bcpitCmpData", original_url, company_id)
    else:
        query = queries["update_refreshed_website1"].format("domain_record", original_url, company_id)
    execute_sql_query_and_write(query)
    return None


def update_has_homepage_text(company_id: int, has_homepage_text: int, master_data_type: str) -> None:
    if master_data_type == "CMP":
        query = queries["update_has_homepage_text"].format("bcpitCmpData", has_homepage_text, company_id)
    else:
        query = queries["update_has_homepage_text"].format("domain_record", has_homepage_text, company_id)
    execute_sql_query_and_write(query)
    return None


def get_urls(task_id: int, master_data_type: str, scraping_policy: str, less_pages) -> object:
    vendor_ids = _get_vendors(task_id)
    countries_codes = _get_counties(task_id)
    project_id = get_task_by_id(task_id).project_id.replace('-', '')
    sids = _get_sids(master_data_type, project_id, countries_codes, vendor_ids)
    for i in sids:
        urls = _normalize_urls(i.website_url, i.refreshed_website_url, i.full_address_url)


def _get_sids(master_data_type: str, project_id: int, countries_codes: Tuple[Any], vendor_ids: Tuple[Any]) -> List[QuerySidDto]:
    if master_data_type == "CMP":
        query = queries["get_sid_cmp"].format(project_id, countries_codes, vendor_ids)
    elif master_data_type == "CMD":
        query = queries["get_sid_cmd"].format(project_id, countries_codes)
    else:
        raise Exception(f"Master data type - {master_data_type} is not implemented")
    rows = execute_sql_query_and_fetch_all(query)
    return [QuerySidDto(company_id=row.CompanyId,
                        full_address_url=row.FullAddress,
                        website_url=row.Website1,
                        refreshed_website_url=row.RefreshedWebsite1) for row in rows]


def _get_cwds(master_data_type: str) -> List[QueryCwdDto]:
    if master_data_type == "CMP":
        query = queries["r"].format()
    elif master_data_type == "CMD":
        query = queries["r"].format()
    else:
        raise Exception(f"Master data type - {master_data_type} is not implemented")
    rows = execute_sql_query_and_fetch_all(query)
    return [QueryCwdDto(company_id=row.CompanyId) for row in rows]


def _normalize_urls(*urls_data: str) -> List[str]:
    normalized_urls = set()

    for url in urls_data:
        domain_names = []

        netloc = re.sub(r'^www\d*\.', '', urlparse(url).netloc)

        if not netloc:
            continue

        domain_names.append(netloc)
        domain_names.append(f"www.{netloc}")

        for domain_name in domain_names:
            normalized_urls.add(f"http://{domain_name}")
            normalized_urls.add(f"https://{domain_name}")

    return list(normalized_urls)