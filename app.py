#!/usr/bin/env python3
# thoth-init-job
# Copyright(C) 2018, 2019 Fridolin Pokorny
#
# This program is free software: you can redistribute it and / or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""Initialize a fresh Thoth deployment."""

import logging
from typing import List
from typing import Generator
from urllib.parse import urljoin

import requests
import click
from bs4 import BeautifulSoup

from thoth.common import init_logging
from thoth.common import OpenShift
from thoth.python import Source
from thoth.storages import GraphDatabase

init_logging()

_LOGGER = logging.getLogger("thoth.init_job")
_DEFAULT_INDEX_BASE_URL = "https://tensorflow.pypi.thoth-station.ninja/index"
_PYPI_SIMPLE_API_URL = "https://pypi.org/simple"


def _html_parse_listing(url: str) -> Generator[str, None, None]:
    """Parse listing of a HTML document."""
    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")
    for row in soup.find("table").find_all("tr"):
        for cell in row.find_all("td"):
            if cell.a:
                if cell.a.text == "Parent Directory":
                    continue

                if not cell.a.text.endswith("/"):
                    yield cell.a.text + "/"

                yield cell.a.text


def _get_build_configuration(url: str) -> List[str]:
    """Get available configuration for a distro."""
    result = []
    for configuration in _html_parse_listing(url):
        sub_url = urljoin(url, configuration)
        present_subdirs = set(_html_parse_listing(sub_url))

        if present_subdirs - {"simple/"}:
            _LOGGER.error(
                "Additional sub-directories not compliant with PEP-503 found (expected only 'simple/'): %r",
                present_subdirs - {"simple/"}
            )

        if "simple/" not in present_subdirs:
            _LOGGER.error(
                "No 'simple/' found on URL %r, cannot treat as PEP-503 compliant simple API",
                url,
            )
            continue

        _LOGGER.info("Detected build configuration %r at %s", configuration[:-1], url)
        result.append(urljoin(url, configuration))

    return result


def _get_os_releases(url: str) -> List[str]:
    """Parse operating systems and their versions available on AICoE indexes."""
    result = []

    os_names = list(_html_parse_listing(url))
    if not os_names:
        _LOGGER.warning("No operating systems detected at %r", url)

    for os_name in os_names:
        _LOGGER.info("Found operating system %r", os_name[:-1])
        os_url = urljoin(url, os_name)
        os_versions = list(_html_parse_listing(os_url))
        if not os_versions:
            _LOGGER.warning("No versions of operating system %r detected at %r", os_name[:-1], url)

        for os_version in os_versions:
            _LOGGER.info("Found operating system %r in version %r", os_name[:-1], os_version[:-1])
            result.extend(_get_build_configuration(urljoin(os_url, os_version)))

    return result


def _list_available_indexes(url: str) -> List[str]:
    """List available indexes on AICoE index."""
    result = []
    _LOGGER.info("Listing available indexes on AICoE index %r", url)

    indexes = list(_html_parse_listing(url))
    if not indexes:
        _LOGGER.error("No AICoE indexes found at %r", url)

    for item in indexes:
        if item.lower() == "os/":
            _LOGGER.info("Discovering available operating systems at %r", url)
            result.extend(_get_os_releases(urljoin(url, item)))
        else:
            _LOGGER.info("Discovering compliant releases at %r", url)
            result.extend(_get_build_configuration(urljoin(url, item)))

    return result


def _register_indexes(graph: GraphDatabase, index_base_url: str, dry_run: bool = False) -> List[str]:
    """Register available AICoE indexes into Thoth's database."""
    _LOGGER.info("Registering PyPI index %r", _PYPI_SIMPLE_API_URL)
    index_urls = [_PYPI_SIMPLE_API_URL]

    if not dry_run:
        _LOGGER.info("Registering index %r", index_urls[0])

        graph.register_python_package_index(
            index_urls[0],
            warehouse_api_url=_PYPI_SIMPLE_API_URL,
            verify_ssl=True,
            enabled=True,
        )

    aicoe_indexes = _list_available_indexes(index_base_url)
    if not aicoe_indexes:
        _LOGGER.error("No AICoE indexes to register")

    for index_url in aicoe_indexes:
        _LOGGER.info("Registering index %r", index_url)

        if not dry_run:
            graph.register_python_package_index(index_url, enabled=True)

        index_urls.append(index_url)

    return index_urls


def _do_schedule_core_solver_jobs(
    openshift: OpenShift,
    index_urls: List[str],
    package_name: str,
    package_version: str,
    output: str,
) -> None:
    """Run Python solvers for the given package in specified version."""
    _LOGGER.info(
        "Running solver jobs for package %r in version %r, results will be submitted to %r",
        package_name,
        package_version,
        output,
    )

    solvers_run = openshift.schedule_all_solvers(
        packages=f"{package_name}=={package_version}",
        output=output,
        indexes=index_urls,
    )

    _LOGGER.debug("Response when running solver jobs: %r", solvers_run)


def _schedule_core_solver_jobs(openshift: OpenShift, index_urls: List[str], result_api: str) -> None:
    """Run solver jobs for core components of Python packaging."""
    pypi = Source("https://pypi.org/simple")
    output = result_api + "/api/v1/solver-result"

    _LOGGER.debug("Obtainig setuptools versions")
    for version in pypi.get_package_versions("setuptools"):
        _do_schedule_core_solver_jobs(openshift, index_urls, "setuptools", version, output)

    _LOGGER.debug("Obtainig six versions")
    for version in pypi.get_package_versions("six"):
        _do_schedule_core_solver_jobs(openshift, index_urls, "six", version, output)

    _LOGGER.debug("Obtainig pip versions")
    for version in pypi.get_package_versions("pip"):
        _do_schedule_core_solver_jobs(openshift, index_urls, "pip", version, output)


@click.command()
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    envvar="THOTH_VERBOSE_INIT_JOB",
    help="Be verbose about what's going on.",
)
@click.option(
    "--dry-run",
    "-n",
    is_flag=True,
    envvar="THOTH_INIT_JOB_DRY_RUN",
    help="Just print stuff on LOG, do not change any data.",
)
@click.option(
    "--index-base-url",
    "-i",
    type=str,
    default=_DEFAULT_INDEX_BASE_URL,
    show_default=True,
    help="AICoE URL base for discovering packages.",
)
@click.option(
    "--result-api",
    "-r",
    type=str,
    envvar="THOTH_RESULT_API_URL",
    help="AICoE URL base for discovering packages.",
)
@click.option(
    "--register-indexes-only",
    required=False,
    is_flag=True,
    envvar="THOTH_INIT_JOB_REGISTER_INDEXES_ONLY",
    help="Do not schedule solver jobs, only register indexes.",
)
def cli(verbose: bool = False, dry_run: bool = False, result_api: str = None, index_base_url:
        str = None, register_indexes_only: bool = False):
    """Register AICoE indexes in Thoth's database."""
    graph = None
    openshift = None

    if verbose:
        _LOGGER.setLevel(logging.DEBUG)

    if not index_base_url.endswith("/"):
        index_base_url += "/"

    if not dry_run:
        openshift = OpenShift()

        graph = GraphDatabase()
        graph.connect()
        _LOGGER.info("Initializing schema")
        graph.initialize_schema()
        _LOGGER.info("Talking to graph database located at %r", graph.hosts)
    elif dry_run:
        _LOGGER.info("dry-run: not talking to OpenShift or Dgraph...")

    _LOGGER.info("Registering indexes...")
    indexes = _register_indexes(graph, index_base_url, dry_run)

    if not register_indexes_only:
        if not result_api:
            raise ValueError("No result API URL provided")

        if not dry_run:
            _LOGGER.info("Scheduling solver jobs...")
            _schedule_core_solver_jobs(openshift, indexes, result_api)
        elif dry_run:
            _LOGGER.info("dry-run: not scheduling core solver jobs!")


if __name__ == "__main__":
    cli()
