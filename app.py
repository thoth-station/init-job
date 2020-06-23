#!/usr/bin/env python3
# thoth-init-job
# Copyright(C) 2018, 2019, 2020 Fridolin Pokorny, Francesco Murdaca
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
import yaml
import os
from typing import List
from typing import Generator
from urllib.parse import urljoin
from pathlib import Path

import requests
import click
from bs4 import BeautifulSoup

from thoth.common import init_logging
from thoth.common import OpenShift
from thoth.python import Source
from thoth.storages import GraphDatabase
from thoth.initializer import __service_version__

init_logging()

_LOGGER = logging.getLogger("thoth.init_job")
_DEFAULT_INDEX_BASE_URL = "https://tensorflow.pypi.thoth-station.ninja/index"
_SOLVER_OUTPUT = os.getenv("THOTH_SOLVER_OUTPUT", "http://result-api/api/v1/solver-result")
_PYPI_SIMPLE_API_URL = "https://pypi.org/simple"
_PYPI_WAREHOUSE_JSON_API_URL = "https://pypi.org/pypi"
_CORE_PACKAGES = ["setuptools", "six", "pip"]

_LOGGER.info(f"Thoth Initializer v%s", __service_version__)


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
                present_subdirs - {"simple/"},
            )

        if "simple/" not in present_subdirs:
            _LOGGER.error("No 'simple/' found on URL %r, cannot treat as PEP-503 compliant simple API", url)
            continue

        _LOGGER.info("Detected build configuration %r at %s", configuration[:-1], url)
        result.append(urljoin(url, configuration + "simple"))

    return result


def _list_available_indexes(url: str) -> List[str]:
    """List available indexes on AICoE index."""
    result = []
    _LOGGER.info("Listing available indexes on AICoE index %r", url)

    indexes = list(_html_parse_listing(url))
    if not indexes:
        _LOGGER.error("No AICoE indexes found at %r", url)

    for item in indexes:
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
            index_urls[0], warehouse_api_url=_PYPI_WAREHOUSE_JSON_API_URL, verify_ssl=True, enabled=True
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


def _take_data_science_packages() -> List[str]:
    """Take list of Python Packages for data science."""
    current_path = Path.cwd()
    complete_file_path = current_path.joinpath("hundredsDatasciencePackages.yaml")

    with open(complete_file_path) as yaml_file:
        requested_packages = yaml.safe_load(yaml_file)

    data_science_packages = []
    for package_name in requested_packages["hundreds_datascience_packages"]:
        _LOGGER.info("Registering package %r", package_name)
        data_science_packages.append(package_name)

    return data_science_packages


def _schedule_default_packages_solver_jobs(packages: List[str], index_urls: List[str]) -> None:
    """Run solver jobs for Python packages list selected."""
    openshift = OpenShift()

    counter = 0
    for index_url in index_urls:
        _LOGGER.debug("consider index %r", index_url)
        source = Source(index_url)

        for package_name in packages:
            _LOGGER.debug("Obtaining %r versions", package_name)

            versions = []

            try:
                versions = source.get_package_versions(package_name)

            except Exception as exc:
                _LOGGER.exception(str(exc))

            if versions:

                for version in versions:
                    _LOGGER.info("Scheduling package_name %r in package_version %r", package_name, version)
                    number_workflows = _do_schedule_solver_jobs(openshift, index_urls, package_name, version, _SOLVER_OUTPUT)

                    counter += number_workflows

            _LOGGER.info(f"Already scheduled {counter} solver workflows...")

    return counter


def _do_schedule_solver_jobs(
    openshift: OpenShift, index_urls: List[str], package_name: str, package_version: str, output: str
) -> int:
    """Run Python solvers for the given package in specified version."""
    if not openshift.use_argo:
        _LOGGER.info(
            "Running solver jobs for package %r in version %r, results will be submitted to %r",
            package_name,
            package_version,
            output,
        )
    else:
        _LOGGER.info(
            "Running solver job for package %r in version %r, results will be scheduled using Argo workflow",
            package_name,
            package_version
        )

    solvers_run = openshift.schedule_all_solvers(
        packages=f"{package_name}==={package_version}", output=output, indexes=index_urls
    )

    _LOGGER.debug("Response when running solver jobs: %r", solvers_run)

    return len(solvers_run)


@click.command()
@click.option(
    "--verbose", "-v", is_flag=True, envvar="THOTH_VERBOSE_INIT_JOB", help="Be verbose about what's going on."
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
    "--initialize-schema",
    required=False,
    is_flag=True,
    envvar="THOTH_INIT_JOB_INITIALIZE_SCHEMA",
    help="Initialize schema in Thoth Knowledge Graph.",
)
@click.option(
    "--register-indexes",
    required=False,
    is_flag=True,
    envvar="THOTH_INIT_JOB_REGISTER_INDEXES",
    help="Register indexes in Thoth Knowledge Graph.",
)
@click.option(
    "--solve-core-packages",
    required=False,
    is_flag=True,
    envvar="THOTH_INIT_JOB_REGISTER_CORE_PACKAGES",
    help="Schedule solver jobs for core packages.",
)
@click.option(
    "--solve-data-science-packages",
    required=False,
    is_flag=True,
    envvar="THOTH_INIT_JOB_REGISTER_DATA_SCIENCE_PACKAGES",
    help="Schedule solver jobs for data science packages.",
)
def cli(
    verbose: bool = False,
    dry_run: bool = False,
    index_base_url: str = None,
    initialize_schema: bool = False,
    register_indexes: bool = False,
    solve_core_packages: bool = False,
    solve_data_science_packages: bool = False,
):
    """Register AICoE indexes in Thoth's database."""
    graph = None
    total_scheduled_solvers = 0

    if not dry_run:
        graph = GraphDatabase()
        graph.connect()
    elif dry_run:
        _LOGGER.info("dry-run: not talking to Thoth Knowledge Graph...")

    if verbose:
        _LOGGER.setLevel(logging.DEBUG)

    if not index_base_url.endswith("/"):
        index_base_url += "/"

    if initialize_schema:
        if not dry_run:
            _LOGGER.info("Initializing schema")
            graph.initialize_schema()
        elif dry_run:
            _LOGGER.info("dry-run: not initializing schema...")

    if register_indexes:
        if not dry_run:
            _LOGGER.info("Registering indexes...")
        elif dry_run:
            _LOGGER.info("dry-run: not registering indexes...")

        _register_indexes(graph, index_base_url, dry_run)

    if solve_core_packages:

        if not dry_run:
            _LOGGER.info("Retrieving registered indexes from Thoth Knowledge Graph...")
            registered_indexes = graph.get_python_package_index_urls_all()

            if not registered_indexes:
                raise ValueError("No registered indexes found in the database")

            _LOGGER.info("Scheduling solver jobs for core packages...")
            scheduled_solvers = _schedule_default_packages_solver_jobs(packages=_CORE_PACKAGES, index_urls=registered_indexes)
            _LOGGER.info(f"Total number of solver workflows scheduled for core packages: {scheduled_solvers}!")
            total_scheduled_solvers += scheduled_solvers

        elif dry_run:
            _LOGGER.info("dry-run: not scheduling core packages solver jobs!")

    if solve_data_science_packages:
        data_science_packages = _take_data_science_packages()

        if not dry_run:
            _LOGGER.info("Retrieving registered indexes from Thoth Knowledge Graph...")
            registered_indexes = graph.get_python_package_index_urls_all()

            if not registered_indexes:
                raise ValueError("No registered indexes found in the database")

            _LOGGER.info("Scheduling solver jobs for data science packages...")
            scheduled_solvers = _schedule_default_packages_solver_jobs(packages=data_science_packages, index_urls=registered_indexes)
            _LOGGER.info(f"Total number of solver workflows scheduled for DS packages: {scheduled_solvers}!")
            total_scheduled_solvers += scheduled_solvers

        elif dry_run:
            _LOGGER.info("dry-run: not scheduling data science packages solver jobs!")

    _LOGGER.info(f"Total number of solver workflows scheduled: {total_scheduled_solvers}!")

if __name__ == "__main__":
    cli()
