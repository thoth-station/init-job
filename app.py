#!/usr/bin/env python3
# thoth-init-job
# Copyright(C) 2018 Fridolin Pokorny
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
import typing

import requests
import click
from bs4 import BeautifulSoup

from thoth.common import init_logging
from thoth.common import OpenShift
from thoth.python import Source
from thoth.storages import GraphDatabase

init_logging()

_LOGGER = logging.getLogger("thoth.init_job")
_DEFAULT_INDEX_BASE_URL = "http://tensorflow.pypi.thoth-station.ninja/index"


def _get_build_configuration(index_base_url, distro) -> list:
    """Get available configration for a distro."""
    build_configuration_url = index_base_url + "/" + distro

    response = requests.get(build_configuration_url)
    soup = BeautifulSoup(response.text, "lxml")
    table = soup.find("table")
    if not table:
        return []

    configurations = []
    for row in table.find_all("tr"):
        for cell in row.find_all("td"):
            if cell.a:
                configuration = cell.a.text
                if configuration == "Parent Directory":
                    continue

                configurations.append(
                    build_configuration_url + configuration + "simple"
                )

    return configurations


def _list_available_indexes(index_base_url: str) -> list:
    """List available indexes on AICoE index."""
    _LOGGER.info("Listing available indexes on AICoE index %r.", index_base_url)
    response = requests.get(index_base_url)
    soup = BeautifulSoup(response.text, "lxml")

    result = []
    for row in soup.find("table").find_all("tr"):
        for cell in row.find_all("td"):
            if cell.a:
                distro = cell.a.text
                if distro == "Parent Directory":
                    continue

                result.extend(_get_build_configuration(index_base_url, distro))

    return result


def _register_indexes(graph: GraphDatabase, index_base_url: str):
    """Register available AICoE indexes into Thoth's database."""
    _LOGGER.info("Registering PyPI index https://pypi.org/simple")
    graph.register_python_package_index(
        "https://pypi.org/simple",
        warehouse_api_url="https://pypi.org/pypi",
        verify_ssl=True,
        warehouse=True,
    )

    for index_url in _list_available_indexes(index_base_url):
        _LOGGER.info("Registering index %r", index)
        graph.register_python_package_index(index_url)


def _do_run_core_solver_jobs(
    openshift: OpenShift,
    index_urls: typing.List[str],
    package_name: str,
    package_version: str,
    output: str,
):
    """Run Python solvers for the given package in specified version."""
    _LOGGER.info(
        "Running solver jobs for package %r in version %r, results will be submitted to %r",
        package_name,
        package_version,
        output,
    )

    solvers_run = openshift.run_solver(
        packages=f"{package_name}=={package_version}",
        output="result-api",
        indexes=index_urls,
    )

    _LOGGER.debug("Response when running solver jobs: %r", solvers_run)


def _run_core_solver_jobs(graph: GraphDatabase, openshift: OpenShift, result_api: str):
    """Run solver jobs for core components of Python packaging."""
    pypi = Source("https://pypi.org/simple")
    index_urls = graph.get_python_package_index_urls()

    output = result_api + "/api/v1/solver-result"

    _LOGGER.debug("Obtainig setuptools versions")
    for version in pypi.get_package_versions("setuptools"):
        _do_run_core_solver_jobs(openshift, index_urls, "setuptools", version, output)

    _LOGGER.debug("Obtainig six versions")
    for version in pypi.get_package_versions("six"):
        _do_run_core_solver_jobs(openshift, index_urls, "six", version, output)


@click.command()
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    envvar="THOTH_VERBOSE_INIT_JOB",
    help="Be verbose about what's going on.",
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
    "-i",
    required=True,
    type=str,
    envvar="THOTH_RESULT_API_URL",
    help="AICoE URL base for discovering packages.",
)
def cli(verbose: bool = False, result_api: str = None, index_base_url: str = None):
    """Register AICoE indexes in Thoth's database."""
    if verbose:
        _LOGGER.setLevel(logging.DEBUG)

    openshift = OpenShift()

    graph = GraphDatabase()
    graph.connect()

    _register_indexes(graph, index_base_url)
    _run_core_solver_jobs(graph, openshift, result_api)


if __name__ == "__main__":
    cli()
