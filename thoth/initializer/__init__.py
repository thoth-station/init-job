#!/usr/bin/env python3
# thoth-initializer
# Copyright(C) 2020 Fridolin Pokorny, Francesco Murdaca
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


"""This is thoth-initializer to start populating Thoth so that its knowledge can be spread."""

from thoth.common import __version__ as __common__version__
from thoth.storages import __version__ as __storage__version__
from thoth.python import __version__ as __python__version__


__name__ = "thoth-initializer"
__version__ = "0.7.0"
__service_version__ = f"{__version__}+\
    python.{__python__version__}.\
        storage.{__storage__version__}.\
            common.{__common__version__}"
