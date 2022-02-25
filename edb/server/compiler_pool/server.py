#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import asyncio

import click

from .. import main
from . import worker
from . import pool


class MultiSchemaPool(pool.FixedPool):
    pass


async def server_main(socket_path):
    print(socket_path)
    # TODO: listen on socket_path, and serve with MultiSchemaPool
    MultiSchemaPool()


@main.main.command()
@click.argument("socket_path")
def compiler(socket_path):
    rv = asyncio.run(server_main(socket_path))
    if rv:
        worker.run_worker(*rv)


if __name__ == '__main__':
    compiler()
