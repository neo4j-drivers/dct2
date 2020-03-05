#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j.
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


from logging import getLogger, DEBUG, basicConfig
from sys import stdout

from dct2 import Bolt


log = getLogger(__name__)


def main():
    basicConfig(stream=stdout, level=DEBUG)
    bolt = Bolt.open(("localhost", 7687), "neo4j", "password")
    result = bolt.run("UNWIND range(1, 3) AS n RETURN n", {})
    bolt.pull(result)
    bolt.sync(result)
    while result.has_records():
        print(result.take_record())
    bolt.close()


if __name__ == "__main__":
    main()
