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


from collections import deque
from logging import getLogger
from socket import socket


from dct2.messaging import MessageReader, MessageWriter
from dct2.wire import Wire


log = getLogger(__name__)


class Bolt(object):
    """ A single point-to-point connection between a client and a
    server.
    """

    user_agent = "dct/2"

    server_agent = None

    connection_id = None

    @classmethod
    def open(cls, address, user, password):
        s = socket()
        log.debug("C: <DIAL> %r", address)
        s.connect(address)
        log.debug("S: <ACCEPT>")

        wire = Wire(s)

        def handshake():
            log.debug("C: <BOLT>")
            wire.write(b"\x60\x60\xB0\x17")
            log.debug("C: <PROTOCOL> 4.0 | 0.0 | 0.0 | 0.0")
            wire.write(b"\x00\x00\x00\x04"
                       b"\x00\x00\x00\x00"
                       b"\x00\x00\x00\x00"
                       b"\x00\x00\x00\x00")
            wire.send()
            v = bytearray(wire.read(4))
            log.debug("S: <PROTOCOL> %d.%d", v[-1], v[-2])
            return v[-1], v[-2]

        protocol_version = handshake()
        assert protocol_version == (4, 0)
        bolt = cls(wire)
        bolt.hello(user, password)
        return bolt

    def __init__(self, wire):
        self.wire = wire
        self.reader = MessageReader(wire)
        self.writer = MessageWriter(wire)
        self.responses = deque()

    def close(self):
        if self.closed or self.broken:
            return
        self.goodbye()
        self.wire.close()
        log.debug("C: <HANGUP>")

    @property
    def closed(self):
        return self.wire.closed

    @property
    def broken(self):
        return self.wire.broken

    def hello(self, user, password):
        extra = {"user_agent": self.user_agent,
                 "scheme": "basic",
                 "principal": user,
                 "credentials": password}
        clean_extra = dict(extra)
        clean_extra.update({"credentials": "*******"})
        log.debug("C: HELLO %r", clean_extra)
        response = self._write_request(0x01, extra)
        self._send()
        self._wait(response)
        self.server_agent = response.metadata.get("server")
        self.connection_id = response.metadata.get("connection_id")

    def goodbye(self):
        log.debug("C: GOODBYE")
        self._write_request(0x02)
        self._send()

    def run(self, cypher, parameters, db=None):
        extra = {"db": db}
        log.debug("C: RUN %r %r %r", cypher, parameters, extra or {})
        response = self._write_request(0x10, cypher, parameters, extra or {})
        result = BoltResult(response)
        return result

    def pull(self, result, n=-1):
        extra = {"n": n}
        log.debug("C: PULL %r", extra)
        response = self._write_request(0x3F, extra)
        result.append(response, final=True)
        return response

    def sync(self, result):
        self._send()
        self._wait(result.last())

    def take(self, result):
        if not result.has_records() and not result.done():
            while self.responses[0] is not result.last():
                self._wait(self.responses[0])
            self._wait(result.last())
        record = result.take_record()
        return record

    def _write_request(self, tag, *fields):
        response = BoltResponse()
        self.writer.write_message(tag, *fields)
        self.responses.append(response)
        return response

    def _send(self):
        sent = self.writer.send()
        if sent:
            log.debug("C: <SENT %r bytes>", sent)

    def _wait(self, response):
        """ Read all incoming responses up to and including a
        particular response.
        """
        while not response.done():
            self._fetch()

    def _fetch(self):
        """ Fetch and process the next incoming message.
        """
        rs = self.responses[0]
        tag, fields = self.reader.read_message()
        if tag == 0x70:
            log.debug("S: SUCCESS %s", " ".join(map(repr, fields)))
            rs.set_success(**fields[0])
            self.responses.popleft()
        elif tag == 0x71:
            log.debug("S: RECORD %s", " ".join(map(repr, fields)))
            rs.add_record(fields[0])


class BoltResult(object):
    """ A query carried out over a Bolt connection.

    Implementation-wise, this form of query is comprised of a number of
    individual message exchanges. Each of these exchanges may succeed
    or fail in its own right, but contribute to the overall success or
    failure of the query.
    """

    def __init__(self, response):
        self._items = deque([response])
        self._complete = False

    def append(self, response, final=False):
        self._items.append(response)
        if final:
            self._complete = True

    def last(self):
        return self._items[-1]

    def has_records(self):
        return any(response.has_records()
                   for response in self._items)

    def take_record(self):
        for response in self._items:
            record = response.take_record()
            if record is None:
                continue
            return record
        return None


class BoltResponse(object):

    # 0 = not done
    # 1 = success

    def __init__(self):
        super(BoltResponse, self).__init__()
        self._records = deque()
        self._status = 0
        self._metadata = {}

    def add_record(self, values):
        self._records.append(values)

    def has_records(self):
        return bool(self._records)

    def take_record(self):
        try:
            return self._records.popleft()
        except IndexError:
            return None

    def set_success(self, **metadata):
        self._status = 1
        self._metadata.update(metadata)

    def done(self):
        return self._status != 0

    @property
    def metadata(self):
        return self._metadata
