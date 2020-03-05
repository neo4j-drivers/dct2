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


from io import BytesIO
import struct


class MessageWriter(object):

    def __init__(self, wire):
        self._wire = wire
        self._buffer = BytesIO()
        self._packer = SimplePacker(self._buffer)

    def _write_chunk(self, data):
        size = len(data)
        self._wire.write(struct.pack(">H", size))
        self._wire.write(data)

    def write_message(self, tag, *fields):
        data = bytearray([0xB0 + len(fields), tag])
        for field in fields:
            self._buffer.seek(0)
            self._buffer.truncate(0)
            self._packer.pack(field)
            data.extend(self._buffer.getvalue())
        for offset in range(0, len(data), 32767):
            end = offset + 32767
            self._write_chunk(data[offset:end])
        self._write_chunk(b"")

    def send(self):
        """ Send any outstanding data and return the number of
        bytes sent.
        """
        return self._wire.send()


class MessageReader(object):

    def __init__(self, wire):
        self._wire = wire
        self._buffer = BytesIO()
        self._unpacker = SimpleUnpacker(self._buffer)

    def _read_chunk(self):
        size, = struct.unpack(">H", self._wire.read(2))
        if size:
            return self._wire.read(size)
        else:
            return b""

    def read_message(self):
        self._buffer.seek(0)
        self._buffer.truncate(0)
        more = True
        while more:
            chunk = self._read_chunk()
            if chunk:
                self._buffer.write(chunk)
            else:
                more = False
        self._buffer.seek(0)
        _, n = divmod(ord(self._buffer.read(1)), 0x10)
        tag = ord(self._buffer.read(1))
        fields = tuple(self._unpacker.unpack() for _ in range(n))
        return tag, fields


class SimplePacker:

    def __init__(self, buffer):
        self._buffer = buffer

    def _write(self, value):
        self._buffer.write(value)

    def _write_u8(self, value):
        self._buffer.write(struct.pack(">B", value))

    def _write_u16be(self, value):
        self._buffer.write(struct.pack(">H", value))

    def _write_u32be(self, value):
        self._buffer.write(struct.pack(">I", value))

    def _write_i8(self, value):
        self._buffer.write(struct.pack(">b", value))

    def _write_i16be(self, value):
        self._buffer.write(struct.pack(">h", value))

    def _write_i32be(self, value):
        self._buffer.write(struct.pack(">i", value))

    def _write_i64be(self, value):
        self._buffer.write(struct.pack(">q", value))

    def _pack_int(self, obj):
        if -0x10 <= obj < 0x80:
            self._write_i8(obj)
        elif -0x80 <= obj < -0x10:
            self._write_u8(0xC8)
            self._write_i8(obj)
        elif -0x8000 <= obj < 0x8000:
            self._write_u8(0xC9)
            self._write_i16be(obj)
        elif -0x80000000 <= obj < 0x80000000:
            self._write_u8(0xCA)
            self._write_i32be(obj)
        elif -0x8000000000000000 <= obj < 0x8000000000000000:
            self._write_u8(0xCB)
            self._write_i64be(obj)
        else:
            raise OverflowError("Integer %r out of range" % obj)

    def _pack_header(self, size, tiny, small, medium, large):
        if size < 0x10:
            self._write_u8(tiny + size)
        elif size < 0x100:
            self._write_u8(small)
            self._write_u8(size)
        elif size < 0x10000:
            self._write_u8(medium)
            self._write_u16be(size)
        elif size < 0x100000000:
            self._write_u8(large)
            self._write_u32be(size)
        else:
            raise OverflowError("Header size %r out of range" % size)

    def _pack_str(self, obj):
        encoded = obj.encode("utf-8")
        size = len(encoded)
        self._pack_header(size, tiny=0x80, small=0xD0, medium=0xD1, large=0xD2)
        self._write(encoded)

    def _pack_list(self, obj):
        size = len(obj)
        self._pack_header(size, tiny=0x90, small=0xD4, medium=0xD5, large=0xD6)
        for item in obj:
            self.pack(item)

    def _pack_dict(self, obj):
        size = len(obj)
        self._pack_header(size, tiny=0xA0, small=0xD8, medium=0xD9, large=0xDA)
        for key, value in obj.items():
            self._pack_str(key)
            self.pack(value)

    def pack(self, obj):
        if obj is None:
            self._write_u8(0xC0)
        elif isinstance(obj, int):
            self._pack_int(obj)
        elif isinstance(obj, str):
            self._pack_str(obj)
        elif isinstance(obj, list):
            self._pack_list(obj)
        elif isinstance(obj, dict):
            self._pack_dict(obj)
        else:
            raise ValueError("Unsupported value {!r}".format(obj))


class SimpleUnpacker:

    def __init__(self, buffer):
        self._buffer = buffer

    def _read(self, n):
        return self._buffer.read(n)

    def _read_u8(self):
        value, = struct.unpack(">B", self._buffer.read(1))
        return value

    def _read_u16be(self):
        value, = struct.unpack(">H", self._buffer.read(2))
        return value

    def _read_u32be(self):
        value, = struct.unpack(">I", self._buffer.read(4))
        return value

    def _read_i8(self):
        value, = struct.unpack(">b", self._buffer.read(1))
        return value

    def _read_i16be(self):
        value, = struct.unpack(">h", self._buffer.read(2))
        return value

    def _read_i32be(self):
        value, = struct.unpack(">i", self._buffer.read(4))
        return value

    def _read_i64be(self):
        value, = struct.unpack(">q", self._buffer.read(8))
        return value

    def _unpack_string(self, size):
        return self._read(size).decode("utf-8")

    def _unpack_list(self, size):
        return [self.unpack() for _ in range(size)]

    def _unpack_dict(self, size):
        return {self.unpack(): self.unpack() for _ in range(size)}

    def unpack(self):
        marker = self._read_u8()
        if marker == 0xC0:
            return None

        # Integer
        elif marker < 0x80:
            return marker
        elif marker >= 0xF0:
            return marker - 0x100
        elif marker == 0xC8:
            return self._read_i8()
        elif marker == 0xC9:
            return self._read_i16be()
        elif marker == 0xCA:
            return self._read_i32be()
        elif marker == 0xCB:
            return self._read_i64be()

        # String
        elif 0x80 <= marker <= 0x8F:
            return self._unpack_string(size=(marker & 0x0F))
        elif marker == 0xD0:
            return self._unpack_string(size=self._read_u8())
        elif marker == 0xD1:
            return self._unpack_string(size=self._read_u16be())
        elif marker == 0xD2:
            return self._unpack_string(size=self._read_u32be())

        # List
        elif 0x90 <= marker <= 0x9F:
            return self._unpack_list(size=(marker & 0x0F))
        elif marker == 0xD4:
            return self._unpack_list(size=self._read_u8())
        elif marker == 0xD5:
            return self._unpack_list(size=self._read_u16be())
        elif marker == 0xD6:
            return self._unpack_list(size=self._read_u32be())

        # Dictionary
        elif 0xA0 <= marker <= 0xAF:
            return self._unpack_dict(size=(marker & 0x0F))
        elif marker == 0xD4:
            return self._unpack_dict(size=self._read_u8())
        elif marker == 0xD5:
            return self._unpack_dict(size=self._read_u16be())
        elif marker == 0xD6:
            return self._unpack_dict(size=self._read_u32be())

        else:
            raise ValueError("Unsupported marker {!r}".format(marker))
