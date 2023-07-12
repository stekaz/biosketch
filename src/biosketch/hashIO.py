#!/usr/bin/env python
# coding: utf-8
###############################################################################
#
#    Biosketch
#
#    Copyright (C) 2023  QIMR Berghofer Medical Research Institute
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
###############################################################################

import io
import struct

from functools import partial, cached_property
from itertools import chain
from os import PathLike
from typing import Optional, Union, BinaryIO, List, Iterable, IO, Sequence, Tuple

from dataclasses import dataclass, astuple

from datasketch import MinHash, LeanMinHash
from xopen import xopen


class BiosketchFileError(Exception):
    pass


@dataclass(frozen=True)
class BiosketchRecord:
    """
    A class representing a Biosketch file record.
    """
    description: str
    hash_values: Sequence[int]
    seed: int
    set_size: int

    def __iter__(self):
        return iter(astuple(self))

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    @cached_property
    def lean_minhash(self):
        lean_minhash = LeanMinHash(
            seed=self.seed,
            hashvalues=self.hash_values,
        )
        return lean_minhash


class BiosketchRecordSerializer:

    byteorder = '<'
    encoding = 'ascii'

    @staticmethod
    def serialize(record: BiosketchRecord) -> Tuple[bytes, bytes]:
        """
        Serialize a BiosketchRecord.

        Parameters:
            record (BiosketchRecord): The BiosketchRecord to serialize.

        Returns:
            Tuple[bytes, bytes]: A tuple (lines) containing the serialized data.

        Notes:
            The first tuple element represents the description encoded as ASCII.
            The second tuple element is the serialized data.
        """
        description = record.description.removeprefix('>')
        defline = f'>{description}\n'.encode(self.encoding)
        try:
            fmt = '%sIqi%dIb' % (self.byteorder, len(record.hash_values))
            buf = struct.pack(
                fmt,
                record.set_size,
                record.seed,
                len(record.hash_values),
                *record.hash_values,
                ord('\n'),
            )
        except struct.error as e:
            raise BiosketchRecordError(f'Failed to pack BiosketchRecord: {str(e)}')

        return defline, buf

    @staticmethod
    def deserialize(fileobj: IO[bytes]) -> Optional[BiosketchRecord]:
        """
        Deserialize a BiosketchRecord from a file object.

        Parameters:
            fileobj (file-like object): The file object containing the serialized data.

        Returns:
            BiosketchRecord: The deserialized BiosketchRecord.
        """
        line = fileobj.readline()
        if not line:
            return None

        try:
            description = line.decode(self.encoding).removeprefix('>')
        except UnicodeDecodeError as e:
            raise BiosketchRecordError(f'Error decoding description line: {line}') from e

        fmt_seed_size = self.byteorder + 'Iqi'
        fmt_hash = self.byteorder + '%dI'
        try:
            # Read and unpack the seed and num_perm values
            seed_data = fileobj.read(struct.calcsize(fmt_seed_size))
            set_size, seed, num_perm = struct.unpack(fmt_seed_size, seed_data)

            # Read and unpack the hash values
            hash_data = fileobj.read(struct.calcsize(fmt_hash % num_perm) + 1)
            hash_values = struct.unpack_from(fmt_hash % num_perm, hash_data)

        except struct.error as e:
            raise BiosketchRecordError(f'Failed to unpack MinHash data: {str(e)}') from e

        return BiosketchRecord(description, hash_values, seed, set_size)


class BiosketchFileWriter:
    """
    Class for writing Biosketch files
    """
    def __init__(
        self,
        file: Union[str, BinaryIO, PathLike],
        opener = partial(xopen, threads=0),
    ):
        self.file = file
        self.opener = opener
        self.fileobj = None
        self.close_file = False

    def __enter__(self):
        if isinstance(self.file, (str, bytes, PathLike)):
            self.fileobj = self.opener(self.file, 'wb')
            self.close_file = True
        else:
            self.fileobj = self.file

        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        if self.close_file and self.fileobj is not None:
            self.fileobj.close()
            self.fileobj = None

    def write_records(self, records: Iterable[BiosketchRecord]):
        serializer = BiosketchRecordSerializer()
        lines = chain.from_iterable(map(serializer.serialize, records))

        self.fileobj.writelines(lines)

    def write_record(self, record: BiosketchRecord):
        lines = BiosketchRecordSerializer.serialize(record)

        self.fileobj.writelines(lines)


class BiosketchFileReader:
    """
    Class for reading Biosketch files
    """
    def __init__(
        self,
        file: Union[str, BinaryIO, PathLike],
        opener = partial(xopen, threads=0),
    ):
        if isinstance(file, (str, bytes, PathLike)):
            self.fileobj = opener(file, 'rb')
            self.close_file = True
        else:
            self.fileobj = file
            self.close_file = False

    def __iter__(self):
        return self

    def __next__(self) -> BiosketchRecord:
        defline = self._read_defline()
        if defline is None:
            raise StopIteration

        raise NotImplementedError

    def close(self):
        if self.close_file and self.fileobj is not None:
            self.fileobj.close()
            self.fileobj = None

    def __enter__(self):
        if self.fileobj is None:
            raise ValueError('I/O operation on closed file')
        return self

    def __exit__(self, *args):
        self.close()

