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

import click
import io
import logging
import os
import pathlib
import pyhash
import sys

from datasketch import MinHash, LeanMinHash
from datasketch.hashfunc import sha1_hash32
from functools import partial
from xopen import xopen

from biosketch.cli import CustomCommand, CustomOption, CompressionOption
from biosketch.logging import log_init
from biosketch.hashIO import BiosketchRecord, BiosketchFileWriter
from biosketch.kmers import iter_kmers
from biosketch.multiprocessing import Pipeline, PipelineRunnerFactory

logger = logging.getLogger()


@click.command(
    cls=CustomCommand,
    name='hashify',
    short_help='Create a sketch of a FASTA/FASTQ file',
    context_settings=dict(max_content_width=120),
)
@click.argument(
    'fastx_file',
    metavar='<input.fa>|<input.fq>',
    type=click.Path(path_type=pathlib.Path),
)
@click.option(
    '-j', '--processes',
    'processes',
    cls=CustomOption,
    metavar='INT',
    type=int,
    default=1,
    show_default=True,
    option_group='Processing options',
    help='Start INT parallel worker processes',
)
@click.option(
    '-c', '--chunk-size',
    'chunk_size',
    cls=CustomOption,
    metavar='INT',
    type=int,
    default=None,
    show_default=True,
    option_group='Processing options',
    help='Read FASTQ/FASTQ chunks of up to INT bytes',
)
@click.option(
    '-k', '--kmer-size',
    'kmer_size',
    cls=CustomOption,
    metavar='INT',
    type=int,
    default=31,
    show_default=True,
    option_group='Hashing options',
    help='Hash k-mers of INT lengths',
)
@click.option(
    '-n', '--num-perm',
    'num_perm',
    cls=CustomOption,
    metavar='INT',
    type=int,
    default=128,
    show_default=True,
    option_group='Hashing options',
    help='Apply INT random permutation functions',
)
@click.option(
    '-s', '--seed',
    'seed',
    cls=CustomOption,
    metavar='INT',
    type=int,
    default=1,
    show_default=True,
    option_group='Hashing options',
    help='The seed for generating permutation functions',
)
@click.option(
    '-f', '--hash-func',
    'hash_func',
    cls=CustomOption,
    type=click.Choice(
        ['FNV', 'MurmurHash', 'CityHash', 'FarmHash', 'xxHash'],
        case_sensitive=False,
    ),
    default='SHA1',
    show_default=True,
    option_group='Hashing options',
    help='The random hash function to use',
)
@click.option(
    '-o', '--output',
    'output_file',
    cls=CustomOption,
    metavar='FILE',
    type=click.Path(path_type=pathlib.Path),
    default='-',
    show_default='stdout',
    option_group='Output options',
    help='Write output to FILE',
)
@click.option(
    '-x', '--compress-fmt',
    'compression_format',
    cls=CustomOption,
    type=click.Choice(
        ['gz', 'xz', 'bz2'],
        case_sensitive=False,
    ),
    default=None,
    show_default='autodetected',
    option_group='Output options',
    help='Override the output file format',
)
@click.option(
    '-#', '--fast', '--best',
    'compression_level',
    cls=CompressionOption,
    is_flag=True,
    type=int,
    default=1,
    show_default=True,
    option_group='Output options',
    help='Set the output compression level',
)
@click.option(
    '-@', '--threads',
    'compression_threads',
    cls=CustomOption,
    metavar='INT',
    type=int,
    default=0,
    show_default=True,
    option_group='Output options',
    help='The number of compression threads to use',
)
@log_init
@click.help_option(
    '-h',
    '--help',
)
def hashify_command(
    fastx_file,

    # Processing options:
    processes,
    chunk_size,

    # Hashing options:
    kmer_size,
    num_perm,
    seed,
    hash_func,

    # Output options:
    output_file,
    compression_format,
    compression_level,
    compression_threads,
):
    """
    Create a sketch of a FASTA/FASTQ file
    """
    try:
        if processes == 0:
            processes = os.cpu_count() - 1
    except TypeError:
        logger.warning('Could not determine the number of CPUs in the system')
        processes = 1

    hash_funcs = {
        'FNV': pyhash.fnv1a_32(),
        'MurmurHash': pyhash.murmur3_32(),
        'CityHash': pyhash.city_32(),
        'FarmHash': pyhash.farm_32(),
        'xxHash': pyhash.xx_32(),
    }


    pipeline = Pipeline()

    @pipeline.task()
    def hashify(records):
        hashfunc = hash_funcs.get(hash_func, sha1_hash32)
        template = MinHash(num_perm=num_perm, seed=seed, hashfunc=hashfunc)
        for rec in records:
            kmers = set(iter_kmers(rec.sequence.encode('ascii'), kmer_size))
            set_size = len(kmers)

            minhash = template.copy()
            minhash.update_batch(kmers)

            yield BiosketchRecord(rec.name, minhash.hashvalues, seed, set_size)

    @pipeline.task()
    def serialize(records):
        outfile = io.BytesIO()
        with BiosketchFileWriter(outfile) as out:
            out.write_records(records)

        return outfile.getvalue()


    runner_factory = PipelineRunnerFactory()

    runner_config = dict(
        pipeline=pipeline,
        processes=processes,
        input_file=fastx_file,
        output_file=output_file,
        logger=logger,
        input_opener=partial(xopen, threads=0),
        output_opener=partial(
            xopen,
            format=compression_format,
            compresslevel=compression_level,
            threads=compression_threads,
        ),
        chunk_size=chunk_size,
    )

    runner = runner_factory.create_runner(**runner_config)
    runner.run()

