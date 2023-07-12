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
import logging
import os
import sys

from dnaio import FileFormatError, UnknownFileFormat

import biosketch.dialects

from biosketch.commands.hashify import hashify_command

from biosketch.hashIO import BiosketchFileError

from biosketch import __version__

logger = logging.getLogger()


def safe_entry_point():
    try:
        path, *args = sys.argv
        cmd_name = os.path.basename(path)

        ctx = entry_point.make_context(cmd_name, args)
        with ctx:
            entry_point.invoke(ctx)

    except click.exceptions.Exit as exc:
        sys.exit(exc.exit_code)

    except click.ClickException as exc:
        exc.show()
        sys.exit(1)

    except KeyboardInterrupt:
        click.echo("Interrupted!", file=sys.stderr)
        sys.exit(130)

    except (
        BiosketchFileError,
        EOFError,
        OSError,
        FileFormatError,
        UnknownFileFormat,
    ) as e:
        logger.error("Biosketch: error: %s", e)
        logger.debug("Biosketch: traceback:", exc_info=True)
        sys.exit(1)

    except Exception as exc:
        logger.exception(exc)
        sys.exit(1)


@click.group()
@click.help_option('-h', '--help')
@click.version_option(__version__, '-v', '--version')
def entry_point():
    pass


entry_point.add_command(hashify_command)
