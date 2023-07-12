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
import platform
import shlex
import sys
import time

from functools import wraps

from biosketch import __version__

import sys, traceback

logger = logging.getLogger()


class LogOption(click.Option):

    def __init__(self, *args, **kwargs):
        self.option_group = 'Logging options'
        super(LogOption, self).__init__(*args, **kwargs)


class LogState:

    def __init__(self):
        self.logfile = None
        self.verbose = False


def logfile_option(func):

    def callback(ctx, param, value):
        log_state = ctx.ensure_object(LogState)
        log_state.logfile = value
        return value

    option = click.option(
        '-l',
        '--logfile',
        cls=LogOption,
        metavar='FILE',
        type=click.Path(),
        default=None,
        show_default='stderr',
        expose_value=False,
        help='Write logging to FILE',
        callback=callback,
    )

    return option(func)


def verbose_option(func):

    def callback(ctx, param, value):
        log_state = ctx.ensure_object(LogState)
        log_state.verbose = value
        return value

    option = click.option(
        '-v',
        '--verbose',
        cls=LogOption,
        is_flag=True,
        default=False,
        show_default=True,
        expose_value=False,
        help='Sets the log level to DEBUG',
        callback=callback,
    )

    return option(func)


def log_init(func):

    @wraps(func)
    def wrapper(*args, **kwargs):
        ctx = click.get_current_context()
        state = ctx.find_object(LogState)

        logging.basicConfig(
            filename=state.logfile,
            level=logging.DEBUG if state.verbose else logging.INFO,
            format='%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )

        logger.info(f'Command: {shlex.join(sys.argv)}')
        logger.info(f'Run options: {str(kwargs)}')
        logger.info(f'PID: {os.getpid()}')
        logger.info(f'Version: {__version__}')

        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()

        logger.info(time.strftime('Time taken: %H:%M:%S', time.gmtime(end-start)))

        return result

    wrapper = verbose_option(wrapper)
    wrapper = logfile_option(wrapper)

    return wrapper

