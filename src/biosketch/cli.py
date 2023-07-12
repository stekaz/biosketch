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
import pathlib

from collections import defaultdict


class CustomCommand(click.Command):

    def format_options(self, ctx, formatter):
        opts = defaultdict(list)
        spacings = dict()

        for param in self.get_params(ctx):
            rv = param.get_help_record(ctx)
            if rv is not None:
                if hasattr(param, 'option_group') and param.option_group:
                    opts[str(param.option_group)].append(rv)
                else:
                    opts['Other options'].append(rv)

        max_spacings = dict()
        for name, opts_group in opts.items():
            max_spacings[name] = 0
            for term, desc in opts_group:
                max_spacings[name] = max(max_spacings[name], len(term))

        col_max = 30
        for name, opts_group in opts.items():
            col_spacing = col_max - min(max_spacings[name], col_max) + 3
            with formatter.section(name):
                formatter.write_dl(opts_group, col_max, col_spacing)


class CustomOption(click.Option):

    def __init__(self, *args, **kwargs):
        self.option_group = kwargs.pop('option_group', None)
        super(CustomOption, self).__init__(*args, **kwargs)


class CompressionOption(CustomOption):

    def __init__(self, *args, **kwargs):
        self.compression_levels = list(range(1, 10))
        super(CompressionOption, self).__init__(*args, **kwargs)

    def get_default(self, ctx, call=False):
        return self.default

    def add_to_parser(self, parser, ctx):
        if '-#' in self.opts:
            for level in self.compression_levels:
                parser.add_option(
                    obj=self,
                    opts=(('-'+str(level)),),
                    dest=self.name,
                    action='store_const',
                    const=level,
                )
        if '--fast' in self.opts:
            parser.add_option(
                obj=self,
                opts=(('--fast'),),
                dest=self.name,
                action='store_const',
                const=1,
            )
        if '--best' in self.opts:
            parser.add_option(
                obj=self,
                opts=(('--best'),),
                dest=self.name,
                action='store_const',
                const=9,
            )

