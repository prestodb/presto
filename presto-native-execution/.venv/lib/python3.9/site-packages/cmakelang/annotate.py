# -*- coding: utf-8 -*-
"""
Parse cmake listfiles and re-emit them with semantic annotations in HTML.

Some options regarding parsing are configurable by providing a configuration
file. The configuration file format is the same as that used by cmake-format,
and the same file can be used for both programs.

cmake-format can spit out the default configuration for you as starting point
for customization. Run with `--dump-config [yaml|json|python]`.
"""
from __future__ import unicode_literals

import argparse
import io
import logging
import os
import sys

import cmakelang
from cmakelang.format import __main__
from cmakelang import configuration
from cmakelang import lex
from cmakelang import parse
from cmakelang import render


EMBED_TPL = """
<template id="renderedcontent">
{{html_content}}
</template>
<iframe id="renderframe" style="width:100%;"></iframe>
<script type="text/javascript">
  var frame = document.getElementById("renderframe");
  frame.addEventListener("load", function() {
    frame.height = frame.contentWindow.document.body.scrollHeight + 30;
  });
  frame.srcdoc = document.getElementById("renderedcontent").innerHTML;
</script>
"""


def annotate_file(config, infile, outfile, outfmt=None):
  """
  Parse the input cmake file, re-format it, and print to the output file.
  """

  infile_content = infile.read()
  if config.format.line_ending == 'auto':
    detected = __main__.detect_line_endings(infile_content)
    config = config.clone()
    config.format.set_line_ending(detected)
  tokens = lex.tokenize(infile_content)
  parse_db = parse.funs.get_parse_db()
  parse_db.update(parse.funs.get_funtree(config.parse.fn_spec))
  ctx = parse.ParseContext(parse_db)
  parse_tree = parse.parse(tokens, ctx)

  if outfmt == "page":
    html_content = render.get_html(parse_tree, fullpage=True)
    outfile.write(html_content)
    return
  if outfmt == "stub":
    html_content = render.get_html(parse_tree, fullpage=False)
    outfile.write(html_content)
    return
  if outfmt == "iframe":
    html_content = render.get_html(parse_tree, fullpage=True)
    wrap_lines = EMBED_TPL.split("\n")
    for line in wrap_lines[:2]:
      outfile.write(line)
      outfile.write("\n")
    outfile.write(html_content)
    for line in wrap_lines[3:]:
      outfile.write(line)
      outfile.write("\n")
    return

  raise ValueError("Invalid output format: {}".format(outfmt))


USAGE_STRING = """
cmake-annotate [-h]
             [--format {page,stub}]
             [-o OUTFILE_PATH]
             [-c CONFIG_FILE]
             infilepath [infilepath ...]
"""


def setup_argparser(arg_parser):
  """
  Add argparse options to the parser.
  """
  arg_parser.add_argument('-v', '--version', action='version',
                          version=cmakelang.__version__)
  arg_parser.add_argument(
      "-f", "--format", choices=["page", "stub", "iframe"], default="stub",
      help="whether to output a standalone `page` complete with <html></html> "
           "tags, or just the annotated content")

  arg_parser.add_argument('-o', '--outfile-path', default=None,
                          help='Where to write the formatted file. '
                               'Default is stdout.')
  arg_parser.add_argument('-c', '--config-file',
                          help='path to configuration file')
  arg_parser.add_argument('infilepaths', nargs='*')


def main():
  """Parse arguments, open files, start work."""

  # set up main logger, which logs everything. We'll leave this one logging
  # to the console
  logging.basicConfig(level=logging.INFO)
  arg_parser = argparse.ArgumentParser(
      description=__doc__,
      formatter_class=argparse.RawDescriptionHelpFormatter,
      usage=USAGE_STRING)
  setup_argparser(arg_parser)
  args = arg_parser.parse_args()

  assert (len(args.infilepaths) == 1
          or args.outfile_path is None), \
      ("if more than one input file is specified, then annotates must be "
       "written to stdout")

  if args.outfile_path is None:
    args.outfile_path = '-'

  if '-' in args.infilepaths:
    assert len(args.infilepaths) == 1, \
        "You cannot mix stdin as an input with other input files"
    assert args.outfile_path == '-', \
        "If stdin is the input file, then stdout must be the output file"

  argdict = __main__.get_argdict(args)
  output_format = argdict.pop("format")

  for infile_path in args.infilepaths:
    # NOTE(josh): have to load config once for every file, because we may pick
    # up a new config file location for each path
    if infile_path == '-':
      config_dict = __main__.get_config(os.getcwd(), args.config_file)
    else:
      config_dict = __main__.get_config(infile_path, args.config_file)
    config_dict.update(argdict)

    cfg = configuration.Configuration(**config_dict)
    if args.outfile_path == '-':
      # NOTE(josh): The behavior or sys.stdout is different in python2 and
      # python3. sys.stdout is opened in 'w' mode which means that write()
      # takes strings in python2 and python3 and, in particular, in python3
      # it does not take byte arrays. io.StreamWriter will write to
      # it with byte arrays (assuming it was opened with 'wb'). So we use
      # io.open instead of open in this case
      outfile = io.open(
          os.dup(sys.stdout.fileno()),
          mode='w', encoding=cfg.encode.output_encoding, newline='')
    else:
      outfile = io.open(
          args.outfile_path, 'w', encoding=cfg.encode.output_encoding,
          newline='')

    if infile_path == '-':
      infile = io.open(
          os.dup(sys.stdin.fileno()),
          mode='r', encoding=cfg.encode.input_encoding, newline='')
    else:
      infile = io.open(infile_path, 'r', encoding=cfg.encode.input_encoding)

    try:
      with infile:
        annotate_file(cfg, infile, outfile, output_format)
    except:
      sys.stderr.write('While processing {}\n'.format(infile_path))
      raise
    finally:
      outfile.close()

  return 0


if __name__ == '__main__':
  sys.exit(main())
