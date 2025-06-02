# -*- coding: utf-8 -*-
# PYTHON_ARGCOMPLETE_OK
"""
Check cmake listfile for lint
"""
from __future__ import unicode_literals

import argparse
import io
import logging
import os
import sys

import cmakelang
from cmakelang import common
from cmakelang.format import __main__
from cmakelang import configuration
from cmakelang import lex
from cmakelang import parse

from cmakelang.lint import basic_checker
from cmakelang.lint import lint_util

logger = logging.getLogger(__name__)


def process_file(config, local_ctx, infile_content):
  """
  Parse the input cmake file, re-format it, and print to the output file.
  """

  if config.format.line_ending == 'auto':
    detected = __main__.detect_line_endings(infile_content)
    config = config.clone()
    config.set_line_ending(detected)

  checker = basic_checker.LintChecker(config, local_ctx)
  checker.check_basics(infile_content)
  tokens = lex.tokenize(infile_content)
  checker.check_tokens(tokens)

  parse_db = parse.funs.get_parse_db()
  parse_db.update(parse.funs.get_funtree(config.parse.fn_spec))
  ctx = parse.ParseContext(parse_db, local_ctx, config)
  parse_tree = parse.parse(tokens, ctx)
  parse_tree.build_ancestry()
  checker.check_parse_tree(parse_tree)


def setup_argparse(argparser):
  argparser.add_argument('-v', '--version', action='version',
                         version=cmakelang.__version__)
  argparser.add_argument(
      '-l', '--log-level', default="info",
      choices=["error", "warning", "info", "debug"])

  mutex = argparser.add_mutually_exclusive_group()
  mutex.add_argument('--dump-config', choices=['yaml', 'json', 'python'],
                     default=None, const='python', nargs='?',
                     help='If specified, print the default configuration to '
                          'stdout and exit')
  mutex.add_argument('-o', '--outfile-path', default=None,
                     help='Write errors to this file. '
                          'Default is stdout.')

  argparser.add_argument(
      "--no-help", action="store_false", dest="with_help",
      help="When used with --dump-config, will omit helptext comments in the"
           " output"
  )
  argparser.add_argument(
      "--no-default", action="store_false", dest="with_defaults",
      help="When used with --dump-config, will omit any unmodified "
           "configuration value."
  )

  argparser.add_argument(
      "--suppress-decorations", action="store_true",
      help="Suppress the file title decoration and summary statistics")

  argparser.add_argument(
      '-c', '--config-files', nargs='+',
      help='path to configuration file(s)')
  argparser.add_argument('infilepaths', nargs='*')

  configuration.Configuration().add_to_argparser(argparser)


USAGE_STRING = """
cmake-lint [-h]
           [--dump-config {yaml,json,python} | -o OUTFILE_PATH]
           [-c CONFIG_FILE]
           infilepath [infilepath ...]
"""


def inner_main():
  """Parse arguments, open files, start work."""
  logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

  argparser = argparse.ArgumentParser(
      description=__doc__,
      formatter_class=argparse.RawDescriptionHelpFormatter,
      usage=USAGE_STRING)

  setup_argparse(argparser)
  try:
    import argcomplete
    argcomplete.autocomplete(argparser)
  except ImportError:
    pass
  args = argparser.parse_args()
  logging.getLogger().setLevel(getattr(logging, args.log_level.upper()))

  if args.dump_config:
    config_dict = __main__.get_config(os.getcwd(), args.config_files)
    __main__.dump_config(args, config_dict, sys.stdout)
    sys.exit(0)

  if args.outfile_path is None:
    args.outfile_path = '-'

  if '-' in args.infilepaths:
    assert len(args.infilepaths) == 1, \
        "You cannot mix stdin as an input with other input files"

  if args.outfile_path == '-':
    outfile = io.open(os.dup(sys.stdout.fileno()),
                      mode='w', encoding="utf-8", newline='')
  else:
    outfile = io.open(args.outfile_path, 'w', encoding="utf-8", newline='')

  global_ctx = lint_util.GlobalContext(outfile)
  returncode = 0
  argdict = __main__.get_argdict(args)

  for infile_path in args.infilepaths:
    # NOTE(josh): have to load config once for every file, because we may pick
    # up a new config file location for each path
    if infile_path == '-':
      config_dict = __main__.get_config(os.getcwd(), args.config_files)
    else:
      config_dict = __main__.get_config(infile_path, args.config_files)
    config_dict.update(argdict)

    cfg = configuration.Configuration(**config_dict)
    if infile_path == '-':
      infile_path = os.dup(sys.stdin.fileno())

    try:
      infile = io.open(
          infile_path, mode='r', encoding=cfg.encode.input_encoding, newline='')
    except (IOError, OSError):
      logger.error("Failed to open %s for read", infile_path)
      returncode = 1
      continue

    try:
      with infile:
        intext = infile.read()
    except UnicodeDecodeError:
      logger.error(
          "Unable to read %s as %s", infile_path, cfg.encode.input_encoding)
      returncode = 1
      continue

    local_ctx = global_ctx.get_file_ctx(infile_path, cfg)
    process_file(cfg, local_ctx, intext)
    if not args.suppress_decorations:
      outfile.write("{}\n{}\n".format(infile_path, "=" * len(infile_path)))
    local_ctx.writeout(outfile)
    if not args.suppress_decorations:
      outfile.write("\n")
    if local_ctx.has_lint():
      returncode = 1

  if not args.suppress_decorations:
    global_ctx.write_summary(outfile)
  outfile.close()
  return returncode


def main():
  try:
    return inner_main()
  except SystemExit:
    return 0
  except common.UserError as ex:
    logger.fatal(ex.msg)
    return 1
  except common.InternalError as ex:
    logger.exception(ex.msg)
    return 2
  except AssertionError as ex:
    logger.exception(
        "An internal error occured. Please consider filing a bug report at "
        "github.com/cheshirekow/cmakelang/issues")
    return 2
  except:  # pylint: disable=bare-except
    logger.exception(
        "An internal error occured. Please consider filing a bug report at "
        "github.com/cheshirekow/cmakelang/issues")
    return 2


if __name__ == "__main__":
  sys.exit(main())
