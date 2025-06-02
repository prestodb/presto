# -*- coding: utf-8 -*-
# PYTHON_ARGCOMPLETE_OK
"""
Parse cmake listfiles and format them nicely.

Formatting is configurable by providing a configuration file. The configuration
file can be in json, yaml, or python format. If no configuration file is
specified on the command line, cmake-format will attempt to find a suitable
configuration for each ``inputpath`` by checking recursively checking it's
parent directory up to the root of the filesystem. It will return the first
file it finds with a filename that matches '\\.?cmake-format(.yaml|.json|.py)'.

cmake-format can spit out the default configuration for you as starting point
for customization. Run with `--dump-config [yaml|json|python]`.
"""
from __future__ import unicode_literals

import argparse
import collections
try:
  from collections.abc import Mapping
except ImportError:
  from collections import Mapping
import io
import json
import logging
import os
import shutil
import sys

import cmakelang
from cmakelang import common
from cmakelang import configuration
from cmakelang import config_util
from cmakelang.format import formatter
from cmakelang import lex
from cmakelang import markup
from cmakelang import parse
from cmakelang.parse.argument_nodes import StandardParser2
from cmakelang.parse.common import NodeType, TreeNode
from cmakelang.parse.printer import dump_tree as dump_parse
from cmakelang.parse.funs import standard_funs


logger = logging.getLogger(__name__)


def detect_line_endings(infile_content):
  windows_count = infile_content.count('\r\n')
  unix_count = infile_content.count('\n') - windows_count
  if windows_count > unix_count:
    return 'windows'

  return 'unix'


def dump_markup(nodes, config, outfile=None, indent=None):
  """
  Print a tree of node objects for debugging purposes. Takes as input a full
  parse tree.
  """

  if indent is None:
    indent = ''

  if outfile is None:
    outfile = sys.stdout

  for idx, node in enumerate(nodes):
    if not isinstance(node, TreeNode):
      continue

    # outfile.write(indent)
    # if idx + 1 == len(nodes):
    #   outfile.write('└─ ')
    # else:
    #   outfile.write('├─ ')

    noderep = repr(node)
    if sys.version_info[0] < 3:
      noderep = getattr(noderep, 'decode')('utf-8')

    if node.node_type is NodeType.COMMENT:
      outfile.write(noderep)
      outfile.write('\n')
      inlines = []
      for token in node.children:
        assert isinstance(token, lex.Token)
        if token.type == lex.TokenType.COMMENT:
          inlines.append(token.spelling.strip().lstrip('#'))
      items = markup.parse(inlines, config)
      for item in items:
        outfile.write("{}\n".format(item))
      outfile.write("\n")

    if not hasattr(node, 'children'):
      continue

    if idx + 1 == len(nodes):
      dump_markup(node.children, config, outfile, indent + '    ')
    else:
      dump_markup(node.children, config, outfile, indent + '│   ')


def dump_parsedb(parsedb, outfile=None, indent=None):
  """
  Dump the parse database to a file
  """

  if indent is None:
    indent = ''

  if outfile is None:
    outfile = sys.stdout

  items = list(sorted(parsedb.items()))
  for idx, (name, value) in enumerate(items):
    outfile.write(indent)
    if idx + 1 == len(items):
      outfile.write('└─ ')
      subindent = indent + "   "
    else:
      outfile.write('├─ ')
      subindent = indent + "|  "

    outfile.write(name)
    if isinstance(value, StandardParser2):
      outfile.write(": {}\n".format(repr(value.cmdspec.pargs)))
      dump_parsedb(value.funtree, outfile, subindent)
    else:
      outfile.write(": {}\n".format(type(value)))


def process_file(config, infile_content, dump=None):
  """
  Parse the input cmake file, re-format it, and print to the output file.
  """

  outfile = io.StringIO(newline='')
  if config.format.line_ending == 'auto':
    detected = detect_line_endings(infile_content)
    config = config.clone()
    config.format.set_line_ending(detected)
  tokens = lex.tokenize(infile_content)
  if dump == "lex":
    for token in tokens:
      outfile.write("{}\n".format(token))
    return outfile.getvalue(), True
  first_token = lex.get_first_non_whitespace_token(tokens)
  parse_db = parse.funs.get_parse_db()
  parse_db.update(parse.funs.get_funtree(config.parse.fn_spec))

  if dump == "parsedb":
    dump_parsedb(parse_db, outfile)
    return outfile.getvalue(), True

  ctx = parse.ParseContext(parse_db, config=config)
  parse_tree = parse.parse(tokens, ctx)
  if dump == "parse":
    dump_parse([parse_tree], outfile)
    return outfile.getvalue(), True
  if dump == "markup":
    dump_markup([parse_tree], config, outfile)
    return outfile.getvalue(), True

  box_tree = formatter.layout_tree(parse_tree, config, first_token=first_token)
  if dump == "layout":
    formatter.dump_tree([box_tree], outfile)
    return outfile.getvalue(), True

  outstr = formatter.write_tree(box_tree, config, infile_content)
  if config.encode.emit_byteorder_mark:
    outstr = "\ufeff" + outstr

  outstr = formatter.replace_with_tabs(outstr, config)
  return (outstr, box_tree.reflow_valid)


def find_config_file(infile_path):
  """
  Search parent directories of an infile path and find a config file if
  one exists.
  """
  realpath = os.path.realpath(infile_path)
  if os.path.isdir(infile_path):
    head = infile_path
  else:
    head, _ = os.path.split(realpath)

  while head:
    for filename in ['.cmake-format',
                     '.cmake-format.py',
                     '.cmake-format.json',
                     '.cmake-format.yaml',
                     'cmake-format.py',
                     'cmake-format.json',
                     'cmake-format.yaml', ]:
      configpath = os.path.join(head, filename)
      if os.path.exists(configpath):
        return configpath
    head2, _ = os.path.split(head)
    if head == head2:
      break
    head = head2
  return None


def load_yaml(config_file):
  """
  Attempt to load yaml configuration from an opened file
  """
  import yaml
  try:
    from yaml import CLoader as Loader
  except ImportError:
    from yaml import Loader
  out = yaml.load(config_file, Loader=Loader)
  if out is None:
    return {}
  return out


def exec_pyconfig(configfile_path):
  _global = config_util.ExecGlobal(configfile_path)
  with io.open(configfile_path, 'r', encoding='utf-8') as infile:
    # pylint: disable=exec-used
    exec(infile.read(), _global)
  _global.pop("__builtins__", None)
  return _global


def try_get_configdict(configfile_path):
  """
  Try to read the configuration as yaml first, then json, then python.

  TODO(josh): maybe read the first line and look for some kind of comment
  sentinel to indicate the file format?
  """
  try:
    with io.open(configfile_path, 'r', encoding='utf-8') as config_file:
      return json.load(config_file)
  except:  # pylint: disable=bare-except
    pass

  try:
    with io.open(configfile_path, 'r', encoding='utf-8') as config_file:
      return load_yaml(config_file)
  except:  # pylint: disable=bare-except
    pass

  try:
    return exec_pyconfig(configfile_path)
  except:  # pylint: disable=bare-except
    pass

  raise RuntimeError("Failed to parse {} as any of yaml, json, or python"
                     .format(configfile_path))


def get_one_config_dict(configfile_path):
  """
  Return a dictionary of configuration options read from the given file path.
  If the filepath has a known extension then we parse it according to that
  extension. Otherwise we try to parse is using each parser one by one.
  """
  if not os.path.exists(configfile_path):
    raise common.UserError(
        "Desired config file does not exist: {}".format(configfile_path))

  if configfile_path.endswith('.json'):
    # NOTE(josh): an empty file is not valid JSON, but we don't need to be as
    # pedantic
    if os.stat(configfile_path).st_size == 0:
      return {}
    with io.open(configfile_path, 'r', encoding='utf-8') as config_file:
      try:
        return json.load(config_file)
      except ValueError as ex:
        message = (
            "Failed to parse json config file {}: {}"
            .format(configfile_path, ex))
        raise common.UserError(message)

  if configfile_path.endswith('.yaml'):
    with io.open(configfile_path, 'r', encoding='utf-8') as config_file:
      try:
        return load_yaml(config_file)
      except ValueError as ex:
        message = (
            "Failed to parse yaml config file {}: {}"
            .format(configfile_path, ex))
        raise common.UserError(message)

  if configfile_path.endswith('.py'):
    try:
      return exec_pyconfig(configfile_path)
    except Exception as ex:
      message = (
          "Failed to parse python config file {}: {}"
          .format(configfile_path, ex))
      raise common.UserError(message)

  return try_get_configdict(configfile_path)


def get_configdict(configfile_paths):
  include_queue = list(configfile_paths)
  config_dict = {}
  while include_queue:
    configfile_path = include_queue.pop(0)
    configfile_path = os.path.expanduser(configfile_path)
    increment_dict = get_one_config_dict(configfile_path)

    for include_path in increment_dict.pop("include", []):
      if not os.path.isabs(include_path):
        include_path = os.path.join(
            os.path.dirname(configfile_path), include_path)
      include_queue.append(include_path)
    map_merge(config_dict, increment_dict)
  return config_dict


def map_merge(output_map, increment_map):
  """
  Merge `increment_map` into `output_map` recursively.
  """
  for key, increment_value in increment_map.items():
    if key not in output_map:
      output_map[key] = increment_value
      continue

    existing_value = output_map[key]
    if isinstance(existing_value, Mapping):
      if isinstance(increment_value, Mapping):
        map_merge(existing_value, increment_value)
      else:
        logger.warning(
            "Cannot merge config %s of type %s into a dictionary",
            key, type(increment_value))
      continue

    output_map[key] = increment_value

  return output_map


def get_config(infile_path, configfile_paths):
  """
  If configfile_path is not none, then load the configuration. Otherwise search
  for a config file in the ancestry of the filesystem of infile_path and find
  a config file to load.
  """
  if configfile_paths is None:
    inferred_configpath = find_config_file(infile_path)
    if inferred_configpath is None:
      return {}
    configfile_paths = [inferred_configpath]

  return get_configdict(configfile_paths)


def yaml_odict_handler(dumper, value):
  """
  Represent ordered dictionaries as yaml maps.
  """
  return dumper.represent_mapping(u'tag:yaml.org,2002:map', value)


def yaml_register_odict(dumper):
  """
  Register an order dictionary handler with the given yaml dumper
  """
  dumper.add_representer(collections.OrderedDict, yaml_odict_handler)


def dump_config(args, config_dict, outfile):
  """
  Dump the default configuration to stdout
  """

  outfmt = args.dump_config
  config_dict.update(get_argdict(args))
  cfg = configuration.Configuration(**config_dict)
  # Don't dump default per-command configs
  for key in standard_funs.get_default_config():
    cfg.misc.per_command.pop(key, None)

  if outfmt == 'yaml':
    import yaml
    yaml_register_odict(yaml.SafeDumper)
    yaml_register_odict(yaml.Dumper)
    yaml.dump(cfg.as_odict(args.with_help, args.with_defaults),
              outfile, indent=2,
              default_flow_style=False, sort_keys=False)
    return
  if outfmt == 'json':
    json.dump(cfg.as_odict(args.with_help, args.with_defaults),
              outfile, indent=2)
    outfile.write('\n')
    return

  cfg.dump(outfile, with_help=args.with_help, with_defaults=args.with_defaults)


USAGE_STRING = """
cmake-format [-h]
             [--dump-config {yaml,json,python} | -i | -o OUTFILE_PATH]
             [-c CONFIG_FILE]
             infilepath [infilepath ...]
"""


class ExtendAction(argparse.Action):
  def __call__(self, parser, namespace, values, option_string=None):
    items = getattr(namespace, self.dest) or []
    items.extend(values)
    setattr(namespace, self.dest, items)


def setup_argparser(argparser):
  """
  Add argparse options to the parser.
  """
  argparser.register('action', 'extend', ExtendAction)
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
  mutex.add_argument(
      '--dump', choices=['lex', 'parse', 'parsedb', 'layout', 'markup'],
      default=None)

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

  mutex = argparser.add_mutually_exclusive_group()
  mutex.add_argument('-i', '--in-place', action='store_true')
  mutex.add_argument(
      '--check', action='store_true',
      help="Exit with status code 0 if formatting would not change file "
           "contents, or status code 1 if it would")
  mutex.add_argument('-o', '--outfile-path', default=None,
                     help='Where to write the formatted file. '
                          'Default is stdout.')

  argparser.add_argument(
      '-c', '--config-files', nargs='+', action='extend',
      help='path to configuration file(s)')
  argparser.add_argument('infilepaths', nargs='*')

  configuration.Configuration().add_to_argparser(argparser)


def get_argdict(args):
  """Return a dictionary representation of the argparser `namespace` object
     returned from parse_args(). The returned dictionary will be suitable
     as a configuration kwargs dict. Any command line options that aren't
     configuration options are removed."""
  out = {}
  for key, value in vars(args).items():
    if key.startswith("_"):
      continue
    if hasattr(configuration.Configuration, key):
      continue
    # Remove common command line arguments
    if key in ["log_level", "outfile_path", "infilepaths", "config_files"]:
      continue
    # Remove --dump-config command line arguments
    if key in ["dump_config", "with_help", "with_defaults"]:
      continue
    # Remove cmake-format command line arguments
    if key in ["dump", "check", "in_place"]:
      continue
    # Remove cmake-lint command line arguments
    if key in ["suppress_decorations"]:
      continue
    if value is None:
      continue
    out[key] = value

  return out


def onefile_main(infile_path, args, argparse_dict):
  """
  Find config, open file, process, write result
  """
  # NOTE(josh): have to load config once for every file, because we may pick
  # up a new config file location for each path
  if infile_path == '-':
    config_dict = get_config(os.getcwd(), args.config_files)
  else:
    config_dict = get_config(infile_path, args.config_files)

  cfg = configuration.Configuration(**config_dict)
  cfg.legacy_consume(argparse_dict)

  if cfg.format.disable:
    return

  if infile_path == '-':
    infile = io.open(os.dup(sys.stdin.fileno()),
                     mode='r', encoding=cfg.encode.input_encoding, newline='')
  else:
    infile = io.open(
        infile_path, 'r', encoding=cfg.encode.input_encoding, newline='')
  with infile:
    intext = infile.read()

  try:
    outtext, reflow_valid = process_file(cfg, intext, args.dump)
    if cfg.format.require_valid_layout and not reflow_valid:
      raise common.FormatError("Failed to format {}".format(infile_path))
  except:
    logger.warning('While processing %s', infile_path)
    raise

  if args.check:
    if intext != outtext:
      raise common.FormatError("Check failed: {}".format(infile_path))
    return

  if args.in_place:
    if intext == outtext:
      logger.debug("No delta for %s", infile_path)
      return
    tempfile_path = infile_path + ".cmf-temp"
    outfile = io.open(
        tempfile_path, 'w', encoding=cfg.encode.output_encoding, newline='')
  else:
    if args.outfile_path == '-':
      # NOTE(josh): The behavior of sys.stdout is different in python2 and
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

  with outfile:
    outfile.write(outtext)

  if args.in_place:
    shutil.copymode(infile_path, tempfile_path)
    shutil.move(tempfile_path, infile_path)


def inner_main():
  """Parse arguments, open files, start work."""

  arg_parser = argparse.ArgumentParser(
      description=__doc__,
      formatter_class=argparse.RawDescriptionHelpFormatter,
      usage=USAGE_STRING)

  setup_argparser(arg_parser)
  try:
    import argcomplete
    argcomplete.autocomplete(arg_parser)
  except ImportError:
    pass
  args = arg_parser.parse_args()
  logging.getLogger().setLevel(getattr(logging, args.log_level.upper()))

  if args.dump_config:
    config_dict = get_config(os.getcwd(), args.config_files)
    dump_config(args, config_dict, sys.stdout)
    sys.exit(0)

  if args.dump_config or args.dump:
    assert not args.in_place, \
        ("-i/--in-place not allowed when dumping")

  assert args.in_place is False or args.outfile_path is None, \
      "if inplace is specified than outfile is invalid"
  assert (len(args.infilepaths) == 1
          or (args.in_place is True or args.outfile_path is None)), \
      ("if more than one input file is specified, then formatting must be done"
       " in-place or written to stdout")

  if args.outfile_path is None:
    args.outfile_path = '-'

  if '-' in args.infilepaths:
    assert len(args.infilepaths) == 1, \
        "You cannot mix stdin as an input with other input files"
    assert args.outfile_path == '-', \
        "If stdin is the input file, then stdout must be the output file"

  argparse_dict = get_argdict(args)

  returncode = 0
  for infile_path in args.infilepaths:
    try:
      onefile_main(infile_path, args, argparse_dict)
    except common.FormatError as ex:
      logger.error(ex.msg)
      returncode = 1

  return returncode


def main():
  # set up main logger, which logs everything. We'll leave this one logging
  # to the console
  format_str = '%(levelname)-4s %(filename)s:%(lineno)-3s: %(message)s'
  logging.basicConfig(level=logging.INFO,
                      format=format_str,
                      filemode='w')

  try:
    return inner_main()
  except common.UserError as ex:
    logger.fatal(ex.msg)
    return 1
  except common.InternalError as ex:
    logger.exception(ex.msg)
    return 1
  except AssertionError as ex:
    logger.exception(
        "An internal error occured. Please consider filing a bug report at "
        "github.com/cheshirekow/cmakelang/issues")
    return 1


if __name__ == '__main__':
  sys.exit(main())
