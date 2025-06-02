# -*- coding: utf-8 -*-
"""
Parse cmake listfiles, find function and macro declarations, and generate
parsers for them.
"""
from __future__ import print_function, unicode_literals

import argparse
import collections
import io
import json
import logging
import os
import pprint
import re
import sys

import cmakelang
from cmakelang.format import __main__
from cmakelang import configuration
from cmakelang import lex
from cmakelang import parse
from cmakelang.parse.body_nodes import BodyNode
from cmakelang.parse.common import TreeNode
from cmakelang.parse.statement_node import StatementNode
from cmakelang.parse.funs.set import SetFnNode

logger = logging.getLogger(__name__)


def find_statements_in_subtree(subtree, funnames):
  """
  Return a generator that yields all statements in the `subtree` which match
  the provided set of `funnames`.
  """
  if isinstance(subtree, (list, tuple)):
    queue = subtree
  else:
    queue = [subtree]

  while queue:
    node = queue.pop(0)

    if isinstance(node, SetFnNode) and "set" in funnames:
      yield node
      continue

    if isinstance(node, StatementNode):
      if node.get_funname() in funnames:
        yield node
        continue

    for child in node.children:
      if isinstance(child, TreeNode):
        queue.append(child)


class VarSub(object):
  def __init__(self, variables):
    self._vars = variables

  def __call__(self, match):
    return self._vars.get(match.group(1), "")


def replace_varrefs(value, variables):
  """
  Replace cmake variable dereferences in the string, given a dictionary of
  currently assigned variables.
  """
  regex = re.compile(r"\$\{([\w_]+)\}")
  repl = VarSub(variables)
  while regex.search(value):
    value = regex.sub(repl, value)
  return value


def process_set_statement(argtree, variables):
  """
  Process a set() statement, updating the variable assignments accordingly
  """
  varname = replace_varrefs(argtree.varname.spelling, variables)
  if not argtree.value_group:
    variables.pop(varname, None)
    return

  setargs = argtree.value_group.get_tokens(kind="semantic")
  valuestr = ";".join(arg.spelling.strip('"') for arg in setargs)
  variables[varname] = replace_varrefs(valuestr, variables)


def process_defn_body(body, out):
  assert isinstance(body, BodyNode)
  variables = {}
  for child in find_statements_in_subtree(
      body, ["list", "cmake_parse_arguments", "set"]):

    if isinstance(child, StatementNode):
      if child.get_funname() == "set":
        process_set_statement(child.argtree, variables)
        continue

      if child.get_funname() == "list":
        # TODO(josh): implement list processor
        continue

      if child.get_funname() == "cmake_parse_arguments":
        pargs = child.argtree.get_tokens(kind="semantic")
        pargs.pop(0)
        kwvargs = replace_varrefs(
            pargs.pop(0).spelling.strip('"'), variables).split(";")
        onevargs = replace_varrefs(
            pargs.pop(0).spelling.strip('"'), variables).split(";")
        multivargs = replace_varrefs(
            pargs.pop(0).spelling.strip('"'), variables).split(";")

        pargs = out["pargs"]
        nargs = pargs["nargs"]
        if nargs == 0:
          pargs["nargs"] = "*"
        else:
          pargs["nargs"] = "{}+".format(nargs)

        pargs["flags"] = flags = []
        for flag in kwvargs:
          flag = flag.strip().upper()
          if flag:
            flags.append(flag)

        out["kwargs"] = kwargs = {}

        for kwarg in onevargs:
          kwarg = kwarg.strip().upper()
          if kwarg:
            kwargs[kwarg] = 1
        for kwarg in multivargs:
          kwarg = kwarg.strip().upper()
          if kwarg:
            kwargs[kwarg] = "+"
        return
  return


def process_defn(statement):
  """
  Process one function or macro definition
  """
  semantic_tokens = statement.argtree.parg_groups[0].get_tokens(kind="semantic")

  canonical_spelling = semantic_tokens[0].spelling
  fnname = canonical_spelling.lower()
  argnames = [token.spelling.lower() for token in semantic_tokens[1:]]

  out = {
      "pargs": {
          "nargs": len(argnames)
      }
  }

  if (canonical_spelling.lower() != canonical_spelling and
      canonical_spelling.upper() != canonical_spelling):
    # mixed case
    out["spelling"] = canonical_spelling

  process_defn_body(statement.parent.children[1], out)
  return fnname, out


def process_tree(parse_tree):
  """
  Process the resulting parse tree
  """
  out = collections.OrderedDict()
  for statement in find_statements_in_subtree(
      parse_tree, ("function", "macro")):
    fnname, spec = process_defn(statement)
    if fnname.startswith("_"):
      continue
    out[fnname] = spec
  return out


def process_file(config, infile_content):
  """
  Parse the input cmake file, return the parse tree
  """

  if config.format.line_ending == 'auto':
    detected = __main__.detect_line_endings(infile_content)
    config = config.clone()
    config.set_line_ending(detected)

  tokens = lex.tokenize(infile_content)
  parse_db = parse.funs.get_parse_db()
  parse_db.update(parse.funs.get_funtree(config.parse.fn_spec))
  ctx = parse.ParseContext(parse_db, config=config)
  parse_tree = parse.parse(tokens, ctx)
  parse_tree.build_ancestry()
  return parse_tree


def setup_argparse(argparser):
  argparser.add_argument('-v', '--version', action='version',
                         version=cmakelang.__version__)
  argparser.add_argument(
      '-l', '--log-level', default="info",
      choices=["error", "warning", "info", "debug"])
  argparser.add_argument(
      '-o', '--outfile-path', default=None,
      help='Write results to this file. Default is stdout.')
  argparser.add_argument(
      '-f', '--output-format', default="python",
      choices=["json", "yaml", "python"])
  argparser.add_argument('infilepaths', nargs='*')


USAGE_STRING = """
cmake-genparsers [-h] [-o OUTFILE_PATH] infilepath [infilepath ...]
"""


def main():
  """Parse arguments, open files, start work."""
  logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

  argparser = argparse.ArgumentParser(
      description=__doc__,
      formatter_class=argparse.RawDescriptionHelpFormatter,
      usage=USAGE_STRING)

  setup_argparse(argparser)
  args = argparser.parse_args()
  logging.getLogger().setLevel(getattr(logging, args.log_level.upper()))

  if '-' in args.infilepaths:
    assert len(args.infilepaths) == 1, \
        "You cannot mix stdin as an input with other input files"

  if args.outfile_path is None:
    args.outfile_path = '-'

  outfile_arg = args.outfile_path
  if outfile_arg == '-':
    outfile_arg = os.dup(sys.stdout.fileno())

  returncode = 0
  outdict = collections.OrderedDict()
  for infile_path in args.infilepaths:
    print(infile_path)
    cfg = configuration.Configuration()
    if infile_path == '-':
      infile_path = os.dup(sys.stdin.fileno())

    try:
      infile = io.open(
          infile_path, mode='r', encoding=cfg.encode.input_encoding, newline='')
    except (IOError, OSError):
      logger.error("Failed to open %s for read", infile_path)
      returncode = 1

    try:
      with infile:
        intext = infile.read()
    except UnicodeDecodeError:
      logger.error(
          "Unable to read %s as %s", infile_path, cfg.encode.input_encoding)

    try:
      parse_tree = process_file(cfg, intext)
      cmd_spec = process_tree(parse_tree)
      outdict.update(cmd_spec)
    except:
      logger.warning('While processing %s', infile_path)
      raise

  outfile = io.open(outfile_arg, mode='w', encoding="utf-8", newline='')
  if args.output_format == "json":
    json.dump(outdict, outfile, indent=2)
  elif args.output_format == "yaml":
    import yaml
    __main__.yaml_register_odict(yaml.SafeDumper)
    __main__.yaml_register_odict(yaml.Dumper)
    yaml.dump(outdict,
              outfile, indent=2,
              default_flow_style=False, sort_keys=False)
  elif args.output_format == "python":
    ppr = pprint.PrettyPrinter(indent=2, width=80)
    outfile.write(ppr.pformat(dict(outdict)))
  else:
    logger.error("Unrecognized output format {}".format(args.output_format))
  outfile.write("\n")
  outfile.close()
  return returncode


if __name__ == "__main__":
  sys.exit(main())
