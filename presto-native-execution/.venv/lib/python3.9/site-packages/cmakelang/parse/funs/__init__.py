# pylint: disable=too-many-lines
"""
Statement parser functions
"""

import importlib
import logging

from cmakelang import lex
from cmakelang.parse.additional_nodes import ShellCommandNode
from cmakelang.parse.argument_nodes import (
    ConditionalGroupNode, StandardParser, StandardParser2
)
from cmakelang.parse.common import NodeType, KwargBreaker, TreeNode
from cmakelang.parse.util import (
    IMPLICIT_PARG_TYPES,
    WHITESPACE_TOKENS,
    get_first_semantic_token,
    get_normalized_kwarg,
    get_tag,
    should_break,
)
from cmakelang.parse.funs import standard_funs

logger = logging.getLogger(__name__)


def get_funtree(cmdspec):
  kwargs = {}

  for kwarg, subspec in cmdspec.kwargs.items():
    if kwarg in ("if", "elseif", "while"):
      subparser = ConditionalGroupNode.parse
    elif kwarg == "COMMAND":
      subparser = ShellCommandNode.parse
    elif isinstance(subspec, (standard_funs.CommandSpec)):
      subparser = StandardParser2(subspec, get_funtree(subspec))
    else:
      raise ValueError("Unexpected kwarg spec of type {}"
                       .format(type(subspec)))

    kwargs[kwarg] = subparser

  return kwargs


SUBMODULE_NAMES = [
    "add_executable",
    "add_library",
    "add_xxx",
    "deprecated",
    "break",
    "external_project",
    "fetch_content",
    "foreach",
    "file",
    "install",
    "list",
    "miscellaneous",
    "random",
    "set",
    "set_target_properties",
]


def get_parse_db():
  """
  Returns a dictionary mapping statement name to parse functor for that
  statement.
  """

  parse_db = {}

  for subname in SUBMODULE_NAMES:
    submodule = importlib.import_module("cmakelang.parse.funs." + subname)
    submodule.populate_db(parse_db)

  for key in (
      "if", "else", "elseif", "endif", "while", "endwhile"):
    parse_db[key] = ConditionalGroupNode.parse

  for key in ("function", "macro"):
    parse_db[key] = StandardParser("1+")

  for key in ("endfunction", "endmacro"):
    parse_db[key] = StandardParser("?")

  parse_db.update(get_funtree(standard_funs.get_fn_spec()))
  return parse_db
