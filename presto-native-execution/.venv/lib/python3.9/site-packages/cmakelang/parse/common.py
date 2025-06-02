# -*- coding: utf-8 -*-
# pylint: disable=too-many-lines
from __future__ import print_function
from __future__ import unicode_literals

import logging

from cmakelang import common
from cmakelang import lex
from cmakelang.parse.util import (
    is_semantic_token, is_syntactic_token, is_whitespace_token, is_comment_token
)

logger = logging.getLogger(__name__)


class NodeType(common.EnumObject):
  """
  Enumeration for AST nodes
  """
  _id_map = {}


NodeType.BODY = NodeType(0)
NodeType.WHITESPACE = NodeType(1)
NodeType.COMMENT = NodeType(2)
NodeType.STATEMENT = NodeType(3)
NodeType.FLOW_CONTROL = NodeType(4)
NodeType.FUNNAME = NodeType(10)
NodeType.ARGGROUP = NodeType(5)
NodeType.KWARGGROUP = NodeType(6)
NodeType.PARGGROUP = NodeType(14)
NodeType.FLAGGROUP = NodeType(15)
NodeType.PARENGROUP = NodeType(16)
NodeType.ARGUMENT = NodeType(7)
NodeType.KEYWORD = NodeType(8)
NodeType.FLAG = NodeType(9)
NodeType.ONOFFSWITCH = NodeType(11)
NodeType.ATWORDSTATEMENT = NodeType(17)
NodeType.ATWORD = NodeType(18)

# NOTE(josh): These aren't really semantic, but they have structural
# significance that is important in formatting. Since they will have a presence
# in the format tree, we give them a presence in the parse tree as well.
NodeType.LPAREN = NodeType(12)
NodeType.RPAREN = NodeType(13)


class FlowType(common.EnumObject):
  """
  Enumeration for flow control types
  """
  _id_map = {}


FlowType.IF = FlowType(0)
FlowType.WHILE = FlowType(1)
FlowType.FOREACH = FlowType(2)
FlowType.FUNCTION = FlowType(3)
FlowType.MACRO = FlowType(4)


class TreeNode(object):
  """
  A node in the full-syntax-tree.
  """

  def __init__(self, node_type):
    self.node_type = node_type
    self.children = []
    self.parent = None

  def build_ancestry(self):
    """Recursively assign the .parent member within the subtree."""
    for child in self.children:
      if isinstance(child, TreeNode):
        child.parent = self
        child.build_ancestry()

  def get_location(self):
    """
    Return the (line, col) of the first token in the subtree rooted at this
    node.
    """
    if self.children:
      return self.children[0].get_location()

    return lex.SourceLocation((0, 0, 0))

  def count_newlines(self):
    newline_count = 0
    for child in self.children:
      newline_count += child.count_newlines()
    return newline_count

  def __repr__(self):
    return '{}: {}'.format(self.__class__.__name__, self.get_location())

  def get_tokens(self, out=None, kind=None):
    if out is None:
      out = []
    if kind is None:
      kind = "all"

    match_group = {
        "semantic": is_semantic_token,
        "syntactic": is_syntactic_token,
        "whitespace": is_whitespace_token,
        "comment": is_comment_token,
        "all": lambda x: True
    }[kind]

    for child in self.children:
      if isinstance(child, lex.Token):
        if match_group(child):
          out.append(child)
      elif isinstance(child, TreeNode):
        child.get_tokens(out, kind)
      else:
        raise RuntimeError("Unexpected child of type {}".format(type(child)))
    return out

  def get_semantic_tokens(self, out=None):
    """
    Recursively reconstruct a stream of semantic tokens
    """
    return self.get_tokens(out, kind="semantic")


class ParenBreaker(object):
  """
  Callable that returns true if the supplied token is a right parenthential
  """

  def __call__(self, token):
    return token.type == lex.TokenType.RIGHT_PAREN


class KwargBreaker(object):
  """
  Callable that returns true if the supplied token is in the list of keywords,
  ignoring case.
  """

  def __init__(self, kwargs):
    self.kwargs = [kwarg.upper() for kwarg in kwargs]

  def __call__(self, token):
    return token.spelling.upper() in self.kwargs

  def __repr__(self):
    return "KwargBreaker({})".format(",".join(self.kwargs))
