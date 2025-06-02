# -*- coding: utf-8 -*-
# pylint: disable=too-many-lines
from __future__ import print_function
from __future__ import unicode_literals

import collections
import logging

from cmakelang.common import InternalError
from cmakelang import lex
from cmakelang.parse.util import (
    COMMENT_TOKENS, ONOFF_TOKENS, WHITESPACE_TOKENS
)
from cmakelang.parse.common import (
    FlowType, NodeType, TreeNode
)
from cmakelang.parse.simple_nodes import (
    CommentNode, OnOffNode, WhitespaceNode
)
from cmakelang.parse.statement_node import (
    AtWordStatementNode, StatementNode
)

logger = logging.getLogger(__name__)


class BodyNode(TreeNode):
  """Top level scope node contains all block-level nodes."""

  def __init__(self):
    super(BodyNode, self).__init__(NodeType.BODY)

  @classmethod
  def consume(cls, ctx, tokens, breakset=None):
    """
    Consume tokens and return a tree of nodes. Top-level consumer parsers
    comments, whitespace, statements, and flow control blocks.
    """
    if breakset is None:
      breakset = ()

    tree = cls()
    blocks = tree.children

    while tokens:
      token = tokens[0]
      if token.type in WHITESPACE_TOKENS:
        node = WhitespaceNode.consume(ctx, tokens)
        blocks.append(node)
      elif token.type in COMMENT_TOKENS:
        node = CommentNode.consume(ctx, tokens)
        blocks.append(node)
      elif token.type in ONOFF_TOKENS:
        node = OnOffNode.consume(ctx, tokens)
        blocks.append(node)
      elif token.type == lex.TokenType.BRACKET_COMMENT:
        node = CommentNode.consume(ctx, tokens)
        blocks.append(node)
      elif token.type == lex.TokenType.WORD:
        upper = token.spelling.upper()
        if upper in breakset:
          return tree
        if FlowType.get(upper) is not None:
          subtree = FlowControlNode.consume(ctx, tokens)
          blocks.append(subtree)
        else:
          subtree = StatementNode.consume(ctx, tokens)
          blocks.append(subtree)
      elif token.type == lex.TokenType.ATWORD:
        subtree = AtWordStatementNode.consume(ctx, tokens)
        blocks.append(subtree)
      elif token.type == lex.TokenType.BYTEORDER_MARK:
        tokens.pop(0)
      else:
        raise InternalError(
            "Unexpected {} token at {}:{}"
            .format(tokens[0].type.name,
                    tokens[0].begin.line, tokens[0].begin.col))

    return tree


FlowControlBlock = collections.namedtuple(
    "FlowControlBlock", ["open_stmt", "body", "close_stmt"])


class FlowControlNode(TreeNode):
  """Parent node of a flow-control statement set, including at least one opening
     statement, a body, and a closing statement. if() tree's may contain
     multiple opening statement and body pairs.
  """

  def __init__(self):
    super(FlowControlNode, self).__init__(NodeType.FLOW_CONTROL)
    self.blocks = []

  def get_block_with(self, node):
    """Return the block which contains the given node"""
    for block in self.blocks:
      if node in block:
        return block
    return None

  @classmethod
  def consume(cls, ctx, tokens):
    """
    Consume tokens and return a flow control tree. A flow control tree starts
    and ends with a statement node, and contains one or more body nodes. An if
    statement is the only flow control that includes multiple body nodes and
    they are separated by either else or elseif nodes.
    """
    flowtype = FlowType.from_name(tokens[0].spelling.upper())
    if flowtype is FlowType.IF:
      return IfBlockNode.consume(ctx, tokens)

    node = cls()
    endflow = 'END' + flowtype.name

    open_stmt = StatementNode.consume(ctx, tokens)
    node.children.append(open_stmt)
    body = BodyNode.consume(ctx, tokens, (endflow,))
    node.children.append(body)
    close_stmt = StatementNode.consume(ctx, tokens)
    node.children.append(close_stmt)

    node.blocks.append(FlowControlBlock(open_stmt, body, close_stmt))
    return node


class IfBlockNode(FlowControlNode):
  """IfBlocks are different than other `FlowControlNode` s in that they can
     contain multiple bodies.
  """

  @classmethod
  def consume(cls, ctx, tokens):
    """
    Consume tokens and return a flow control tree. ``IF`` statements are special
    because they have interior ``ELSIF`` and ``ELSE`` blocks, while all other
    flow control have a single body.
    """

    tree = cls()
    breakset = ("ELSE", "ELSEIF", "ENDIF")

    prev_open_stmt = None
    prev_body = None
    while tokens and tokens[0].spelling.upper() != "ENDIF":
      open_stmt = StatementNode.consume(ctx, tokens)
      body = BodyNode.consume(ctx, tokens, breakset)
      tree.children.append(open_stmt)
      tree.children.append(body)
      if prev_open_stmt is not None:
        tree.blocks.append(
            FlowControlBlock(prev_open_stmt, prev_body, open_stmt))

      prev_open_stmt = open_stmt
      prev_body = body

    if tokens:
      close_stmt = StatementNode.consume(ctx, tokens)
      tree.children.append(close_stmt)
      tree.blocks.append(
          FlowControlBlock(prev_open_stmt, prev_body, close_stmt))

    return tree
