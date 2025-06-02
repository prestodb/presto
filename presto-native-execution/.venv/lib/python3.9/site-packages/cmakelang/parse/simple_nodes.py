# -*- coding: utf-8 -*-
# pylint: disable=W0613
from __future__ import print_function
from __future__ import unicode_literals

import logging

from cmakelang import lex
from cmakelang.parse.util import (
    COMMENT_TOKENS, WHITESPACE_TOKENS,
    are_column_aligned, next_is_trailing_comment,
    next_is_explicit_trailing_comment,
    is_valid_trailing_comment
)
from cmakelang.parse.common import NodeType, TreeNode

logger = logging.getLogger(__name__)


class WhitespaceNode(TreeNode):
  """Stores a sequence of whitespace tokens at body scope."""

  def __init__(self):
    super(WhitespaceNode, self).__init__(NodeType.WHITESPACE)

  @classmethod
  def consume(cls, ctx, tokens):
    """
    Consume sequential whitespace, removing tokens from the input list and
    returning a whitespace BlockNode
    """
    node = cls()
    while tokens and tokens[0].type in WHITESPACE_TOKENS:
      node.children.append(tokens.pop(0))
    return node


class CommentNode(TreeNode):
  """Stores a sequence of one or more comment tokens separated by at most
     one newline. These comment tokens together form one semantic comment."""

  def __init__(self):
    super(CommentNode, self).__init__(NodeType.COMMENT)
    self.is_explicit_trailing = False
    self.is_implicit_trailing = False

  def __repr__(self):
    base = super(CommentNode, self).__repr__()
    if self.is_explicit_trailing:
      return base + "(explicit trailing)"
    if self.is_implicit_trailing:
      return base + "(implicit trailing)"

    return base

  @classmethod
  def consume(cls, ctx, tokens):
    """
    Consume sequential comment lines, removing tokens from the input list and
    returning a comment Block
    """
    len_before = len(tokens)

    node = cls()
    if tokens[0].type in (
        lex.TokenType.BRACKET_COMMENT,
        lex.TokenType.FORMAT_OFF,
        lex.TokenType.FORMAT_ON):
      # Bracket comments get their own node because they are capable of
      # globbing up their newlines. Thus they are their own semantic comment,
      # and are not part of any larger semantic structures.
      node.children.append(tokens.pop(0))
      return node

    comment_tokens = []
    while tokens and tokens[0].type in COMMENT_TOKENS:
      # If the next comment token is not column-aligned, then we don't
      # merge it with the previous comment line
      if (comment_tokens and
          not are_column_aligned(comment_tokens[-1], tokens[0])):
        break

      comment_token = tokens.pop(0)
      comment_tokens.append(comment_token)
      node.children.append(comment_token)

      # Multiple comments separated by only one newline are joined together into
      # a single block
      if (len(tokens) > 1
          # pylint: disable=bad-continuation
          and tokens[0].type == lex.TokenType.NEWLINE
          and tokens[1].type in COMMENT_TOKENS):
        node.children.append(tokens.pop(0))

      # Multiple comments separated only by one newline and some whitespace are
      # joined together into a single block
      elif (len(tokens) > 2
            # pylint: disable=bad-continuation
            and tokens[0].type == lex.TokenType.NEWLINE
            and tokens[1].type == lex.TokenType.WHITESPACE
            and tokens[2].type in COMMENT_TOKENS):
        node.children.append(tokens.pop(0))
        node.children.append(tokens.pop(0))

    assert len(tokens) < len_before, \
        "CommentNode.consume didn't consume any tokens"
    return node

  @classmethod
  def consume_explicit_trailing(cls, ctx, tokens, parent):
    """
    Consume sequential comment lines, removing tokens from the input list and
    appending the resulting node as a child to the provided parent
    """

    while tokens and tokens[0].type in (lex.TokenType.WHITESPACE,
                                        lex.TokenType.NEWLINE):
      parent.children.append(tokens.pop(0))

    node = cls()
    node.is_explicit_trailing = True
    parent.children.append(node)
    while tokens and next_is_explicit_trailing_comment(ctx.config, tokens):
      node.children.append(tokens.pop(0))

  @classmethod
  def consume_implicit_trailing(cls, ctx, tokens, parent):
    """
    Consume sequential comment lines, removing tokens from the input list and
    appending the resulting node as a child to the provided parent
    """

    if tokens[0].type == lex.TokenType.WHITESPACE:
      parent.children.append(tokens.pop(0))

    node = cls()
    node.is_implicit_trailing = True
    parent.children.append(node)
    comment_tokens = node.children

    comment_tokens = []
    while tokens and is_valid_trailing_comment(tokens[0]):
      if (comment_tokens and
          not are_column_aligned(comment_tokens[-1], tokens[0])):
        break

      comment_token = tokens.pop(0)
      comment_tokens.append(comment_token)
      node.children.append(comment_token)

      # Multiple comments separated by only one newline are joined together into
      # a single block
      if (len(tokens) > 1 and
          tokens[0].type == lex.TokenType.NEWLINE and
          is_valid_trailing_comment(tokens[1])):
        node.children.append(tokens.pop(0))

      # Multiple comments separated only by one newline and some whitespace are
      # joined together into a single block
      # TODO(josh)[873a811]: maybe match only on comment tokens that start at
      # the same column?
      elif (len(tokens) > 2 and
            tokens[0].type == lex.TokenType.NEWLINE and
            tokens[1].type == lex.TokenType.WHITESPACE and
            is_valid_trailing_comment(tokens[2])):
        node.children.append(tokens.pop(0))
        node.children.append(tokens.pop(0))

  @classmethod
  def consume_trailing(cls, ctx, tokens, parent):
    """
    Consume sequential comment lines, removing tokens from the input list and
    appending the resulting node as a child to the provided parent
    """
    if not next_is_trailing_comment(ctx.config, tokens):
      return None

    if next_is_explicit_trailing_comment(ctx.config, tokens):
      return cls.consume_explicit_trailing(ctx, tokens, parent)

    return cls.consume_implicit_trailing(ctx, tokens, parent)


class OnOffNode(TreeNode):
  """Stores a single comment token signifying cmake-format should enable or
     disable.
  """

  def __init__(self):
    super(OnOffNode, self).__init__(NodeType.ONOFFSWITCH)

  @classmethod
  def consume(cls, ctx, tokens):
    """
    Consume a 'cmake-format: [on|off]' comment
    """

    node = cls()
    node.children.append(tokens.pop(0))
    return node


def consume_whitespace_and_comments(ctx, tokens, tree):
  """Consume any whitespace or comments that occur at the current depth
  """
  # If it is a whitespace token then put it directly in the parse tree at
  # the current depth
  while tokens:
    # If it is a whitespace token then put it directly in the parse tree at
    # the current depth
    if tokens[0].type in WHITESPACE_TOKENS:
      tree.children.append(tokens.pop(0))
      continue

    # If it's a comment token not associated with an argument, then put it
    # directly into the parse tree at the current depth
    if tokens[0].type in (lex.TokenType.COMMENT,
                          lex.TokenType.BRACKET_COMMENT):
      child = CommentNode.consume(ctx, tokens)
      tree.children.append(child)
      continue

    # If it's a sentinel comment, then add it at the current depth
    if tokens[0].type in (lex.TokenType.FORMAT_OFF,
                          lex.TokenType.FORMAT_ON):
      tree.children.append(OnOffNode.consume(ctx, tokens))
      continue
    break
