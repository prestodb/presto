import logging

from cmakelang import lex
from cmakelang.parse.argument_nodes import (
    ArgGroupNode, PositionalGroupNode, StandardArgTree)
from cmakelang.parse.common import NodeType, TreeNode
from cmakelang.parse.simple_nodes import CommentNode

from cmakelang.parse.util import (
    WHITESPACE_TOKENS,
    get_tag,
    get_normalized_kwarg,
    iter_semantic_tokens,
    only_comments_and_whitespace_remain,
    PositionalSpec
)

logger = logging.getLogger(__name__)


def parse_add_executable_imported(ctx, tokens, breakstack):
  """
  ::

    add_executable(<name> IMPORTED [GLOBAL])

  :see: https://cmake.org/cmake/help/latest/command/add_executable.html
  """
  return StandardArgTree.parse(
      ctx, tokens, npargs='+',
      kwargs={},
      flags=["IMPORTED", "GLOBAL"],
      breakstack=breakstack)


def parse_add_executable_alias(ctx, tokens, breakstack):
  """
  ::
    add_executable(<name> ALIAS <target>)

    :see: https://cmake.org/cmake/help/latest/command/add_executable.html
  """
  return StandardArgTree.parse(
      ctx, tokens, npargs=3,
      kwargs={},
      flags=["ALIAS"],
      breakstack=breakstack)


def parse_add_executable_standard(ctx, tokens, breakstack, sortable):
  """
  ::

    add_executable(<name> [WIN32] [MACOSX_BUNDLE]
                   [EXCLUDE_FROM_ALL]
                   [source1] [source2 ...])

  :see: https://cmake.org/cmake/help/latest/command/add_executable.html#command:add_executable
  """
  # pylint: disable=too-many-statements

  parsing_name = 1
  parsing_flags = 2
  parsing_sources = 3

  tree = ArgGroupNode()

  # If it is a whitespace token then put it directly in the parse tree at
  # the current depth
  while tokens and tokens[0].type in WHITESPACE_TOKENS:
    tree.children.append(tokens.pop(0))
    continue

  state_ = parsing_name
  parg_group = None
  src_group = None
  active_depth = tree

  while tokens:
    # This parse function breaks on the first right paren, since parenthetical
    # groups are not allowed. A parenthesis might exist in a filename, but
    # if so that filename should be quoted so it wont show up as a RIGHT_PAREN
    # token.
    if tokens[0].type is lex.TokenType.RIGHT_PAREN:
      break

    # If it is a whitespace token then put it directly in the parse tree at
    # the current depth
    if tokens[0].type in WHITESPACE_TOKENS:
      active_depth.children.append(tokens.pop(0))
      continue

    # If it's a comment token not associated with an argument, then put it
    # directly into the parse tree at the current depth
    if tokens[0].type in (lex.TokenType.COMMENT,
                          lex.TokenType.BRACKET_COMMENT):
      if state_ > parsing_name:
        if get_tag(tokens[0]) in ("unsort", "unsortable"):
          sortable = False
        elif get_tag(tokens[0]) in ("unsort", "unsortable"):
          sortable = True
      child = TreeNode(NodeType.COMMENT)
      active_depth.children.append(child)
      child.children.append(tokens.pop(0))
      continue

    if state_ is parsing_name:
      token = tokens.pop(0)
      parg_group = PositionalGroupNode()
      parg_group.spec = PositionalSpec("+")
      active_depth = parg_group
      tree.children.append(parg_group)
      child = TreeNode(NodeType.ARGUMENT)
      child.children.append(token)
      CommentNode.consume_trailing(ctx, tokens, child)
      parg_group.children.append(child)
      state_ += 1
    elif state_ is parsing_flags:
      if get_normalized_kwarg(tokens[0]) in (
          "WIN32", "MACOSX_BUNDLE", "EXCLUDE_FROM_ALL"):
        token = tokens.pop(0)
        child = TreeNode(NodeType.FLAG)
        child.children.append(token)
        CommentNode.consume_trailing(ctx, tokens, child)
        parg_group.children.append(child)
      else:
        state_ += 1
        src_group = PositionalGroupNode(sortable=sortable, tags=["file-list"])
        src_group.spec = PositionalSpec("+")
        active_depth = src_group
        tree.children.append(src_group)
    elif state_ is parsing_sources:
      token = tokens.pop(0)
      child = TreeNode(NodeType.ARGUMENT)
      child.children.append(token)
      CommentNode.consume_trailing(ctx, tokens, child)
      src_group.children.append(child)

      if only_comments_and_whitespace_remain(tokens, breakstack):
        active_depth = tree

  return tree


def parse_add_executable(ctx, tokens, breakstack):
  """
  ``add_executable()`` has a couple of forms:

  * normal executables
  * imported executables
  * alias executables

  This function is just the dispatcher

  :see: https://cmake.org/cmake/help/latest/command/add_executable.html
  """

  semantic_iter = iter_semantic_tokens(tokens)
  # NOTE(josh): first token is always the name of the executable
  _ = next(semantic_iter, None)
  # Second token is usually the descriminator
  second_token = next(semantic_iter, None)

  if second_token is None:
    # All add_library() commands should have at least two arguments
    logger.warning("Invalid add_executable() command at %s",
                   tokens[0].get_location())
    return StandardArgTree.parse(ctx, tokens, npargs='*', kwargs={}, flags=[],
                                 breakstack=breakstack)

  descriminator = second_token.spelling.upper()
  parsemap = {
      "ALIAS": parse_add_executable_alias,
      "IMPORTED": parse_add_executable_imported
  }
  if descriminator in parsemap:
    return parsemap[descriminator](ctx, tokens, breakstack)

  # If the descriminator token might be a variable dereference, then it
  # might be hiding the descriminator... so we shouldn't infer
  # sortability unless it is a word that doesn't match any of the descriminator
  # flags
  sortable = True
  if "${" in second_token.spelling:
    sortable = False

  return parse_add_executable_standard(ctx, tokens, breakstack, sortable)


def populate_db(parse_db):
  parse_db["add_executable"] = parse_add_executable
