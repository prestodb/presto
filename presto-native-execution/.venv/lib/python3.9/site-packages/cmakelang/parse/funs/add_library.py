import logging

from cmakelang.lex import TokenType
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
    PositionalSpec,
    should_break
)

logger = logging.getLogger(__name__)


def parse_add_library_standard(ctx, tokens, breakstack, sortable):
  """
  ::

    add_library(<name> [STATIC | SHARED | MODULE]
               [EXCLUDE_FROM_ALL]
               source1 [source2 ...])

  :see: https://cmake.org/cmake/help/v3.0/command/add_library.html
  """
  # pylint: disable=too-many-statements

  parsing_name = 1
  parsing_type = 2
  parsing_flag = 3
  parsing_sources = 4

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

  flags = ("STATIC", "SHARED", "MODULE", "OBJECT")

  while tokens:
    # This parse function breakson the first right paren, since parenthetical
    # groups are not allowed. A parenthesis might exist in a filename, but
    # if so that filename should be quoted so it wont show up as a RIGHT_PAREN
    # token.
    if tokens[0].type is TokenType.RIGHT_PAREN:
      break

    # If it is a whitespace token then put it directly in the parse tree at
    # the current depth
    if tokens[0].type in WHITESPACE_TOKENS:
      active_depth.children.append(tokens.pop(0))
      continue

    # If it's a comment token not associated with an argument, then put it
    # directly into the parse tree at the current depth
    if tokens[0].type in (TokenType.COMMENT,
                          TokenType.BRACKET_COMMENT):
      if state_ > parsing_name:
        if get_tag(tokens[0]) in ("unsort", "unsortable"):
          sortable = False
        elif get_tag(tokens[0]) in ("sort", "sortable"):
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
    elif state_ is parsing_type:
      if get_normalized_kwarg(tokens[0]) in flags:
        token = tokens.pop(0)
        child = TreeNode(NodeType.FLAG)
        child.children.append(token)
        CommentNode.consume_trailing(ctx, tokens, child)
        parg_group.children.append(child)
      state_ += 1
    elif state_ is parsing_flag:
      if get_normalized_kwarg(tokens[0]) == "EXCLUDE_FROM_ALL":
        token = tokens.pop(0)
        child = TreeNode(NodeType.FLAG)
        child.children.append(token)
        CommentNode.consume_trailing(ctx, tokens, child)
        parg_group.children.append(child)
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


def parse_add_library_imported(ctx, tokens, breakstack):
  """
  ::
    add_library(<name> <SHARED|STATIC|MODULE|OBJECT|UNKNOWN> IMPORTED
                [GLOBAL])

    :see: https://cmake.org/cmake/help/latest/command/add_library.html
  """
  return StandardArgTree.parse(
      ctx, tokens, npargs='+',
      kwargs={},
      flags=["SHARED", "STATIC", "MODULE", "OBJECT", "UNKOWN", "IMPORTED",
             "GLOBAL"],
      breakstack=breakstack)


def parse_add_library_object(ctx, tokens, breakstack):
  """
  ::
    add_library(<name> OBJECT <src>...)

    :see: https://cmake.org/cmake/help/latest/command/add_library.html#object-libraries
  """
  tree = ArgGroupNode()

  # If it is a whitespace token then put it directly in the parse tree at
  # the current depth
  while tokens and tokens[0].type in WHITESPACE_TOKENS:
    tree.children.append(tokens.pop(0))
    continue

  ntokens = len(tokens)
  subtree = PositionalGroupNode.parse(ctx, tokens, 2, ["OBJECT"], breakstack)
  assert len(tokens) < ntokens
  tree.children.append(subtree)

  while tokens:
    # Break if the next token belongs to a parent parser, i.e. if it
    # matches a keyword argument of something higher in the stack, or if
    # it closes a parent group.
    if should_break(tokens[0], breakstack):
      break

    # If it is a whitespace token then put it directly in the parse tree at
    # the current depth
    if tokens[0].type in WHITESPACE_TOKENS:
      tree.children.append(tokens.pop(0))
      continue

    # If it's a comment, then add it at the current depth
    if tokens[0].type in (TokenType.COMMENT,
                          TokenType.BRACKET_COMMENT):
      if not get_tag(tokens[0]) in ("sort", "sortable"):
        child = TreeNode(NodeType.COMMENT)
        tree.children.append(child)
        child.children.append(tokens.pop(0))
        continue

    ntokens = len(tokens)
    subtree = PositionalGroupNode.parse(ctx, tokens, '+', [], breakstack)
    assert len(tokens) < ntokens
    tree.children.append(subtree)
  return tree


def parse_add_library_alias(ctx, tokens, breakstack):
  """
  ::
    add_library(<name> ALIAS <target>)

    :see: https://cmake.org/cmake/help/latest/command/add_library.html#alias-libraries
  """
  return StandardArgTree.parse(
      ctx, tokens, npargs=3,
      kwargs={},
      flags=["ALIAS"],
      breakstack=breakstack)


def parse_add_library_interface(ctx, tokens, breakstack):
  """
  ::
    add_library(<name> INTERFACE [IMPORTED [GLOBAL]])

    :see: https://cmake.org/cmake/help/latest/command/add_library.html#interface-libraries
  """
  return StandardArgTree.parse(
      ctx, tokens, npargs='+',
      kwargs={},
      flags=["INTERFACE", "IMPORTED", "GLOBAL"],
      breakstack=breakstack)


def parse_add_library(ctx, tokens, breakstack):
  """
  ``add_library()`` has several forms:

  * normal libraires
  * imported libraries
  * object libraries
  * alias libraries
  * interface libraries

  This function is just the dispatcher

  :see: https://cmake.org/cmake/help/latest/command/add_library.html
  """

  semantic_iter = iter_semantic_tokens(tokens)
  # NOTE(josh): first token is always the name of the library
  _ = next(semantic_iter, None)
  # Second token is usually the descriminator, except for INTERFACE
  second_token = next(semantic_iter, None)

  if second_token is None:
    # All add_library() commands should have at least two arguments
    logger.warning("Invalid add_library() command at %s",
                   tokens[0].get_location())
    return StandardArgTree.parse(ctx, tokens, npargs='*', kwargs={}, flags=[],
                                 breakstack=breakstack)

  descriminator = second_token.spelling.upper()
  parsemap = {
      "OBJECT": parse_add_library_object,
      "ALIAS": parse_add_library_alias,
      "INTERFACE": parse_add_library_interface,
      "IMPORTED": parse_add_library_imported
  }
  if descriminator in parsemap:
    return parsemap[descriminator](ctx, tokens, breakstack)

  third_token = next(semantic_iter, None)
  if third_token is not None:
    descriminator = third_token.spelling.upper()
    if descriminator == "IMPORTED":
      return parse_add_library_imported(ctx, tokens, breakstack)

  # If the descriminator token might be a variable dereference, then it
  # might be hiding the descriminator... so we shouldn't infer
  # sortability unless it is a word that doesn't match any of the descriminator
  # flags
  sortable = True
  if "${" in second_token.spelling or "${" in third_token.spelling:
    sortable = False
  return parse_add_library_standard(ctx, tokens, breakstack, sortable)


def populate_db(parse_db):
  parse_db["add_library"] = parse_add_library
