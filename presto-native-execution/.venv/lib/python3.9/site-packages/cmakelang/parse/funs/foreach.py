
import logging

from cmakelang import lex
from cmakelang.parse.common import KwargBreaker, NodeType
from cmakelang.parse.common import TreeNode
from cmakelang.parse.argument_nodes import (
    ArgGroupNode,
    PositionalParser,
    StandardArgTree,
    PositionalGroupNode,
    KeywordGroupNode)
from cmakelang.parse.util import (
    WHITESPACE_TOKENS,
    get_normalized_kwarg,
    get_tag,
    iter_semantic_tokens,
    should_break,
)

logger = logging.getLogger(__name__)


def parse_foreach_standard(ctx, tokens, breakstack):
  """
  ::

    foreach(<loop_var> <items>)
      <commands>
    endforeach()
  """
  tree = ArgGroupNode()

  # If it is a whitespace token then put it directly in the parse tree at
  # the current depth
  while tokens and tokens[0].type in WHITESPACE_TOKENS:
    tree.children.append(tokens.pop(0))
    continue

  # Consume the loop variable and any attached comments
  ntokens = len(tokens)
  subtree = PositionalGroupNode.parse(ctx, tokens, 1, [], breakstack)
  assert len(tokens) < ntokens
  tree.children.append(subtree)

  # Consume the items of iteration
  ntokens = len(tokens)
  subtree = PositionalGroupNode.parse(ctx, tokens, '+', [], breakstack)
  assert len(tokens) < ntokens
  tree.children.append(subtree)

  return tree


def parse_foreach_range(ctx, tokens, breakstack):
  """
  ::

    foreach(<loop_var> RANGE <start> <stop> [<step>])
      <commands>
    endforeach()
  """

  return StandardArgTree.parse(
      ctx, tokens,
      npargs=1,
      kwargs={"RANGE": PositionalParser(3)},
      flags=[],
      breakstack=breakstack
  )


def parse_foreach_in(ctx, tokens, breakstack):
  """
  ::

    foreach(loop_var IN [LISTS [<lists>]] [ITEMS [<items>]])
  """
  tree = ArgGroupNode()

  # If it is a whitespace token then put it directly in the parse tree at
  # the current depth
  while tokens and tokens[0].type in WHITESPACE_TOKENS:
    tree.children.append(tokens.pop(0))
    continue

  # Consume the loop variable and any attached comments
  ntokens = len(tokens)
  pargs = PositionalGroupNode.parse(ctx, tokens, 2, ["IN"], breakstack)
  assert len(tokens) < ntokens
  tree.children.append(pargs)

  kwargs = {
      "LISTS": PositionalParser("+"),
      "ITEMS": PositionalParser("+")
  }
  sub_breakstack = breakstack + [KwargBreaker(list(kwargs.keys()))]

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
    if tokens[0].type in (lex.TokenType.COMMENT,
                          lex.TokenType.BRACKET_COMMENT):
      if not get_tag(tokens[0]) in ("sort", "sortable"):
        child = TreeNode(NodeType.COMMENT)
        tree.children.append(child)
        child.children.append(tokens.pop(0))
        continue

    ntokens = len(tokens)
    word = get_normalized_kwarg(tokens[0])
    subparser = kwargs.get(word, None)
    if subparser is not None:
      subtree = KeywordGroupNode.parse(
          ctx, tokens, word, subparser, sub_breakstack)
    else:
      logger.warning(
          "Unexpected positional argument at %s", tokens[0].location)
      subtree = PositionalGroupNode.parse(ctx, tokens, '+', [], sub_breakstack)

    assert len(tokens) < ntokens
    tree.children.append(subtree)
  return tree


def parse_foreach(ctx, tokens, breakstack):
  """
  ``parse_foreach()`` has a couple of forms:

  * the usual form
  * range form
  * in (lists/items) form

  This function is just the dispatcher

  :see: https://cmake.org/cmake/help/latest/command/foreach.html
  """

  semantic_iter = iter_semantic_tokens(tokens)
  # first token is always the loop variable
  _ = next(semantic_iter, None)
  # Second token is usually the descriminator
  second_token = next(semantic_iter, None)

  if second_token is None:
    # All foreach() statements should have at least two arguments
    logger.warning("Invalid foreach() statement at %s",
                   tokens[0].get_location())
    return StandardArgTree.parse(
        ctx, tokens, npargs='*', kwargs={}, flags=[], breakstack=breakstack)

  descriminator = second_token.spelling.upper()
  dispatchee = {
      "RANGE": parse_foreach_range,
      "IN": parse_foreach_in
  }.get(descriminator, None)
  if dispatchee is not None:
    return dispatchee(ctx, tokens, breakstack)

  return parse_foreach_standard(ctx, tokens, breakstack)


def populate_db(parse_db):
  parse_db["foreach"] = parse_foreach
