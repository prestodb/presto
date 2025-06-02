from cmakelang import lex
from cmakelang.parse.argument_nodes import (
    ArgGroupNode,
    PositionalGroupNode
)
from cmakelang.parse.common import (
    TreeNode,
    NodeType,
)
from cmakelang.parse.util import (
    WHITESPACE_TOKENS,
    should_break,
)


def parse_empty(ctx, tokens, breakstack):
  """
  ::

    break()
    continue()
    enable_testing()

  :see: https://cmake.org/cmake/help/latest/command/break.html
  :see: https://cmake.org/cmake/help/latest/command/continue.html
  :see: https://cmake.org/cmake/help/latest/command/enable_testing.html
  :see: https://cmake.org/cmake/help/latest/command/return.html
  """
  tree = ArgGroupNode()

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
      child = TreeNode(NodeType.COMMENT)
      tree.children.append(child)
      child.children.append(tokens.pop(0))
      continue

    ctx.lint_ctx.record_lint("E1121", location=tokens[0].get_location())
    ntokens = len(tokens)
    subtree = PositionalGroupNode.parse(ctx, tokens, "+", [], breakstack)
    assert len(tokens) < ntokens
    tree.children.append(subtree)
  return tree


def populate_db(parse_db):
  parse_db["break"] = parse_empty
  parse_db["continue"] = parse_empty
  parse_db["enable_testing"] = parse_empty
  parse_db["return"] = parse_empty
