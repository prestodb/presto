# pylint: disable=too-many-statements
import logging

from cmakelang.lex import TokenType
from cmakelang.parse.additional_nodes import (
    FlagGroupNode, ShellCommandNode, TupleParser)
from cmakelang.parse.argument_nodes import (
    ArgGroupNode,
    KeywordGroupNode,
    PositionalGroupNode,
    PositionalParser,
    StandardArgTree)
from cmakelang.parse.common import KwargBreaker, NodeType, TreeNode
from cmakelang.parse.util import (
    WHITESPACE_TOKENS,
    get_first_semantic_token,
    get_normalized_kwarg,
    should_break)

logger = logging.getLogger(__name__)


def parse_add_custom_command_events(ctx, tokens, breakstack):
  """
  ::
    add_custom_command(TARGET <target>
                    PRE_BUILD | PRE_LINK | POST_BUILD
                    COMMAND command1 [ARGS] [args1...]
                    [COMMAND command2 [ARGS] [args2...] ...]
                    [BYPRODUCTS [files...]]
                    [WORKING_DIRECTORY dir]
                    [COMMENT comment]
                    [VERBATIM] [USES_TERMINAL]
                    [COMMAND_EXPAND_LISTS])
  :see: https://cmake.org/cmake/help/latest/command/add_custom_command.html
  """
  subtree = StandardArgTree.parse(
      ctx, tokens,
      npargs='*',
      kwargs={
          "BYPRODUCTS": PositionalParser('*'),
          "COMMAND": ShellCommandNode.parse,
          "COMMENT": PositionalParser('*'),
          "TARGET": PositionalParser(1),
          "WORKING_DIRECTORY": PositionalParser(1)
      },
      flags=[
          "APPEND", "VERBATIM", "USES_TERMINAL", "COMMAND_EXPAND_LISTS",
          "PRE_BUILD", "PRE_LINK", "POST_BUILD"],
      breakstack=breakstack)

  subtree.check_required_kwargs(ctx.lint_ctx, {
      # Truly required keyword arguments
      "COMMAND": "E1125",
      # Required by convention
      "COMMENT": "C0113"
  })
  return subtree


def parse_add_custom_command_standard(ctx, tokens, breakstack):
  """
  ::

      add_custom_command(OUTPUT output1 [output2 ...]
                         COMMAND command1 [ARGS] [args1...]
                         [COMMAND command2 [ARGS] [args2...] ...]
                         [MAIN_DEPENDENCY depend]
                         [DEPENDS [depends...]]
                         [BYPRODUCTS [files...]]
                         [IMPLICIT_DEPENDS <lang1> depend1
                                          [<lang2> depend2] ...]
                         [WORKING_DIRECTORY dir]
                         [COMMENT comment]
                         [DEPFILE depfile]
                         [JOB_POOL job_pool]
                         [VERBATIM] [APPEND] [USES_TERMINAL]
                         [COMMAND_EXPAND_LISTS])

  :see: https://cmake.org/cmake/help/latest/command/add_custom_command.html
  """
  subtree = StandardArgTree.parse(
      ctx, tokens,
      npargs='*',
      kwargs={
          "BYPRODUCTS": PositionalParser('*'),
          "COMMAND": ShellCommandNode.parse,
          "COMMENT": PositionalParser('*'),
          "DEPENDS": PositionalParser('*'),
          "DEPFILE": PositionalParser(1),
          "JOB_POOL": PositionalParser(1),
          "IMPLICIT_DEPENDS": TupleParser(2, '+'),
          "MAIN_DEPENDENCY": PositionalParser(1),
          "OUTPUT": PositionalParser('+'),
          "WORKING_DIRECTORY": PositionalParser(1)
      },
      flags=["APPEND", "VERBATIM", "USES_TERMINAL", "COMMAND_EXPAND_LISTS"],
      breakstack=breakstack)

  subtree.check_required_kwargs(ctx.lint_ctx, {
      # Truly required keyword arguments
      "COMMAND": "E1125",
      # Required by convention
      "COMMENT": "C0113"
  })

  return subtree


def parse_add_custom_command(ctx, tokens, breakstack):
  """
  There are two forms of `add_custom_command`. This is the dispatcher between
  the two forms.
  """
  descriminator_token = get_first_semantic_token(tokens)
  if (descriminator_token is None or
      descriminator_token.type is TokenType.RIGHT_PAREN):
    location = ()
    if tokens:
      location = tokens[0].get_location()
    # logger.warning("Invalid empty file() command at %s", location)
    ctx.lint_ctx.record_lint("E1120", location=location)
    return StandardArgTree.parse(ctx, tokens, npargs='*', kwargs={}, flags=[],
                                 breakstack=breakstack)

  if descriminator_token.type is TokenType.DEREF:
    ctx.lint_ctx.record_lint(
        "C0114", location=descriminator_token.get_location())
    return parse_add_custom_command_standard(ctx, tokens, breakstack)

  descriminator_word = get_normalized_kwarg(descriminator_token)
  if descriminator_word == "TARGET":
    return parse_add_custom_command_events(ctx, tokens, breakstack)
  if descriminator_word == "OUTPUT":
    return parse_add_custom_command_standard(ctx, tokens, breakstack)

  # logger.warning(
  #     "Indeterminate form of add_custom_command \"%s\" at %s",
  #     descriminator_word, descriminator_token.location())
  ctx.lint_ctx.record_lint("E1126", location=descriminator_token.get_location())
  return parse_add_custom_command_standard(ctx, tokens, breakstack)


def parse_add_custom_target(ctx, tokens, breakstack):
  """
  ::
    add_custom_target(Name [ALL] [command1 [args1...]]
                      [COMMAND command2 [args2...] ...]
                      [DEPENDS depend depend depend ... ]
                      [BYPRODUCTS [files...]]
                      [WORKING_DIRECTORY dir]
                      [COMMENT comment]
                      [JOB_POOL job_pool]
                      [VERBATIM] [USES_TERMINAL]
                      [COMMAND_EXPAND_LISTS]
                      [SOURCES src1 [src2...]])

  :see: https://cmake.org/cmake/help/latest/command/add_custom_target.html
  """
  kwargs = {
      "BYPRODUCTS": PositionalParser("+"),
      "COMMAND": ShellCommandNode.parse,
      "COMMENT": PositionalParser(1),
      "DEPENDS": PositionalParser("+"),
      "JOB_POOL": PositionalParser(1),
      "SOURCES": PositionalParser("+"),
      "WORKING_DIRECTORY": PositionalParser(1),
  }

  required_kwargs = {
      # Required by convention
      "COMMENT": "C0113"
  }

  flags = ("VERBATIM", "USES_TERMINAL", "COMMAND_EXPAND_LISTS")
  tree = ArgGroupNode()

  # If it is a whitespace token then put it directly in the parse tree at
  # the current depth
  while tokens and tokens[0].type in WHITESPACE_TOKENS:
    tree.children.append(tokens.pop(0))
    continue

  breaker = KwargBreaker(list(kwargs.keys()) + list(flags))
  child_breakstack = breakstack + [breaker]

  nametree = None
  state = "name"

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
      child = TreeNode(NodeType.COMMENT)
      tree.children.append(child)
      child.children.append(tokens.pop(0))
      continue

    ntokens = len(tokens)
    if state == "name":
      next_semantic = get_first_semantic_token(tokens[1:])
      if (next_semantic is not None and
          get_normalized_kwarg(next_semantic) == "ALL"):
        npargs = 2
      else:
        npargs = 1

      nametree = PositionalGroupNode.parse(
          ctx, tokens, npargs, ["ALL"], child_breakstack)
      assert len(tokens) < ntokens
      tree.children.append(nametree)
      state = "first-command"
      continue

    word = get_normalized_kwarg(tokens[0])
    if state == "first-command":
      if not(word in kwargs or word in flags):
        subtree = PositionalGroupNode.parse(
            ctx, tokens, '+', [], child_breakstack)
        tree.children.append(subtree)
        assert len(tokens) < ntokens
      state = "kwargs"
      continue

    if word in flags:
      subtree = FlagGroupNode.parse(
          ctx, tokens, flags, breakstack + [KwargBreaker(list(kwargs.keys()))])
      assert len(tokens) < ntokens
      tree.children.append(subtree)
      continue

    if word in kwargs:
      required_kwargs.pop(word, None)
      subtree = KeywordGroupNode.parse(
          ctx, tokens, word, kwargs[word], child_breakstack)
      assert len(tokens) < ntokens
      tree.children.append(subtree)
      continue

    ctx.lint_ctx.record_lint("E1122", location=tokens[0].get_location())
    # logger.warning(
    #     "Unexpected positional argument %s at %s",
    #     tokens[0].spelling, tokens[0].location())
    subtree = PositionalGroupNode.parse(ctx, tokens, '+', [], child_breakstack)
    assert len(tokens) < ntokens
    tree.children.append(subtree)
    continue

  if required_kwargs:
    location = ()
    for token in tree.get_semantic_tokens():
      location = token.get_location()
      break

    missing_kwargs = sorted(
        (lintid, word) for word, lintid in sorted(required_kwargs.items()))
    for lintid, word in missing_kwargs:
      ctx.lint_ctx.record_lint(lintid, word, location=location)
  return tree


def parse_add_compile_options(ctx, tokens, breakstack):
  """
  ::

    add_compile_options(<option> ...)

  :see: https://cmake.org/cmake/help/latest/command/add_compile_options.html
  """
  return StandardArgTree.parse(ctx, tokens, "+", {}, [], breakstack)


def parse_add_definitions(ctx, tokens, breakstack):
  """
  ::

    add_definitions(-DFOO -DBAR ...)

  :see: https://cmake.org/cmake/help/latest/command/add_definitions.html
  """
  return StandardArgTree.parse(ctx, tokens, "+", {}, [], breakstack)


def parse_add_dependencies(ctx, tokens, breakstack):
  """
  ::

    add_dependencies(<target> [<target-dependency>]...)

  :see: https://cmake.org/cmake/help/latest/command/add_dependencies.html
  """

  return StandardArgTree.parse(ctx, tokens, "2+", {}, [], breakstack)


def parse_add_subdirectory(ctx, tokens, breakstack):
  """
  ::

    add_subdirectory(source_dir [binary_dir] [EXCLUDE_FROM_ALL])

  :see: https://cmake.org/cmake/help/latest/command/add_subdirectory.html
  """

  return StandardArgTree.parse(
      ctx, tokens, "+", {}, ["EXCLUDE_FROM_ALL"], breakstack)


def parse_aux_source_directory(ctx, tokens, breakstack):
  """
  ::

    aux_source_directory(<dir> <variable>)

  :see: https://cmake.org/cmake/help/latest/command/aux_source_directory.html
  """

  return StandardArgTree.parse(
      ctx, tokens, "2", {}, [], breakstack)


def populate_db(parse_db):
  parse_db["add_custom_command"] = parse_add_custom_command
  parse_db["add_custom_target"] = parse_add_custom_target
  parse_db["add_compile_options"] = parse_add_compile_options
  parse_db["add_definitions"] = parse_add_definitions
  parse_db["add_dependencies"] = parse_add_dependencies
  parse_db["add_subdirectory"] = parse_add_subdirectory
  parse_db["aux_source_directory"] = parse_aux_source_directory
