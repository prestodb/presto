import logging

from cmakelang import lex
from cmakelang.parse.common import KwargBreaker
from cmakelang.parse.simple_nodes import CommentNode
from cmakelang.parse.argument_nodes import (
    KeywordGroupNode, PositionalGroupNode, PositionalParser, StandardArgTree)
from cmakelang.parse.additional_nodes import PatternNode
from cmakelang.parse.util import (
    WHITESPACE_TOKENS,
    get_first_semantic_token,
    get_normalized_kwarg,
    should_break)

logger = logging.getLogger(__name__)


def parse_install_targets_sub(ctx, tokens, breakstack):
  """
    Parse the inner kwargs of an ``install(TARGETS)`` command. This is common
    logic for ARCHIVE, LIBRARY, RUNTIME, etc.
  :see: https://cmake.org/cmake/help/v3.14/command/install.html#targets
  """
  return StandardArgTree.parse(
      ctx, tokens,
      npargs='*',
      kwargs={
          "DESTINATION": PositionalParser(1),
          "PERMISSIONS": PositionalParser('+'),
          "CONFIGURATIONS": PositionalParser('+'),
          "COMPONENT": PositionalParser(1),
          "NAMELINK_COMPONENT": PositionalParser(1),
      },
      flags=[
          "OPTIONAL",
          "EXCLUDE_FROM_ALL",
          "NAMELINK_ONLY",
          "NAMELINK_SKIP"
      ],
      breakstack=breakstack)


def parse_install_targets(ctx, tokens, breakstack):
  """
  ::

    install(TARGETS targets... [EXPORT <export-name>]
            [[ARCHIVE|LIBRARY|RUNTIME|OBJECTS|FRAMEWORK|BUNDLE|
              PRIVATE_HEADER|PUBLIC_HEADER|RESOURCE]
             [DESTINATION <dir>]
             [PERMISSIONS permissions...]
             [CONFIGURATIONS [Debug|Release|...]]
             [COMPONENT <component>]
             [NAMELINK_COMPONENT <component>]
             [OPTIONAL] [EXCLUDE_FROM_ALL]
             [NAMELINK_ONLY|NAMELINK_SKIP]
            ] [...]
            [INCLUDES DESTINATION [<dir> ...]]
            )

  :see: https://cmake.org/cmake/help/v3.14/command/install.html#targets
  """
  kwargs = {
      "TARGETS": PositionalParser('+'),
      "EXPORT": PositionalParser(1),
      "INCLUDES": PositionalParser('+', flags=["DESTINATION"]),
      # Common kwargs
      "DESTINATION": PositionalParser(1),
      "PERMISSIONS": PositionalParser('+'),
      "CONFIGURATIONS": PositionalParser('+'),
      "COMPONENT": PositionalParser(1),
      "NAMELINK_COMPONENT": PositionalParser(1),
  }
  flags = (
      "OPTIONAL",
      "EXCLUDE_FROM_ALL",
      "NAMELINK_ONLY",
      "NAMELINK_SKIP"
  )
  designated_kwargs = (
      "ARCHIVE", "LIBRARY", "RUNTIME", "OBJECTS", "FRAMEWORK",
      "BUNDLE", "PRIVATE_HEADER", "PUBLIC_HEADER", "RESOURCE"
  )

  # NOTE(josh): from here on, code is essentially StandardArgTree.parse(),
  # except that
  # we cannot break on the common subset of kwargs in the breakstack because
  # they are valid kwargs for the subtrees (ARCHIVE, LIBRARY, etc) as well as
  # the primary tree
  tree = StandardArgTree()

  # If it is a whitespace token then put it directly in the parse tree at
  # the current depth
  while tokens and tokens[0].type in WHITESPACE_TOKENS:
    tree.children.append(tokens.pop(0))
    continue

  # ARCHIVE, LIBRARY, RUNTIME, subtrees etc only break on the start of
  # another subtree, or on "INCLUDES DESTINATION"
  subtree_breakstack = breakstack + [KwargBreaker(
      list(designated_kwargs) + ["INCLUDES"]
  )]

  # kwargs at this tree depth break on other kwargs or flags
  kwarg_breakstack = breakstack + [KwargBreaker(
      list(kwargs.keys()) + list(designated_kwargs) + list(flags)
  )]

  # and flags at this depth break only on kwargs
  positional_breakstack = breakstack + [KwargBreaker(
      list(kwargs.keys()) + list(designated_kwargs)
  )]

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
      child = CommentNode()
      tree.children.append(child)
      child.children.append(tokens.pop(0))
      continue

    ntokens = len(tokens)
    # NOTE(josh): each flag is also stored in kwargs as with a positional parser
    # of size zero. This is a legacy thing that should be removed, but for now
    # just make sure we check flags first.
    word = get_normalized_kwarg(tokens[0])
    if word in designated_kwargs:
      subtree = KeywordGroupNode.parse(
          ctx, tokens, word, parse_install_targets, subtree_breakstack)
    elif word in kwargs:
      subtree = KeywordGroupNode.parse(
          ctx, tokens, word, kwargs[word], kwarg_breakstack)
    else:
      subtree = PositionalGroupNode.parse(
          ctx, tokens, '+', flags, positional_breakstack)

    assert len(tokens) < ntokens
    tree.children.append(subtree)
  return tree


def parse_install_files(ctx, tokens, breakstack):
  """
  ::
    install(<FILES|PROGRAMS> files...
            TYPE <type> | DESTINATION <dir>
            [PERMISSIONS permissions...]
            [CONFIGURATIONS [Debug|Release|...]]
            [COMPONENT <component>]
            [RENAME <name>] [OPTIONAL] [EXCLUDE_FROM_ALL])

  :see: https://cmake.org/cmake/help/v3.14/command/install.html#files
  """
  return StandardArgTree.parse(
      ctx, tokens,
      npargs='*',
      kwargs={
          "FILES": PositionalParser('+'),
          "PROGRAMS": PositionalParser('+'),
          "TYPE": PositionalParser(1),
          "DESTINATION": PositionalParser(1),
          "PERMISSIONS": PositionalParser('+'),
          "CONFIGURATIONS": PositionalParser('+'),
          "COMPONENT": PositionalParser(1),
          "RENAME": PositionalParser(1),
      },
      flags=[
          "OPTIONAL",
          "EXCLUDE_FROM_ALL",
      ],
      breakstack=breakstack)


def parse_install_directory(ctx, tokens, breakstack):
  """
  ::

    install(DIRECTORY dirs...
            TYPE <type> | DESTINATION <dir>
            [FILE_PERMISSIONS permissions...]
            [DIRECTORY_PERMISSIONS permissions...]
            [USE_SOURCE_PERMISSIONS] [OPTIONAL] [MESSAGE_NEVER]
            [CONFIGURATIONS [Debug|Release|...]]
            [COMPONENT <component>] [EXCLUDE_FROM_ALL]
            [FILES_MATCHING]
            [[PATTERN <pattern> | REGEX <regex>]
             [EXCLUDE] [PERMISSIONS permissions...]] [...])

  :see: https://cmake.org/cmake/help/v3.14/command/install.html#directory
  """
  return StandardArgTree.parse(
      ctx, tokens,
      npargs='*',
      kwargs={
          "DIRECTORY": PositionalParser('+'),
          "TYPE": PositionalParser(1),
          "DESTINATION": PositionalParser(1),
          "FILE_PERMISSIONS": PositionalParser('+'),
          "DIRECTORY_PERMISSIONS": PositionalParser('+'),
          "CONFIGURATIONS": PositionalParser('+'),
          "COMPONENT": PositionalParser(1),
          "RENAME": PositionalParser(1),
          "PATTERN": PatternNode.parse,
          "REGEX": PatternNode.parse,
      },
      flags=[
          "USER_SOURCE_PERMISSIONS",
          "OPTIONAL",
          "MESSAGE_NEVER",
          "FILES_MATCHING",
      ],
      breakstack=breakstack)


def parse_install_script(ctx, tokens, breakstack):
  """
  ::

    install([[SCRIPT <file>] [CODE <code>]]
            [COMPONENT <component>] [EXCLUDE_FROM_ALL] [...])

  :see: https://cmake.org/cmake/help/v3.14/command/install.html#custom-installation-logic
  """
  return StandardArgTree.parse(
      ctx, tokens,
      npargs='*',
      kwargs={
          "SCRIPT": PositionalParser(1),
          "CODE": PositionalParser(1),
          "COMPONENT": PositionalParser(1),
      },
      flags=[
          "EXCLUDE_FROM_ALL"
      ],
      breakstack=breakstack)


def parse_install_export(ctx, tokens, breakstack):
  """
  ::

    install(EXPORT <export-name> DESTINATION <dir>
            [NAMESPACE <namespace>] [[FILE <name>.cmake]|
            [PERMISSIONS permissions...]
            [CONFIGURATIONS [Debug|Release|...]]
            [EXPORT_LINK_INTERFACE_LIBRARIES]
            [COMPONENT <component>]
            [EXCLUDE_FROM_ALL])

  :see: https://cmake.org/cmake/help/v3.14/command/install.html#installing-exports
  """
  return StandardArgTree.parse(
      ctx, tokens,
      npargs='*',
      kwargs={
          "EXPORT": PositionalParser(1),
          "DESTINATION": PositionalParser(1),
          "NAMESPACE": PositionalParser(1),
          "FILE": PositionalParser(1),
          "PERMISSIONS": PositionalParser('+'),
          "CONFIGURATIONS": PositionalParser('+'),
          "COMPONENT": PositionalParser(1),
      },
      flags=[
          "EXCLUDE_FROM_ALL"
      ],
      breakstack=breakstack)


def parse_install(ctx, tokens, breakstack):
  """
  The ``install()`` command has multiple different forms, implemented
  by different functions. The forms are indicated by the first token
  and are:

  * TARGETS
  * FILES
  * DIRECTORY
  * SCRIPT
  * CODE
  * EXPORT

  :see: https://cmake.org/cmake/help/v3.0/command/install.html
  """

  descriminator_token = get_first_semantic_token(tokens)
  if descriminator_token is None:
    logger.warning("Invalid install() command at %s", tokens[0].get_location())
    return StandardArgTree.parse(ctx, tokens, npargs='*', kwargs={}, flags=[],
                                 breakstack=breakstack)

  descriminator = descriminator_token.spelling.upper()
  parsemap = {
      "TARGETS": parse_install_targets,
      "FILES": parse_install_files,
      "PROGRAMS": parse_install_files,
      "DIRECTORY": parse_install_directory,
      "SCRIPT": parse_install_script,
      "CODE": parse_install_script,
      "EXPORT": parse_install_export
  }
  if descriminator not in parsemap:
    logger.warning("Invalid install form \"%s\" at %s", descriminator,
                   tokens[0].location())
    return StandardArgTree.parse(ctx, tokens, npargs='*', kwargs={}, flags=[],
                                 breakstack=breakstack)

  return parsemap[descriminator](ctx, tokens, breakstack)


def populate_db(parse_db):
  parse_db["install"] = parse_install
