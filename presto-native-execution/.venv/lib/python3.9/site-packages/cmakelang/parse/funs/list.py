import logging

from cmakelang.lex import TokenType
from cmakelang.parse.argument_nodes import (
    StandardArgTree,
    StandardParser)
from cmakelang.parse.util import get_first_semantic_token

logger = logging.getLogger(__name__)


def null_parser(ctx, tokens, breakstack):  # pylint: disable=unused-argument
  return None


SORT_KWARGS = {
    "COMPARE": StandardParser(1, flags=["STRING", "FILE_BASENAME"]),
    "CASE": StandardParser(1, flags=["SENSITIVE", "INSENSITIVE"]),
    "ORDER": StandardParser(1, flags=["ASCENDING", "DESCENDING"])
}

TRANSFORM_KWARGS = {
    # actions
    "APPEND": StandardParser(1),
    "PREPEND": StandardParser(1),
    "TOLOWER": null_parser,
    "TOUPPER": null_parser,
    "STRIP": null_parser,
    "GENEX_STRIP": null_parser,
    "REPLACE": StandardParser(2),
    # selectors
    "AT": StandardParser("+"),
    "FOR": StandardParser("2+"),
    "REGEX": StandardParser(1)
}


def parse_list(ctx, tokens, breakstack):
  """
  Reading
    list(LENGTH <list> <out-var>)
    list(GET <list> <element index> [<index> ...] <out-var>)
    list(JOIN <list> <glue> <out-var>)
    list(SUBLIST <list> <begin> <length> <out-var>)

  Search
    list(FIND <list> <value> <out-var>)

  Modification
    list(APPEND <list> [<element>...])
    list(FILTER <list> {INCLUDE | EXCLUDE} REGEX <regex>)
    list(INSERT <list> <index> [<element>...])
    list(POP_BACK <list> [<out-var>...])
    list(POP_FRONT <list> [<out-var>...])
    list(PREPEND <list> [<element>...])
    list(REMOVE_ITEM <list> <value>...)
    list(REMOVE_AT <list> <index>...)
    list(REMOVE_DUPLICATES <list>)
    list(TRANSFORM <list> <ACTION> [...])

  Ordering
    list(REVERSE <list>)
    list(SORT <list> [...])

  TRANSFORM actions:
    list(TRANSFORM <list> <APPEND|PREPEND> <value> ...)
    list(TRANSFORM <list> <TOLOWER|TOUPPER> ...)
    list(TRANSFORM <list> STRIP ...)
    list(TRANSFORM <list> GENEX_STRIP ...)
    list(TRANSFORM <list> REPLACE <regular_expression>
                                  <replace_expression> ...)

  TRANSFORM selectors
    list(TRANSFORM <list> <ACTION> AT <index> [<index> ...] ...)
    list(TRANSFORM <list> <ACTION> FOR <start> <stop> [<step>] ...)
    list(TRANSFORM <list> <ACTION> REGEX <regular_expression> ...)

  :see: https://cmake.org/cmake/help/latest/command/list.html
  """

  descriminator_token = get_first_semantic_token(tokens)
  if (descriminator_token is None or
      descriminator_token.type is TokenType.RIGHT_PAREN):
    location = ()
    if tokens:
      location = tokens[0].get_location()
    logger.warning("Invalid empty list() command at %s", location)
    ctx.lint_ctx.record_lint("E1120", location=location)
    return StandardArgTree.parse(ctx, tokens, npargs='*', kwargs={}, flags=[],
                                 breakstack=breakstack)

  if descriminator_token.type is TokenType.DEREF:
    ctx.lint_ctx.record_lint(
        "C0114", location=descriminator_token.get_location())
    return StandardArgTree.parse(ctx, tokens, npargs='*', kwargs={}, flags=[],
                                 breakstack=breakstack)

  descriminator = descriminator_token.spelling.upper()
  parsemap = {
      "LENGTH": StandardParser(3, flags=["LENGTH"]),
      "GET": StandardParser("4+", flags=["GET"]),
      "JOIN": StandardParser(4, flags=["JOIN"]),
      "SUBLIST": StandardParser(5, flags=["SUBLIST"]),
      "FIND": StandardParser(4, flags=["FIND"]),
      "APPEND": StandardParser("2+", flags=["APPEND"]),
      "FILTER": StandardParser(
          5, flags=["FILTER", "INCLUDE", "EXCLUDE", "REGEX"]),
      "INSERT": StandardParser("4+", flags=["INSERT"]),
      "POP_BACK": StandardParser("2+", flags=["POP_BACK"]),
      "POP_FRONT": StandardParser("2+", flags=["POP_FRONT"]),
      "PREPEND": StandardParser("2+", flags=["PREPEND"]),
      "REMOVE_ITEM": StandardParser("3+", flags=["REMOVE_ITEM"]),
      "REMOVE_AT": StandardParser("3+", flags=["REMOVE_AT"]),
      "REMOVE_DUPLICATES": StandardParser(2, flags=["REMOVE_DUPLICATES"]),
      "TRANSFORM": StandardParser(
          2, flags=["TRANSFORM"], kwargs=TRANSFORM_KWARGS),
      "REVERSE": StandardParser(2, flags=["REVERSE"]),
      "SORT": StandardParser(2, flags=["SORT"], kwargs=SORT_KWARGS)
  }

  if descriminator not in parsemap:
    logger.warning(
        "Indeterminate form of file() command \"%s\" at %s", descriminator,
        tokens[0].location())
    ctx.lint_ctx.record_lint(
        "E1126", location=descriminator_token.get_location())
    return StandardArgTree.parse(ctx, tokens, npargs='*', kwargs={}, flags=[],
                                 breakstack=breakstack)

  return parsemap[descriminator](ctx, tokens, breakstack)


def populate_db(parse_db):
  parse_db["list"] = parse_list
