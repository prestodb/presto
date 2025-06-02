import logging

from cmakelang.lex import TokenType
from cmakelang.parse.additional_nodes import PatternNode
from cmakelang.parse.argument_nodes import (
    ArgGroupNode,
    ConditionalGroupNode,
    PositionalGroupNode,
    PositionalParser,
    StandardArgTree,
    StandardParser)
from cmakelang.parse.util import get_first_semantic_token
from cmakelang.parse.simple_nodes import consume_whitespace_and_comments

logger = logging.getLogger(__name__)


def parse_file_read(ctx, tokens, breakstack):
  """
  ::

    file(READ <filename> <variable>
         [OFFSET <offset>] [LIMIT <max-in>] [HEX])

  :see: https://cmake.org/cmake/help/v3.14/command/file.html#read
  """

  return StandardArgTree.parse(
      ctx, tokens,
      npargs='*',
      kwargs={
          "OFFSET": PositionalParser(1),
          "LIMIT": PositionalParser(1),
      },
      flags=[
          "READ",
          "HEX"
      ],
      breakstack=breakstack)


def parse_file_strings(ctx, tokens, breakstack):
  """
  ::

    file(STRINGS <filename> <variable> [<options>...])

  :see: https://cmake.org/cmake/help/v3.14/command/file.html#read
  """

  return StandardArgTree.parse(
      ctx, tokens,
      npargs='*',
      kwargs={
          "LENGTH_MAXIMUM": PositionalParser(1),
          "LENGTH_MINIMUM": PositionalParser(1),
          "LIMIT_COUNT": PositionalParser(1),
          "LIMIT_INPUT": PositionalParser(1),
          "LIMIT_OUTPUT": PositionalParser(1),
          "REGEX": PositionalParser(1),
          "ENCODING": PositionalParser(1, flags=[
              "UTF-8", "UTF-16LE", "UTF-16BE", "UTF-32LE", "UTF-32BE"
          ]),
      },
      flags=[
          "STRINGS",
          "NEWLINE_CONSUME",
          "NO_HEX_CONVERSION",

      ],
      breakstack=breakstack)


HASH_STRINGS = [
    "MD5",
    "SHA1",
    "SHA224",
    "SHA256",
    "SHA384",
    "SHA512",
    "SHA3_224",
    "SHA3_256",
    "SHA3_384",
    "SHA3_512",
]


def parse_file_hash(ctx, tokens, breakstack):
  """
  ::

    file(<HASH> <filename> <variable>)

  :see: https://cmake.org/cmake/help/v3.14/command/file.html#strings
  """

  return StandardArgTree.parse(
      ctx, tokens,
      npargs=3,
      kwargs={},
      flags=HASH_STRINGS,
      breakstack=breakstack)


def parse_file_timestamp(ctx, tokens, breakstack):
  """
  ::

    file(TIMESTAMP <filename> <variable> [<format>] [UTC])

  :see: https://cmake.org/cmake/help/v3.14/command/file.html#strings
  """

  return StandardArgTree.parse(
      ctx, tokens,
      npargs='+',
      kwargs={},
      flags=["TIMESTAMP", "UTC"],
      breakstack=breakstack)


def parse_file_write(ctx, tokens, breakstack):
  """
  ::

    file(WRITE <filename> <content>...)
    file(APPEND <filename> <content>...)

  :see: https://cmake.org/cmake/help/v3.14/command/file.html#writing
  """
  tree = ArgGroupNode()
  consume_whitespace_and_comments(ctx, tokens, tree)
  tree.children.append(
      PositionalGroupNode.parse(
          ctx, tokens, 2, ["WRITE", "APPEND"], breakstack))
  consume_whitespace_and_comments(ctx, tokens, tree)
  tree.children.append(
      PositionalGroupNode.parse(ctx, tokens, '+', [], breakstack))

  return tree


def parse_file_generate_output(ctx, tokens, breakstack):
  """
  ::

    file(GENERATE OUTPUT output-file
        <INPUT input-file|CONTENT content>
        [CONDITION expression])

  :see: https://cmake.org/cmake/help/v3.14/command/file.html#writing
  """

  return StandardArgTree.parse(
      ctx, tokens,
      npargs=1,
      kwargs={
          "OUTPUT": PositionalParser(1),
          "INPUT": PositionalParser(1),
          "CONTENT": PositionalParser('+'),
          "CONDITION": ConditionalGroupNode.parse,
      },
      flags=["GENERATE"],
      breakstack=breakstack)


def parse_file_glob(ctx, tokens, breakstack):
  """
  ::

    file(GLOB <variable>
        [LIST_DIRECTORIES true|false] [RELATIVE <path>] [CONFIGURE_DEPENDS]
        [<globbing-expressions>...])
    file(GLOB_RECURSE <variable> [FOLLOW_SYMLINKS]
        [LIST_DIRECTORIES true|false] [RELATIVE <path>] [CONFIGURE_DEPENDS]
        [<globbing-expressions>...])
  :see: https://cmake.org/cmake/help/v3.14/command/file.html#filesystem
  """

  return StandardArgTree.parse(
      ctx, tokens,
      npargs='+',
      kwargs={
          "LIST_DIRECTORIES": PositionalParser(1),
          "RELATIVE": PositionalParser(1)
      },
      flags=[
          "GLOB",
          "GLOB_RECURSE",
          "CONFIGURE_DEPENDS",
          "FOLLOW_SYMLINKS"],
      breakstack=breakstack)


def parse_file_copy(ctx, tokens, breakstack):
  """
  ::

    file(<COPY|INSTALL> <files>... DESTINATION <dir>
         [FILE_PERMISSIONS <permissions>...]
         [DIRECTORY_PERMISSIONS <permissions>...]
         [NO_SOURCE_PERMISSIONS] [USE_SOURCE_PERMISSIONS]
         [FILES_MATCHING]
         [[PATTERN <pattern> | REGEX <regex>]
          [EXCLUDE] [PERMISSIONS <permissions>...]] [...])

  :see: https://cmake.org/cmake/help/v3.14/command/file.html#filesystem
  """

  return StandardArgTree.parse(
      ctx, tokens,
      npargs='*',
      kwargs={
          "COPY": PositionalParser('*'),
          "DESTINATION": PositionalParser(1),
          "FILE_PERMISSIONS":
              PositionalParser(
                  '+',
                  flags=["OWNER_READ", "OWNER_WRITE", "OWNER_EXECUTE",
                         "GROUP_READ", "GROUP_WRITE", "GROUP_EXECUTE",
                         "WORLD_READ", "WORLD_WRITE", "WORLD_EXECUTE",
                         "SETUID", "SETGID"]),
          "DIRECTORY_PERMISSIONS": PositionalParser('+'),
          "PATTERN": PatternNode.parse,
          "REGEX": PatternNode.parse,
      },
      flags=[
          "INSTALL",
          "NO_SOURCE_PERMISSIONS",
          "USE_SOURCE_PERMISSIONS",
          "FILES_MATCHING",
      ],
      breakstack=breakstack)


def parse_file_create_link(ctx, tokens, breakstack):
  """
  ::

    file(CREATE_LINK <original> <linkname>
        [RESULT <result>] [COPY_ON_ERROR] [SYMBOLIC])

  :see: https://cmake.org/cmake/help/v3.14/command/file.html#filesystem
  """

  return StandardArgTree.parse(
      ctx, tokens,
      npargs='+',
      kwargs={
          "RESULT": PositionalParser(1),
      },
      flags=[
          "COPY_ON_ERROR",
          "SYMBOLIC",
      ],
      breakstack=breakstack)


def parse_file_xfer(ctx, tokens, breakstack):
  """
  ::

    file(DOWNLOAD <url> <file> [<options>...])
    file(UPLOAD   <file> <url> [<options>...])

  :see: https://cmake.org/cmake/help/v3.14/command/file.html#transfer
  """

  return StandardArgTree.parse(
      ctx, tokens,
      npargs=3,
      kwargs={
          "INACTIVITY_TIMEOUT": PositionalParser(1),
          "LOG": PositionalParser(1),
          "STATUS": PositionalParser(1),
          "TIMEOUT": PositionalParser(1),
          "USERPWD": PositionalParser(1),
          "HTTPHEADER": PositionalParser(1),
          "NETRC": PositionalParser(
              1, flags=["CMAKE_NETRC", "IGNORED", "OPTIONAL", "REQUIRED"]),
          "NETRC_FILE": PositionalParser(1),
          "EXPECTED_HASH": PositionalParser(1),
          "EXPECTED_MD5": PositionalParser(1),
          "TLS_VERIFY": PositionalParser(1),
          "TLS_CAINFO": PositionalParser(1),
      },
      flags=[
          "DOWNLOAD",
          "UPLOAD",
          "SHOW_PROGRESS",

      ],
      breakstack=breakstack)


def parse_file_lock(ctx, tokens, breakstack):
  """
  ::

    file(LOCK <path> [DIRECTORY] [RELEASE]
         [GUARD <FUNCTION|FILE|PROCESS>]
         [RESULT_VARIABLE <variable>]
         [TIMEOUT <seconds>])

  :see: https://cmake.org/cmake/help/v3.14/command/file.html#locking
  """

  return StandardArgTree.parse(
      ctx, tokens,
      npargs='+',
      kwargs={
          "GUARD": PositionalParser(1, flags=["FUNCTION", "FILE", "PROCESS"]),
          "RESULT_VARIABLE": PositionalParser(1),
          "TIMEOUT": PositionalParser(1)
      },
      flags=[
          "LOCK",
          "DIRECTORY",
          "RELEASE",
      ],
      breakstack=breakstack)


def parse_file(ctx, tokens, breakstack):
  """
  The ``file()`` command has a lot of different forms, depending on the first
  argument. This function just dispatches the correct parse implementation for
  the given form::

    Reading
      file(READ <filename> <out-var> [...])
      file(STRINGS <filename> <out-var> [...])
      file(<HASH> <filename> <out-var>)
      file(TIMESTAMP <filename> <out-var> [...])

    Writing
      file({WRITE | APPEND} <filename> <content>...)
      file({TOUCH | TOUCH_NOCREATE} [<file>...])
      file(GENERATE OUTPUT <output-file> [...])

    Filesystem
      file({GLOB | GLOB_RECURSE} <out-var> [...] [<globbing-expr>...])
      file(RENAME <oldname> <newname>)
      file({REMOVE | REMOVE_RECURSE } [<files>...])
      file(MAKE_DIRECTORY [<dir>...])
      file({COPY | INSTALL} <file>... DESTINATION <dir> [...])
      file(SIZE <filename> <out-var>)
      file(READ_SYMLINK <linkname> <out-var>)
      file(CREATE_LINK <original> <linkname> [...])

    Path Conversion
      file(RELATIVE_PATH <out-var> <directory> <file>)
      file({TO_CMAKE_PATH | TO_NATIVE_PATH} <path> <out-var>)

    Transfer
      file(DOWNLOAD <url> <file> [...])
      file(UPLOAD <file> <url> [...])

    Locking
      file(LOCK <path> [...])

  :see: https://cmake.org/cmake/help/v3.14/command/file.html
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
    return StandardArgTree.parse(ctx, tokens, npargs='*', kwargs={}, flags=[],
                                 breakstack=breakstack)

  descriminator = descriminator_token.spelling.upper()
  parsemap = {
      "READ": parse_file_read,
      "STRINGS": parse_file_strings,
      "TIMESTAMP": parse_file_timestamp,
      "WRITE": parse_file_write,
      "APPEND": parse_file_write,
      "TOUCH": StandardParser('+', flags=["TOUCH"]),
      "TOUCH_NOCREATE": StandardParser('+', flags=["TOUCH_NOCREATE"]),
      "GENERATE": parse_file_generate_output,
      "GLOB": parse_file_glob,
      "GLOB_RECURSE": parse_file_glob,
      "RENAME": StandardParser(3, flags=["RENAME"]),
      "REMOVE": StandardParser('+', flags=["REMOVE"]),
      "REMOVE_RECURSE": StandardParser('+', flags=["REMOVE_RECURSE"]),
      "MAKE_DIRECTORY": StandardParser('+', flags=["MAKE_DIRECTORY"]),
      "COPY": parse_file_copy,
      "INSTALL": parse_file_copy,
      "SIZE": StandardParser(3, flags=["SIZE"]),
      "READ_SYMLINK": StandardParser(3, flags=["READ_SYMLINK"]),
      "CREATE_LINK": parse_file_create_link,
      "RELATIVE_PATH": StandardParser(4, flags=["RELATIVE_PATH"]),
      "TO_CMAKE_PATH": StandardParser(3, flags=["TO_CMAKE_PATH"]),
      "TO_NATIVE_PATH": StandardParser(3, flags=["TO_NATIVE_PATH"]),
      "DOWNLOAD": parse_file_xfer,
      "UPLOAD": parse_file_xfer,
      "LOCK": parse_file_lock
  }
  for hashname in HASH_STRINGS:
    parsemap[hashname] = parse_file_hash

  if descriminator not in parsemap:
    # logger.warning(
    #     "Indeterminate form of file() command \"%s\" at %s", descriminator,
    #     tokens[0].location())
    ctx.lint_ctx.record_lint(
        "E1126", location=descriminator_token.get_location())
    return StandardArgTree.parse(ctx, tokens, npargs='*', kwargs={}, flags=[],
                                 breakstack=breakstack)

  return parsemap[descriminator](ctx, tokens, breakstack)


def populate_db(parse_db):
  parse_db["file"] = parse_file
