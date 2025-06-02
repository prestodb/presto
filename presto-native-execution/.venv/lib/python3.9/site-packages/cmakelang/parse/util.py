# -*- coding: utf-8 -*-
# pylint: disable=too-many-lines
from __future__ import print_function
from __future__ import unicode_literals

import logging
from operator import itemgetter as _itemgetter
import re
import sys

from cmakelang import lex
from cmakelang.common import UserError, InternalError

logger = logging.getLogger(__name__)

ZERO_OR_MORE = '*'
ONE_OR_MORE = '+'

if sys.version_info[0] < 3:
  STRING_TYPES = (str, unicode)
else:
  STRING_TYPES = (str,)

IMPLICIT_PARG_TYPES = STRING_TYPES + (int,)

WHITESPACE_TOKENS = (lex.TokenType.WHITESPACE,
                     lex.TokenType.NEWLINE)

# TODO(josh): Don't include FORMAT_OFF and FORMAT_ON in comment tokens. They
# wont get reflowed within a comment block so there is no reason to parse them
# as such.
COMMENT_TOKENS = (lex.TokenType.COMMENT,)

ONOFF_TOKENS = (lex.TokenType.FORMAT_ON,
                lex.TokenType.FORMAT_OFF)

ALL_COMMENT_TOKENS = (
    lex.TokenType.COMMENT,
    lex.TokenType.BRACKET_COMMENT,
    lex.TokenType.FORMAT_ON,
    lex.TokenType.FORMAT_OFF
)

NON_SEMANTIC_TOKENS = ALL_COMMENT_TOKENS + WHITESPACE_TOKENS


class PositionalSpec(tuple):
  """
  Encapsulates the parse specification for a positional argument group.
  NOTE(josh): this is a named tuple with default arguments and some init
  processing. If it wasn't for the init processing, we could just do:

  PositionalSpec = collections.namedtuple(
    "PositionalSpec", ["nargs", ...])
  PositionalSpec.__new__.__defaults__ = (False, None, None, False)

  But we don't want to self.tags and self.flags to point to a mutable global
  variable...
  """

  def __new__(cls, nargs, sortable=False, tags=None, flags=None, legacy=False,
              max_pargs_hwrap=None, always_wrap=None):
    if not tags:
      tags = []
    if not flags:
      flags = []
    return tuple.__new__(
        cls, (nargs, sortable, tags, flags, legacy, max_pargs_hwrap,
              always_wrap))

  nargs = property(_itemgetter(0))
  npargs = property(_itemgetter(0))
  sortable = property(_itemgetter(1))
  tags = property(_itemgetter(2))
  flags = property(_itemgetter(3))
  legacy = property(_itemgetter(4))
  max_pargs_hwrap = property(_itemgetter(5))
  always_wrap = property(_itemgetter(6))

  def replace(self, **kwargs):
    selfdict = {
        "nargs": self.nargs,
        "sortable": self.sortable,
        "tags": list(self.tags),
        "flags": list(self.flags),
        "legacy": self.legacy,
        "max_pargs_hwrap": self.max_pargs_hwrap,
        "always_wrap": self.always_wrap
    }
    selfdict.update(kwargs)
    return PositionalSpec(**selfdict)


def is_whitespace_token(token):
  return token.type in WHITESPACE_TOKENS


def is_comment_token(token):
  return token.type in COMMENT_TOKENS


def is_syntactic_token(token):
  """Return true for everything that isn't whitespace"""
  if token.type in WHITESPACE_TOKENS:
    return False
  return True


def is_semantic_token(token):
  """Return true for everything that isnt' whitespace or a comment"""
  if token.type in NON_SEMANTIC_TOKENS:
    return False
  return True


def is_variable_dereference(token):
  return token.type is lex.TokenType.DEREF


def is_valid_trailing_comment(token):
  """
  Return true if the token is a valid trailing comment
  """
  return (token.type in (lex.TokenType.COMMENT,
                         lex.TokenType.BRACKET_COMMENT) and
          not comment_is_tag(token))


def is_comment_matching_pattern(token, regex):
  """
  Return true if the token is an explicit trailing comment
  """
  if token.type is not lex.TokenType.COMMENT:
    return False

  return bool(regex.match(token.spelling))


def next_is_explicit_trailing_comment(config, tokens):
  """
  Return true if the next comment is an explicit trailing comment, false
  otherwise
  """

  regex = re.compile("^" + config.markup.explicit_trailing_pattern + ".*")
  for token in iter_syntactic_tokens(tokens):
    return is_comment_matching_pattern(token, regex)
  return False


def are_column_aligned(token_a, token_b):
  """
  Return true if both tokens are on the same column.
  """
  return token_a.begin.col == token_b.begin.col


def next_is_trailing_comment(config, tokens):
  """
  Return true if there is a trailing comment in the token stream
  """

  if not tokens:
    return False

  # If the next token is
  if next_is_explicit_trailing_comment(config, tokens):
    return True

  if is_valid_trailing_comment(tokens[0]):
    return True

  if len(tokens) < 2:
    return False

  if (tokens[0].type == lex.TokenType.WHITESPACE
      and is_valid_trailing_comment(tokens[1])):
    return True
  return False


def comment_is_tag(token):
  """
  Return true if the comment token has one of the tag-forms:

  # cmake-format: <tag>
  # cmf: <tag>
  #[[cmake-format:<tag>]]
  #[[cmf:<tag>]]
  """
  return get_tag(token) is not None


def pargs_are_full(npargs, nconsumed):
  if isinstance(npargs, int):
    return nconsumed >= npargs

  assert isinstance(npargs, STRING_TYPES), (
      "Unexpected npargs type {}".format(type(npargs)))

  if npargs == "?":
    return nconsumed >= 1

  if npargs in ("*", "+"):
    return False

  if npargs.endswith("+"):
    try:
      _ = int(npargs[:-1])
    except ValueError:
      raise ValueError("Unexepected npargs {}".format(npargs))
  return False


def npargs_is_exact(npargs):
  """
  Return true if npargs has an exact specification
  """
  return isinstance(npargs, int)


def get_min_npargs(npargs):
  """If the npargs specification string indicates a minimum number of arguments
     then return that value."""

  if npargs is None:
    return 0

  if isinstance(npargs, int):
    return npargs

  if npargs in ("*", "?"):
    return 0

  if npargs == "+":
    return 1

  if npargs.endswith("+"):
    try:
      return int(npargs[:-1])
    except ValueError:
      pass

  try:
    return int(npargs)
  except ValueError:
    pass

  raise ValueError(
      "Unexpected npargs {}({})".format(npargs, type(npargs).__name__))


LINE_TAG = re.compile(r"#\s*(cmake-format|cmf): ([^\n]*)")
BRACKET_TAG = re.compile(r"#\[(=*)\[(cmake-format|cmf):(.*)\]\1\]")


def get_tag(token):
  """
  If the token is a comment with one of the cmake-format tag forms, then
  extract the tag.
  """
  if token.type is lex.TokenType.COMMENT:
    match = LINE_TAG.match(token.spelling)
    if match:
      return match.group(2).strip().lower()
  elif token.type is lex.TokenType.BRACKET_COMMENT:
    match = BRACKET_TAG.match(token.spelling)
    if match:
      return match.group(3).strip().lower()

  return None


def get_normalized_kwarg(token):
  """
  Return uppercase token spelling if it is a word, otherwise return None
  """

  if (token.type == lex.TokenType.UNQUOTED_LITERAL
      and token.spelling.startswith('-')):
    return token.spelling.lower()

  if token.type != lex.TokenType.WORD:
    return None

  return token.spelling.upper()


def should_break(token, breakstack):
  """
  Return true if any function in breakstack evaluates to true on the current
  token. Otherwise return false.
  """
  for breakcheck in breakstack[::-1]:
    if breakcheck(token):
      return True
  return False


def only_comments_and_whitespace_remain(tokens, breakstack):
  skip_tokens = (lex.TokenType.WHITESPACE,
                 lex.TokenType.NEWLINE,
                 lex.TokenType.COMMENT,
                 lex.TokenType.BRACKET_COMMENT)

  for token in tokens:
    if token.type in skip_tokens:
      continue

    if should_break(token, breakstack):
      return True

    return False
  return True


def iter_syntactic_tokens(tokens):
  """
  Return a generator over the list of tokens yielding only those that are
  not whitespace
  """
  skip_tokens = (lex.TokenType.WHITESPACE,
                 lex.TokenType.NEWLINE)

  for token in tokens:
    if token.type in skip_tokens:
      continue
    yield token


def get_next_syntactic_token(tokens):
  """
  return the first non-whitespace token in the list
  """
  for token in iter_syntactic_tokens(tokens):
    return token
  return None


def iter_semantic_tokens(tokens):
  """
  Return a generator over the list of tokens yielding only those that
  have semantic meaning
  """

  for token in tokens:
    if is_semantic_token(token):
      yield token


def get_nth_semantic_token(tokens, nth):
  idx = 0
  for token in iter_semantic_tokens(tokens):
    if idx == nth:
      return token
    idx += 1
  return None


def get_first_semantic_token(tokens):
  """
  Return the first token with semantic meaning
  """
  return get_nth_semantic_token(tokens, 0)


def comment_belongs_up_tree(ctx, tokens, node, breakstack):
  """
  Return true if the comment token at tokens[0] belongs to some parent in the
  ancestry of `node` rather than to `node` itself. We determine this if the
  following is true:

  1. There is a semantic token remaining in the token stream
  2. The next semantic token would break the current scope of the node
  3. The column of the coment token is to the left of the column where the
     node starts

  e.g.

  ~~~
  statement_name(
    KEYWORD1 argument
             argument
             # comment 1
      # comment 2
    # comment 3
  )
  ~~~

  In this example:

  * comment 1 belongs to the positional group child of the keyword group
  * comment 2 belongs to the keyword group
  * comment 3 belongs to the statement
  """

  next_semantic = get_first_semantic_token(tokens)

  if next_semantic is None:
    # There is no next semantic token. We don't really expect this to happen
    return False

  if not should_break(next_semantic, breakstack):
    # The next token wouldn't break the current node
    return False

  if not ctx.argstack:
    # There is no parent node to assign the comment to, it must belong to the
    # current node
    return False

  return tokens[0].get_location().col < node.get_location().col


def parse_pspec(pargs, flags):
  """
  Parse a positional argument specification.
  """
  out = []

  # Default pargs is "*"
  if pargs is None:
    pargs = ZERO_OR_MORE

  # If we only have one scalar specification, return a legacy specification
  # TODO(josh): should we only do this if we also have flags?
  if isinstance(pargs, STRING_TYPES + (int,)):
    return [PositionalSpec(pargs, flags=flags, legacy=True)]

  if flags:
    raise UserError(
        "Illegal use of top-level 'flags' keyword with new-style positional"
        " argument declaration")

  # If we only have one dictionary specification, then put it in a dictionary
  # so that we can do the rest consistently
  if isinstance(pargs, dict):
    pargs = [pargs]

  for pargdecl in pargs:
    if isinstance(pargdecl, STRING_TYPES + (int,)):
      # A scalar declaration is interpreted as npargs
      out.append(PositionalSpec(pargdecl))
      continue

    if isinstance(pargdecl, dict):
        # A dictionary is interpreted as init kwargs
      if "npargs" not in pargdecl:
        pargdecl = dict(pargdecl)
        pargdecl["nargs"] = ZERO_OR_MORE
      out.append(PositionalSpec(**pargdecl))
      continue

    if isinstance(pargdecl, (list, tuple)):
        # A list or tuple is interpreted as (*args, [kwargs])
      args = list(pargdecl)
      kwargs = {}
      if isinstance(args[-1], dict):
        kwargs = args.pop(-1)
      out.append(PositionalSpec(*args, **kwargs))

  return out


class CommandSpec(object):
  """
  A command specification is composed of a sequence of positional argument
  specifications, and a dictionary mapping keyword arguments to (nested)
  command specifications. It also includes a command (or keyword) name.
  """

  def __init__(self, name, pargs=None, flags=None, kwargs=None, spelling=None,
               max_subgroups_hwrap=None, always_wrap=None):
    super(CommandSpec, self).__init__()
    scalar_types = (int,) + STRING_TYPES

    self.name = name
    self.spelling = spelling
    if spelling is None:
      self.spelling = name
    self.max_subgroups_hwrap = max_subgroups_hwrap
    self.always_wrap = always_wrap

    try:
      self.pargs = parse_pspec(pargs, flags)
    except (TypeError, UserError) as ex:
      message = (
          "Invalid user-supplied specification for positional arguments of "
          "{}:\n{}".format(name, ex))
      raise UserError(message)

    self.kwargs = {}

    if kwargs is not None:
      if isinstance(kwargs, dict):
        items = kwargs.items()
      elif isinstance(kwargs, (list, tuple)):
        items = list(kwargs)
      else:
        raise ValueError(
            "Invalid type {} for kwargs of {}: {}"
            .format(type(kwargs), name, kwargs))

      for keyword, spec in items:
        if isinstance(spec, scalar_types):
          self.kwargs[keyword] = CommandSpec(name=keyword, pargs=spec)
        elif isinstance(spec, CommandSpec):
          self.kwargs[keyword] = spec
        elif isinstance(spec, dict):
          self.kwargs[keyword] = CommandSpec(name=keyword, **spec)
        else:
          raise ValueError("Unexpected type '{}' for kwargs"
                           .format(type(kwargs)))

  def is_flag(self, key):
    return self.kwargs.get(key, None) == 0

  def is_kwarg(self, key):
    subspec = self.kwargs.get(key, None)
    if subspec is None:
      return False
    if isinstance(subspec, int):
      return subspec != 0
    if isinstance(subspec, STRING_TYPES + (CommandSpec,)):
      return True
    raise ValueError("Unexpected kwargspec for {}: {}"
                     .format(key, type(subspec)))

  def add(self, name, pargs=None, flags=None, kwargs=None, **extra):
    self.kwargs[name.lower()] = CommandSpec(
        name, pargs, flags, kwargs, **extra)


def apply_overrides(spectree, pathkeystr, value):
  """Apply command specification overrides to a previously constructed
     parse function tree."""

  if "." not in pathkeystr:
    raise UserError("Invalid override key {}".format(pathkeystr))
  pathstr, keystr = pathkeystr.rsplit(".", 1)

  pathparts = pathstr.split("/")

  while pathparts:
    if not isinstance(spectree, CommandSpec):
      raise InternalError("Invalid spectree entry of type {}"
                          .format(type(spectree)))

    pathkey = pathparts.pop(0)
    if pathkey.startswith("parg"):
      if pathparts:
        raise UserError(
            "Invalid override key {} contains path components after pspec"
            .format(pathkeystr))
      try:
        idx = int(pathkey[len("parg"):].strip("[]"))
      except ValueError:
        raise UserError(
            "Invalid override key {} contains non-int pspec"
            .format(pathkeystr))
      if idx > len(spectree.pargs):
        raise UserError(
            "Invalid override key {} is out of bounds"
            .format(pathkeystr))

      repl = {keystr: value}
      spectree.pargs[idx] = spectree.pargs[idx].replace(**repl)
      return

    if pathkey not in spectree.kwargs:
      raise UserError(
          "Invalid override key {} at {}".format(pathkeystr, pathkey))
    spectree = spectree.kwargs[pathkey]

  if not hasattr(spectree, keystr):
    raise UserError(
        "Invalid override key {}, keypart {} is not valid for type {}"
        .format(pathkeystr, keystr, type(spectree)))

  try:
    setattr(spectree, keystr, value)
  except AttributeError:
    raise UserError(
        "Invalid override key {}, can't set attribute {} for type {}"
        .format(pathkeystr, keystr, type(spectree)))
