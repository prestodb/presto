# -*- coding: utf-8 -*-
"""
Functions for parsing comments in markup
"""
from __future__ import unicode_literals
import math
import textwrap
import re
from cmakelang import common

# Matches comment strings like ``# TODO(josh):`` or ``# NOTE(josh):``
NOTE_REGEX = re.compile(r'^\s*[A-Z_]+\([^)]+\):.*')

# Matches comment lines that are clearly meant to separate sections or
# headers. The meaning of this regex is "a line consisting of three or more
# non-word characters ending with three or more non-word characters"
RULER_PATTERN = r'^\s*[^\w\s]{3}.*[^\w\s]{3}$'
RULER_REGEX = re.compile(RULER_PATTERN)

# Matches lines that start a bulleted list
BULLET_REGEX = re.compile(r'^(\s*)([\*-])( .+)$')

# Matches lines that start an itemized list
ENUM_REGEX = re.compile(r'^(\s*)\d+([.:])( .+)$')

# Matches a verbatim fence
FENCE_PATTERN = r'^\s*([`~]{3}[`~]*)(.*)$'
FENCE_REGEX = re.compile(FENCE_PATTERN)


class CommentType(common.EnumObject):
  _id_map = {}


CommentType.PARAGRAPH = CommentType(0)
CommentType.BULLET_LIST = CommentType(1)
CommentType.ENUM_LIST = CommentType(2)
CommentType.NOTE = CommentType(3)
CommentType.RULER = CommentType(4)
CommentType.SEPARATOR = CommentType(5)
CommentType.FENCE = CommentType(6)
CommentType.VERBATIM = CommentType(7)


class CommentItem(object):

  def __init__(self, kind):
    self.kind = kind
    self.indent = None
    self.lines = []

  def __repr__(self):
    return "{}".format(self.kind.name)


def parse(lines, config=None):
  """
  Parse comment lines. Returns objects of different formatable entities
  """
  # pylint: disable=too-many-statements

  obj_list = []
  state = None
  bullet_regex = None

  if config is None:
    fence_re = FENCE_REGEX
    ruler_re = RULER_REGEX
  else:
    fence_re = re.compile(config.markup.fence_pattern)
    ruler_re = re.compile(config.markup.ruler_pattern)

  for line in lines:
    fence_match = fence_re.match(line)
    if fence_match:
      obj_list.append(CommentItem(CommentType.FENCE))
      obj_list[-1].lines.append(fence_match.group(1).strip())
      content = fence_match.group(2).strip()
      line = content
      if state == CommentType.VERBATIM:
        state = None
      else:
        obj_list.append(CommentItem(CommentType.VERBATIM))
        state = CommentType.VERBATIM
      if not line:
        continue

    if state == CommentType.VERBATIM:
      if line and line[0] == ' ':
        obj_list[-1].lines.append(line[1:])
      else:
        obj_list[-1].lines.append(line)
      continue

    if not line:
      if state is CommentType.SEPARATOR:
        continue

      obj_list.append(CommentItem(CommentType.SEPARATOR))
      state = CommentType.SEPARATOR
      continue

    if ruler_re.match(line):
      obj_list.append(CommentItem(CommentType.RULER))
      obj_list[-1].lines.append(line.strip())
      state = CommentType.RULER
      continue

    if state in (None, CommentType.SEPARATOR, CommentType.RULER):
      match = BULLET_REGEX.match(line)
      if match:
        obj_list.append(CommentItem(CommentType.BULLET_LIST))
        indent_str = match.group(1)
        bullet_punctuation = match.group(2)
        obj_list[-1].lines.append(match.group(3))
        obj_list[-1].indent = len(indent_str)
        state = CommentType.BULLET_LIST

        if bullet_punctuation == '*':
          bullet_punctuation = r'\*'
        bullet_regex = re.compile(
            '^{}{}( .*)$'.format(indent_str, bullet_punctuation))
        continue

      match = ENUM_REGEX.match(line)
      if match:
        obj_list.append(CommentItem(CommentType.ENUM_LIST))
        indent_str = match.group(1)
        bullet_punctuation = match.group(2)
        obj_list[-1].lines.append(match.group(3).strip())
        obj_list[-1].indent = len(indent_str)
        state = CommentType.ENUM_LIST

        # TODO(josh) We want to match lines with either the same number of
        # spaces or with the colon in the same column
        bullet_regex = re.compile(
            r'^{}\d+{}( .*)$'.format(indent_str, bullet_punctuation))
        continue

      if NOTE_REGEX.match(line):
        obj_list.append(CommentItem(CommentType.NOTE))
        obj_list[-1].lines.append(line.strip())
        state = CommentType.NOTE
        continue

      if ruler_re.match(line):
        obj_list.append(CommentItem(CommentType.RULER))
        obj_list[-1].lines.append(line.strip())
        state = CommentType.RULER
        continue

      state = CommentType.PARAGRAPH
      obj_list.append(CommentItem(CommentType.PARAGRAPH))
      obj_list[-1].lines.append(line.strip())

    elif state in (CommentType.PARAGRAPH, CommentType.NOTE):
      if NOTE_REGEX.match(line):
        obj_list.append(CommentItem(CommentType.NOTE))
        state = CommentType.NOTE
      elif RULER_REGEX.match(line):
        obj_list.append(CommentItem(CommentType.RULER))
        state = CommentType.RULER
      obj_list[-1].lines.append(line.strip())

    elif state in (CommentType.BULLET_LIST, CommentType.ENUM_LIST):
      match = bullet_regex.match(line)
      if match:
        obj_list[-1].lines.append(match.group(1).strip())
      else:
        obj_list[-1].lines[-1] += '\n' + line.strip()

  return obj_list


def is_hashruler(item):
  """
  Return true if the markup item is a hash ruler, i.e.::

      ###########################
      # Like this ^^^ or this vvv
      ###########################
  """
  if item.kind != CommentType.RULER:
    return False
  if len(item.lines) != 1:
    return False
  if item.lines[0].strip('#'):
    return False
  return True


COMMON_KWARGS = dict(
    expand_tabs=True,
    replace_whitespace=True,
    drop_whitespace=True,
    break_long_words=False,
    break_on_hyphens=False
)


def format_item(config, line_width, item):
  """
  Return lines of formatted text based on the typeof markup
  """

  if item.kind == CommentType.SEPARATOR:
    return ['']
  if item.kind == CommentType.FENCE:
    return ['~~~']
  if item.kind == CommentType.VERBATIM:
    return [line.rstrip() for line in item.lines]
  if is_hashruler(item) and config.markup.canonicalize_hashrulers:
    return ['#' * line_width]
  if item.kind in (CommentType.PARAGRAPH, CommentType.NOTE, CommentType.RULER):
    wrapper = textwrap.TextWrapper(width=line_width, **COMMON_KWARGS)
    return common.stable_wrap(wrapper, '\n'.join(item.lines).strip())

  if item.kind == CommentType.BULLET_LIST:
    assert line_width > 2
    outlines = []

    wrapper = textwrap.TextWrapper(width=line_width - 2, **COMMON_KWARGS)
    for line in item.lines:
      increment_lines = common.stable_wrap(wrapper, line.strip())
      outlines.append(config.markup.bullet_char + ' ' + increment_lines[0])
      outlines.extend('  ' + iline for iline in increment_lines[1:])
    return outlines

  if item.kind == CommentType.ENUM_LIST:
    assert line_width > 2
    outlines = []
    wrapper = textwrap.TextWrapper(width=line_width - 2, **COMMON_KWARGS)

    digits = int(math.ceil(math.log(len(item.lines), 10)))
    fmt = '{:%dd}%s ' % (digits, config.markup.enum_char)
    indent = ' ' * (digits + 2)

    for idx, line in enumerate(item.lines):
      increment_lines = common.stable_wrap(wrapper, line.strip())
      outlines.append(fmt.format(idx + 1) + increment_lines[0])
      outlines.extend(indent + iline for iline in increment_lines[1:])
    return outlines

  raise AssertionError('Unexepected case')


def format_items(config, line_width, items):
  """
  Return lines of formatted text for the sequence of items within a comment
  block
  """

  outlines = []
  indent_history = []
  for item in items:
    if item.kind in (CommentType.BULLET_LIST, CommentType.ENUM_LIST):
      while indent_history and indent_history[-1] >= item.indent:
        indent_history.pop(-1)
      indent_history.append(item.indent)
      nindent = 2 * (len(indent_history) - 1)
      ilines = format_item(config, line_width - nindent, item)
      outlines.extend(' ' * nindent + iline for iline in ilines)
    else:
      outlines.extend(format_item(config, line_width, item))
      if item.kind != CommentType.SEPARATOR:
        indent_history = []
  return outlines
