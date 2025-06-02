# -*- coding: utf-8 -*-
# pylint: disable=too-many-lines
from __future__ import print_function
from __future__ import unicode_literals

import io
import sys

from cmakelang.parse.common import TreeNode


def dump_tree(nodes, outfile=None, indent=None):
  """
  Print a tree of node objects for debugging purposes
  """

  if indent is None:
    indent = ''

  if outfile is None:
    outfile = sys.stdout

  for idx, node in enumerate(nodes):
    outfile.write(indent)
    if idx + 1 == len(nodes):
      outfile.write('└─ ')
    else:
      outfile.write('├─ ')
    noderep = repr(node)
    if sys.version_info[0] < 3:
      # python2
      outfile.write(getattr(noderep, 'decode')('utf-8'))
    else:
      # python3
      outfile.write(noderep)
    outfile.write('\n')

    if not hasattr(node, 'children'):
      continue

    if idx + 1 == len(nodes):
      dump_tree(node.children, outfile, indent + '    ')
    else:
      dump_tree(node.children, outfile, indent + '│   ')


def dump_tree_tostr(nodes, indent=None):
  outfile = io.StringIO()
  dump_tree(nodes, outfile, indent)
  return outfile.getvalue()


def dump_tree_upto(nodes, history, outfile=None, indent=None):
  """
  Print a tree of node objects for debugging purposes
  """

  if indent is None:
    indent = ''

  if outfile is None:
    outfile = sys.stdout

  for idx, node in enumerate(nodes):
    outfile.write(indent)
    if idx + 1 == len(nodes):
      outfile.write('└─ ')
    else:
      outfile.write('├─ ')

    if history[0] is node:
      # ANSI red
      outfile.write("\u001b[31m")
      if sys.version_info[0] < 3:
        outfile.write(repr(node).decode('utf-8'))
      else:
        outfile.write(repr(node))
      # ANSI reset
      outfile.write("\u001b[0m")
    else:
      if sys.version_info[0] < 3:
        outfile.write(repr(node).decode('utf-8'))
      else:
        outfile.write(repr(node))
    outfile.write('\n')

    if not hasattr(node, 'children'):
      continue

    subhistory = history[1:]
    if not subhistory:
      continue

    if idx + 1 == len(nodes):
      dump_tree_upto(node.children, subhistory, outfile, indent + '    ')
    else:
      dump_tree_upto(node.children, subhistory, outfile, indent + '│   ')


def tree_string(nodes, history=None):
  outfile = io.StringIO()
  if history:
    dump_tree_upto(nodes, history, outfile)
  else:
    dump_tree(nodes, outfile)
  return outfile.getvalue()


def has_nontoken_children(node):
  if not hasattr(node, 'children'):
    return False

  for child in node.children:
    if isinstance(child, TreeNode):
      return True

  return False


def dump_tree_for_test(nodes, outfile=None, indent=None, increment=None):
  """
  Print a tree of node objects for debugging purposes
  """
  if increment is None:
    increment = '    '

  if indent is None:
    indent = ''

  if outfile is None:
    outfile = sys.stdout

  for node in nodes:
    if not isinstance(node, TreeNode):
      continue
    outfile.write(indent)
    outfile.write('({}, ['.format(node.node_type))
    if has_nontoken_children(node):
      outfile.write('\n')
      dump_tree_for_test(node.children, outfile, indent + increment, increment)
      outfile.write(indent)
    outfile.write(']),\n')


def test_string(nodes, indent=None, increment=None):
  if indent is None:
    indent = ' ' * 10
  if increment is None:
    increment = '    '

  outfile = io.StringIO()
  dump_tree_for_test(nodes, outfile, indent, increment)
  return outfile.getvalue()
