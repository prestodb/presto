# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import contextlib
import logging
import unittest
import sys

from cmakelang.format import __main__
from cmakelang import configuration
from cmakelang import lex
from cmakelang import parse
from cmakelang.format import formatter
from cmakelang.parse.common import NodeType


def strip_indent(content, indent=6):
  """
  Strings used in this file are indented by 6-spaces to keep them readable
  within the python code that they are embedded. Remove those 6-spaces from
  the front of each line before running the tests.
  """

  # NOTE(josh): don't use splitlines() so that we get the same result
  # regardless of windows or unix line endings in content.
  return '\n'.join([line[indent:] for line in content.split('\n')])


def overzip(iterable_a, iterable_b):
  """
  Like itertools.izip but instead if the two lists have different sizes then
  the resulting generator will yield a number of pairs equal to the larger of
  the two inputs (rathe than the smaller). The empty list will be padded with
  None elements.
  """
  iter_a = iter(iterable_a)
  iter_b = iter(iterable_b)

  item_a = next(iter_a, None)
  item_b = next(iter_b, None)

  while item_a is not None and item_b is not None:
    yield(item_a, item_b)
    item_a = next(iter_a, None)
    item_b = next(iter_b, None)

  while item_a is not None:
    yield(item_a, None)
    item_a = next(iter_a, None)

  while item_b is not None:
    yield(None, item_b)
    item_b = next(iter_b, None)


def assert_tree(test, nodes, tups, tree=None, history=None):
  if tree is None:
    tree = nodes
  if history is None:
    history = []

  for node, tup in overzip(nodes, tups):
    if isinstance(node, lex.Token):
      continue
    subhistory = history + [node]
    message = (" for node {} at\n {} \n\n\n"
               "If the actual result is expected, then update the test with"
               " this:\n# pylint: disable=bad-continuation\n# noqa: E122\n{}"
               .format(node,
                       formatter.tree_string(tree, subhistory),
                       formatter.test_string(tree)))
    test.assertIsNotNone(node, msg="Missing node" + message)
    test.assertIsNotNone(tup, msg="Extra node" + message)
    if len(tup) == 6:
      ntype, wrap, row, col, colextent, expect_children = tup
      test.assertEqual(node.passno, wrap,
                       msg="Expected wrap={}".format(wrap) + message)
    else:
      ntype, row, col, colextent, expect_children = tup

    test.assertEqual(node.node_type, ntype,
                     msg="Expected type={}".format(ntype) + message)
    test.assertEqual(node.position[0], row,
                     msg="Expected row={}".format(row) + message)
    test.assertEqual(node.position[1], col,
                     msg="Expected col={}".format(col) + message)
    test.assertEqual(node.colextent, colextent,
                     msg="Expected colextent={}".format(colextent) + message)
    assert_tree(test, node.children, expect_children, tree, subhistory)


class TestCanonicalLayout(unittest.TestCase):
  """
  Given a bunch of example inputs, ensure that the output is as expected.
  """

  def __init__(self, *args, **kwargs):
    super(TestCanonicalLayout, self).__init__(*args, **kwargs)
    self.config = configuration.Configuration()
    parse_db = parse.funs.get_parse_db()
    self.parse_ctx = parse.ParseContext(parse_db)

  def setUp(self):
    self.config.parse.fn_spec.add(
        'foo',
        flags=['BAR', 'BAZ'],
        kwargs={
            "HEADERS": '*',
            "SOURCES": '*',
            "DEPENDS": '*'
        })

    self.parse_ctx.parse_db.update(
        parse.funs.get_funtree(self.config.parse.fn_spec))

  def tearDown(self):
    pass

  @contextlib.contextmanager
  def subTest(self, msg=None, **params):
    # pylint: disable=no-member
    if sys.version_info < (3, 4, 0):
      yield None
    else:
      yield super(TestCanonicalLayout, self).subTest(msg=msg, **params)

  def do_layout_test(self, input_str, expect_tree, strip_len=6):
    """
    Run the formatter on the input string and assert that the result matches
    the output string
    """

    input_str = strip_indent(input_str, strip_len)
    tokens = lex.tokenize(input_str)
    parse_tree = parse.parse(tokens, self.parse_ctx)
    box_tree = formatter.layout_tree(parse_tree, self.config)
    assert_tree(self, [box_tree], expect_tree)

  def test_simple_statement(self):
    self.do_layout_test("""\
      cmake_minimum_required(VERSION 2.8.11)
      """, [
# pylint: disable=bad-continuation
# noqa: E122
(NodeType.BODY, 0, 0, 0, 38, [
  (NodeType.STATEMENT, 0, 0, 0, 38, [
    (NodeType.FUNNAME, 0, 0, 0, 22, []),
    (NodeType.LPAREN, 0, 0, 22, 23, []),
    (NodeType.ARGGROUP, 0, 0, 23, 37, [
      (NodeType.KWARGGROUP, 0, 0, 23, 37, [
        (NodeType.KEYWORD, 0, 0, 23, 30, []),
        (NodeType.ARGGROUP, 0, 0, 31, 37, [
          (NodeType.PARGGROUP, 0, 0, 31, 37, [
            (NodeType.ARGUMENT, 0, 0, 31, 37, []),
          ]),
        ]),
      ]),
    ]),
    (NodeType.RPAREN, 0, 0, 37, 38, []),
  ]),
]),
      ])

  def test_collapse_additional_newlines(self):
    self.do_layout_test("""\
      # The following multiple newlines should be collapsed into a single newline




      cmake_minimum_required(VERSION 2.8.11)
      """, [
# pylint: disable=bad-continuation
# noqa: E122
(NodeType.BODY, 0, 0, 0, 75, [
  (NodeType.COMMENT, 0, 0, 0, 75, []),
  (NodeType.WHITESPACE, 0, 1, 0, 0, []),
  (NodeType.STATEMENT, 0, 2, 0, 38, [
    (NodeType.FUNNAME, 0, 2, 0, 22, []),
    (NodeType.LPAREN, 0, 2, 22, 23, []),
    (NodeType.ARGGROUP, 0, 2, 23, 37, [
      (NodeType.KWARGGROUP, 0, 2, 23, 37, [
        (NodeType.KEYWORD, 0, 2, 23, 30, []),
        (NodeType.ARGGROUP, 0, 2, 31, 37, [
          (NodeType.PARGGROUP, 0, 2, 31, 37, [
            (NodeType.ARGUMENT, 0, 2, 31, 37, []),
          ]),
        ]),
      ]),
    ]),
    (NodeType.RPAREN, 0, 2, 37, 38, []),
  ]),
]),
      ])

  def test_multiline_reflow(self):
    self.do_layout_test("""\
      # This multiline-comment should be reflowed
      # into a single comment
      # on one line
      """, [
          (NodeType.BODY, 0, 0, 77, [
              (NodeType.COMMENT, 0, 0, 77, []),
          ])
      ])

  def test_long_args_command_split(self):
    self.do_layout_test("""\
      # This very long command should be split to multiple lines
      set(HEADERS very_long_header_name_a.h very_long_header_name_b.h very_long_header_name_c.h)
      """, [
# pylint: disable=bad-continuation
# pylint: disable=bad-continuation
# noqa: E122
(NodeType.BODY, 0, 0, 0, 63, [
  (NodeType.COMMENT, 0, 0, 0, 58, []),
  (NodeType.STATEMENT, 0, 1, 0, 63, [
    (NodeType.FUNNAME, 0, 1, 0, 3, []),
    (NodeType.LPAREN, 0, 1, 3, 4, []),
    (NodeType.ARGGROUP, 0, 1, 4, 63, [
      (NodeType.PARGGROUP, 0, 1, 4, 11, [
        (NodeType.ARGUMENT, 0, 1, 4, 11, []),
      ]),
      (NodeType.PARGGROUP, 0, 1, 12, 63, [
        (NodeType.ARGUMENT, 0, 1, 12, 37, []),
        (NodeType.ARGUMENT, 0, 1, 38, 63, []),
        (NodeType.ARGUMENT, 0, 2, 12, 37, []),
      ]),
    ]),
    (NodeType.RPAREN, 0, 2, 37, 38, []),
  ]),
]),
      ])

  def test_long_arg_on_newline(self):
    self.do_layout_test("""\
      # This command has a very long argument and can't be aligned with the command
      # end, so it should be moved to a new line with block indent + 1.
      some_long_command_name("Some very long argument that really needs to be on the next line.")
      """, [
# pylint: disable=bad-continuation
# noqa: E122
(NodeType.BODY, 0, 0, 0, 77, [
  (NodeType.COMMENT, 0, 0, 0, 77, []),
  (NodeType.STATEMENT, 1, 2, 0, 70, [
    (NodeType.FUNNAME, 0, 2, 0, 22, []),
    (NodeType.LPAREN, 0, 2, 22, 23, []),
    (NodeType.ARGGROUP, 0, 3, 2, 69, [
      (NodeType.PARGGROUP, 0, 3, 2, 69, [
        (NodeType.ARGUMENT, 0, 3, 2, 69, []),
      ]),
    ]),
    (NodeType.RPAREN, 0, 3, 69, 70, []),
  ]),
]),
      ])

  def test_argcomment_preserved_and_reflowed(self):
    self.do_layout_test("""\
      set(HEADERS header_a.h header_b.h # This comment should
                                        # be preserved, moreover it should be split
                                        # across two lines.
          header_c.h header_d.h)
      """, [
# pylint: disable=bad-continuation
# noqa: E122
(NodeType.BODY, 0, 0, 0, 80, [
  (NodeType.STATEMENT, 4, 0, 0, 80, [
    (NodeType.FUNNAME, 0, 0, 0, 3, []),
    (NodeType.LPAREN, 0, 0, 3, 4, []),
    (NodeType.ARGGROUP, 4, 0, 4, 80, [
      (NodeType.PARGGROUP, 0, 0, 4, 11, [
        (NodeType.ARGUMENT, 0, 0, 4, 11, []),
      ]),
      (NodeType.PARGGROUP, 0, 1, 4, 80, [
        (NodeType.ARGUMENT, 0, 1, 4, 14, []),
        (NodeType.ARGUMENT, 0, 1, 15, 80, [
          (NodeType.COMMENT, 0, 1, 26, 80, []),
        ]),
        (NodeType.ARGUMENT, 0, 3, 4, 14, []),
        (NodeType.ARGUMENT, 0, 3, 15, 25, []),
      ]),
    ]),
    (NodeType.RPAREN, 0, 3, 25, 26, []),
  ]),
]),
      ])

  def test_complex_nested_stuff(self):
    self.config.format.autosort = False
    self.do_layout_test("""\
      if(foo)
      if(sbar)
      # This comment is in-scope.
      add_library(foo_bar_baz foo.cc bar.cc # this is a comment for arg2
                    # this is more comment for arg2, it should be joined with the first.
          baz.cc) # This comment is part of add_library

      other_command(some_long_argument some_long_argument) # this comment is very long and gets split across some lines

      other_command(some_long_argument some_long_argument some_long_argument) # this comment is even longer and wouldn't make sense to pack at the end of the command so it gets it's own lines
      endif()
      endif()
      """, [
# pylint: disable=bad-continuation
# noqa: E122
(NodeType.BODY, 0, 0, 0, 80, [
  (NodeType.FLOW_CONTROL, 0, 0, 0, 80, [
    (NodeType.STATEMENT, 0, 0, 0, 7, [
      (NodeType.FUNNAME, 0, 0, 0, 2, []),
      (NodeType.LPAREN, 0, 0, 2, 3, []),
      (NodeType.ARGGROUP, 0, 0, 3, 6, [
        (NodeType.PARGGROUP, 0, 0, 3, 6, [
          (NodeType.ARGUMENT, 0, 0, 3, 6, []),
        ]),
      ]),
      (NodeType.RPAREN, 0, 0, 6, 7, []),
    ]),
    (NodeType.BODY, 0, 1, 2, 80, [
      (NodeType.FLOW_CONTROL, 0, 1, 2, 80, [
        (NodeType.STATEMENT, 0, 1, 2, 10, [
          (NodeType.FUNNAME, 0, 1, 2, 4, []),
          (NodeType.LPAREN, 0, 1, 4, 5, []),
          (NodeType.ARGGROUP, 0, 1, 5, 9, [
            (NodeType.PARGGROUP, 0, 1, 5, 9, [
              (NodeType.ARGUMENT, 0, 1, 5, 9, []),
            ]),
          ]),
          (NodeType.RPAREN, 0, 1, 9, 10, []),
        ]),
        (NodeType.BODY, 0, 2, 4, 80, [
          (NodeType.COMMENT, 0, 2, 4, 31, []),
          (NodeType.STATEMENT, 4, 3, 4, 74, [
            (NodeType.FUNNAME, 0, 3, 4, 15, []),
            (NodeType.LPAREN, 0, 3, 15, 16, []),
            (NodeType.ARGGROUP, 4, 4, 6, 74, [
              (NodeType.PARGGROUP, 0, 4, 6, 17, [
                (NodeType.ARGUMENT, 0, 4, 6, 17, []),
              ]),
              (NodeType.PARGGROUP, 0, 5, 6, 74, [
                (NodeType.ARGUMENT, 0, 5, 6, 12, []),
                (NodeType.ARGUMENT, 0, 5, 13, 48, [
                  (NodeType.COMMENT, 0, 5, 20, 48, []),
                ]),
                (NodeType.COMMENT, 0, 6, 6, 74, []),
                (NodeType.ARGUMENT, 0, 7, 6, 12, []),
              ]),
            ]),
            (NodeType.RPAREN, 0, 7, 12, 13, []),
            (NodeType.COMMENT, 0, 7, 14, 51, []),
          ]),
          (NodeType.WHITESPACE, 0, 8, 4, 0, []),
          (NodeType.STATEMENT, 1, 9, 4, 76, [
            (NodeType.FUNNAME, 0, 9, 4, 17, []),
            (NodeType.LPAREN, 0, 9, 17, 18, []),
            (NodeType.ARGGROUP, 0, 10, 6, 43, [
              (NodeType.PARGGROUP, 0, 10, 6, 43, [
                (NodeType.ARGUMENT, 0, 10, 6, 24, []),
                (NodeType.ARGUMENT, 0, 10, 25, 43, []),
              ]),
            ]),
            (NodeType.RPAREN, 0, 10, 43, 44, []),
            (NodeType.COMMENT, 0, 10, 45, 76, []),
          ]),
          (NodeType.WHITESPACE, 0, 12, 4, 0, []),
          (NodeType.STATEMENT, 1, 13, 4, 80, [
            (NodeType.FUNNAME, 0, 13, 4, 17, []),
            (NodeType.LPAREN, 0, 13, 17, 18, []),
            (NodeType.ARGGROUP, 0, 14, 6, 62, [
              (NodeType.PARGGROUP, 0, 14, 6, 62, [
                (NodeType.ARGUMENT, 0, 14, 6, 24, []),
                (NodeType.ARGUMENT, 0, 14, 25, 43, []),
                (NodeType.ARGUMENT, 0, 14, 44, 62, []),
              ]),
            ]),
            (NodeType.RPAREN, 0, 14, 62, 63, []),
            (NodeType.COMMENT, 0, 14, 64, 80, []),
          ]),
        ]),
        (NodeType.STATEMENT, 0, 23, 2, 9, [
          (NodeType.FUNNAME, 0, 23, 2, 7, []),
          (NodeType.LPAREN, 0, 23, 7, 8, []),
          (NodeType.ARGGROUP, 0, 23, 8, 8, []),
          (NodeType.RPAREN, 0, 23, 8, 9, []),
        ]),
      ]),
    ]),
    (NodeType.STATEMENT, 0, 24, 0, 7, [
      (NodeType.FUNNAME, 0, 24, 0, 5, []),
      (NodeType.LPAREN, 0, 24, 5, 6, []),
      (NodeType.ARGGROUP, 0, 24, 6, 6, []),
      (NodeType.RPAREN, 0, 24, 6, 7, []),
    ]),
  ]),
]),
      ])

  def test_custom_command(self):
    self.do_layout_test("""\
      # This very long command should be broken up along keyword arguments
      foo(nonkwarg_a nonkwarg_b HEADERS a.h b.h c.h d.h e.h f.h SOURCES a.cc b.cc d.cc DEPENDS foo bar baz)
      """, [
# pylint: disable=bad-continuation
# noqa: E122
(NodeType.BODY, 0, 0, 0, 68, [
  (NodeType.COMMENT, 0, 0, 0, 68, []),
  (NodeType.STATEMENT, 4, 1, 0, 35, [
    (NodeType.FUNNAME, 0, 1, 0, 3, []),
    (NodeType.LPAREN, 0, 1, 3, 4, []),
    (NodeType.ARGGROUP, 4, 1, 4, 35, [
      (NodeType.PARGGROUP, 0, 1, 4, 25, [
        (NodeType.ARGUMENT, 0, 1, 4, 14, []),
        (NodeType.ARGUMENT, 0, 1, 15, 25, []),
      ]),
      (NodeType.KWARGGROUP, 0, 2, 4, 35, [
        (NodeType.KEYWORD, 0, 2, 4, 11, []),
        (NodeType.ARGGROUP, 0, 2, 12, 35, [
          (NodeType.PARGGROUP, 0, 2, 12, 35, [
            (NodeType.ARGUMENT, 0, 2, 12, 15, []),
            (NodeType.ARGUMENT, 0, 2, 16, 19, []),
            (NodeType.ARGUMENT, 0, 2, 20, 23, []),
            (NodeType.ARGUMENT, 0, 2, 24, 27, []),
            (NodeType.ARGUMENT, 0, 2, 28, 31, []),
            (NodeType.ARGUMENT, 0, 2, 32, 35, []),
          ]),
        ]),
      ]),
      (NodeType.KWARGGROUP, 0, 3, 4, 26, [
        (NodeType.KEYWORD, 0, 3, 4, 11, []),
        (NodeType.ARGGROUP, 0, 3, 12, 26, [
          (NodeType.PARGGROUP, 0, 3, 12, 26, [
            (NodeType.ARGUMENT, 0, 3, 12, 16, []),
            (NodeType.ARGUMENT, 0, 3, 17, 21, []),
            (NodeType.ARGUMENT, 0, 3, 22, 26, []),
          ]),
        ]),
      ]),
      (NodeType.KWARGGROUP, 0, 4, 4, 15, [
        (NodeType.KEYWORD, 0, 4, 4, 11, []),
        (NodeType.ARGGROUP, 0, 4, 12, 15, [
          (NodeType.PARGGROUP, 0, 4, 12, 15, [
            (NodeType.ARGUMENT, 0, 4, 12, 15, []),
          ]),
        ]),
      ]),
      (NodeType.PARGGROUP, 0, 5, 4, 11, [
        (NodeType.FLAG, 0, 5, 4, 7, []),
        (NodeType.FLAG, 0, 5, 8, 11, []),
      ]),
    ]),
    (NodeType.RPAREN, 0, 5, 11, 12, []),
  ]),
]),
      ])

  def test_multiline_string(self):
    self.do_layout_test("""\
      foo(some_arg some_arg "
          This string is on multiple lines
      ")
      """, [
# pylint: disable=bad-continuation
# noqa: E122
(NodeType.BODY, 0, 0, 0, 36, [
  (NodeType.STATEMENT, 0, 0, 0, 36, [
    (NodeType.FUNNAME, 0, 0, 0, 3, []),
    (NodeType.LPAREN, 0, 0, 3, 4, []),
    (NodeType.ARGGROUP, 0, 0, 4, 36, [
      (NodeType.PARGGROUP, 0, 0, 4, 36, [
        (NodeType.ARGUMENT, 0, 0, 4, 12, []),
        (NodeType.ARGUMENT, 0, 0, 13, 21, []),
        (NodeType.ARGUMENT, 0, 0, 22, 36, []),
      ]),
    ]),
    (NodeType.RPAREN, 0, 2, 1, 2, []),
  ]),
]),
      ])

  def test_nested_parens(self):
    self.do_layout_test("""\
      if((NOT HELLO) OR (NOT EXISTS ${WORLD}))
        message(WARNING "something is wrong")
        set(foobar FALSE)
      endif()
      """, [
# pylint: disable=bad-continuation
# noqa: E122
(NodeType.BODY, 0, 0, 0, 40, [
  (NodeType.FLOW_CONTROL, 0, 0, 0, 40, [
    (NodeType.STATEMENT, 0, 0, 0, 40, [
      (NodeType.FUNNAME, 0, 0, 0, 2, []),
      (NodeType.LPAREN, 0, 0, 2, 3, []),
      (NodeType.ARGGROUP, 0, 0, 3, 39, [
        (NodeType.PARENGROUP, 0, 0, 3, 14, [
          (NodeType.LPAREN, 0, 0, 3, 4, []),
          (NodeType.ARGGROUP, 0, 0, 4, 13, [
            (NodeType.PARGGROUP, 0, 0, 4, 13, [
              (NodeType.FLAG, 0, 0, 4, 7, []),
              (NodeType.ARGUMENT, 0, 0, 8, 13, []),
            ]),
          ]),
          (NodeType.RPAREN, 0, 0, 13, 14, []),
        ]),
        (NodeType.KWARGGROUP, 0, 0, 15, 39, [
          (NodeType.KEYWORD, 0, 0, 15, 17, []),
          (NodeType.ARGGROUP, 0, 0, 18, 39, [
            (NodeType.PARENGROUP, 0, 0, 18, 39, [
              (NodeType.LPAREN, 0, 0, 18, 19, []),
              (NodeType.ARGGROUP, 0, 0, 19, 38, [
                (NodeType.PARGGROUP, 0, 0, 19, 38, [
                  (NodeType.FLAG, 0, 0, 19, 22, []),
                  (NodeType.FLAG, 0, 0, 23, 29, []),
                  (NodeType.ARGUMENT, 0, 0, 30, 38, []),
                ]),
              ]),
              (NodeType.RPAREN, 0, 0, 38, 39, []),
            ]),
          ]),
        ]),
      ]),
      (NodeType.RPAREN, 0, 0, 39, 40, []),
    ]),
    (NodeType.BODY, 0, 1, 2, 39, [
      (NodeType.STATEMENT, 0, 1, 2, 39, [
        (NodeType.FUNNAME, 0, 1, 2, 9, []),
        (NodeType.LPAREN, 0, 1, 9, 10, []),
        (NodeType.ARGGROUP, 0, 1, 10, 38, [
          (NodeType.KWARGGROUP, 0, 1, 10, 38, [
            (NodeType.KEYWORD, 0, 1, 10, 17, []),
            (NodeType.ARGGROUP, 0, 1, 18, 38, [
              (NodeType.PARGGROUP, 0, 1, 18, 38, [
                (NodeType.ARGUMENT, 0, 1, 18, 38, []),
              ]),
            ]),
          ]),
        ]),
        (NodeType.RPAREN, 0, 1, 38, 39, []),
      ]),
      (NodeType.STATEMENT, 0, 2, 2, 19, [
        (NodeType.FUNNAME, 0, 2, 2, 5, []),
        (NodeType.LPAREN, 0, 2, 5, 6, []),
        (NodeType.ARGGROUP, 0, 2, 6, 18, [
          (NodeType.PARGGROUP, 0, 2, 6, 12, [
            (NodeType.ARGUMENT, 0, 2, 6, 12, []),
          ]),
          (NodeType.PARGGROUP, 0, 2, 13, 18, [
            (NodeType.ARGUMENT, 0, 2, 13, 18, []),
          ]),
        ]),
        (NodeType.RPAREN, 0, 2, 18, 19, []),
      ]),
    ]),
    (NodeType.STATEMENT, 0, 3, 0, 7, [
      (NodeType.FUNNAME, 0, 3, 0, 5, []),
      (NodeType.LPAREN, 0, 3, 5, 6, []),
      (NodeType.ARGGROUP, 0, 3, 6, 6, []),
      (NodeType.RPAREN, 0, 3, 6, 7, []),
    ]),
  ]),
]),
      ])

  def test_comment_after_command(self):
    with self.subTest(sub=1):
      self.do_layout_test("""\
        foo_command() # comment
        """, [
# pylint: disable=bad-continuation
# noqa: E122
(NodeType.BODY, 0, 0, 0, 23, [
  (NodeType.STATEMENT, 0, 0, 0, 23, [
    (NodeType.FUNNAME, 0, 0, 0, 11, []),
    (NodeType.LPAREN, 0, 0, 11, 12, []),
    (NodeType.ARGGROUP, 0, 0, 12, 12, []),
    (NodeType.RPAREN, 0, 0, 12, 13, []),
    (NodeType.COMMENT, 0, 0, 14, 23, []),
  ]),
]),
        ])

    with self.subTest(sub=2):
      self.do_layout_test("""\
        foo_command() # this is a long comment that exceeds the desired page width and will be wrapped to a newline
        """, [
# pylint: disable=bad-continuation
# noqa: E122
(NodeType.BODY, 0, 0, 0, 78, [
  (NodeType.STATEMENT, 0, 0, 0, 78, [
    (NodeType.FUNNAME, 0, 0, 0, 11, []),
    (NodeType.LPAREN, 0, 0, 11, 12, []),
    (NodeType.ARGGROUP, 0, 0, 12, 12, []),
    (NodeType.RPAREN, 0, 0, 12, 13, []),
    (NodeType.COMMENT, 0, 0, 14, 78, []),
  ]),
]),

        ])

  def test_arg_just_fits(self):
    """
    Ensure that if an argument *just* fits that it isn't superfluously wrapped
    """

    with self.subTest(chars=81):
      self.do_layout_test("""\
      message(FATAL_ERROR "81 character line ----------------------------------------")
    """, [
# pylint: disable=bad-continuation
# noqa: E122
(NodeType.BODY, 0, 0, 0, 75, [
  (NodeType.STATEMENT, 1, 0, 0, 75, [
    (NodeType.FUNNAME, 0, 0, 0, 7, []),
    (NodeType.LPAREN, 0, 0, 7, 8, []),
    (NodeType.ARGGROUP, 0, 1, 2, 74, [
      (NodeType.KWARGGROUP, 0, 1, 2, 74, [
        (NodeType.KEYWORD, 0, 1, 2, 13, []),
        (NodeType.ARGGROUP, 0, 1, 14, 74, [
          (NodeType.PARGGROUP, 0, 1, 14, 74, [
            (NodeType.ARGUMENT, 0, 1, 14, 74, []),
          ]),
        ]),
      ]),
    ]),
    (NodeType.RPAREN, 0, 1, 74, 75, []),
  ]),
]),
    ])

    with self.subTest(chars=100, with_prefix=True):
      self.do_layout_test("""\
      message(FATAL_ERROR
              "100 character line ----------------------------------------------------------"
      ) # Closing parenthesis is indented one space!
    """, [
# pylint: disable=bad-continuation
# noqa: E122
(NodeType.BODY, 0, 0, 0, 83, [
  (NodeType.STATEMENT, 5, 0, 0, 83, [
    (NodeType.FUNNAME, 0, 0, 0, 7, []),
    (NodeType.LPAREN, 0, 0, 7, 8, []),
    (NodeType.ARGGROUP, 5, 1, 2, 83, [
      (NodeType.KWARGGROUP, 5, 1, 2, 83, [
        (NodeType.KEYWORD, 0, 1, 2, 13, []),
        (NodeType.ARGGROUP, 5, 2, 4, 83, [
          (NodeType.PARGGROUP, 4, 2, 4, 83, [
            (NodeType.ARGUMENT, 0, 2, 4, 83, []),
          ]),
        ]),
      ]),
    ]),
    (NodeType.RPAREN, 0, 3, 0, 1, []),
    (NodeType.COMMENT, 0, 3, 2, 46, []),
  ]),
]),
    ])

    with self.subTest(chars=100):
      self.do_layout_test("""\
      message(
        "100 character line ----------------------------------------------------------------------"
        ) # Closing parenthesis is indented one space!
    """, [
# pylint: disable=bad-continuation
# noqa: E122
(NodeType.BODY, 0, 0, 0, 93, [
  (NodeType.STATEMENT, 5, 0, 0, 93, [
    (NodeType.FUNNAME, 0, 0, 0, 7, []),
    (NodeType.LPAREN, 0, 0, 7, 8, []),
    (NodeType.ARGGROUP, 5, 1, 2, 93, [
      (NodeType.PARGGROUP, 4, 1, 2, 93, [
        (NodeType.ARGUMENT, 0, 1, 2, 93, []),
      ]),
    ]),
    (NodeType.RPAREN, 0, 2, 0, 1, []),
    (NodeType.COMMENT, 0, 2, 2, 46, []),
  ]),
]),
    ])

  def test_string_preserved_during_split(self):
    self.do_layout_test("""\
      # The string in this command should not be split
      set_target_properties(foo bar baz PROPERTIES COMPILE_FLAGS "-std=c++11 -Wall -Wextra")
      """, [
# pylint: disable=bad-continuation
# noqa: E122
(NodeType.BODY, 0, 0, 0, 72, [
  (NodeType.COMMENT, 0, 0, 0, 48, []),
  (NodeType.STATEMENT, 0, 1, 0, 72, [
    (NodeType.FUNNAME, 0, 1, 0, 21, []),
    (NodeType.LPAREN, 0, 1, 21, 22, []),
    (NodeType.ARGGROUP, 0, 1, 22, 71, [
      (NodeType.PARGGROUP, 0, 1, 22, 33, [
        (NodeType.ARGUMENT, 0, 1, 22, 25, []),
        (NodeType.ARGUMENT, 0, 1, 26, 29, []),
        (NodeType.ARGUMENT, 0, 1, 30, 33, []),
      ]),
      (NodeType.KWARGGROUP, 0, 1, 34, 71, [
        (NodeType.KEYWORD, 0, 1, 34, 44, []),
        (NodeType.ARGGROUP, 0, 1, 45, 71, [
          (NodeType.PARGGROUP, 0, 1, 45, 71, [
            (NodeType.ARGUMENT, 0, 1, 45, 58, []),
            (NodeType.ARGUMENT, 0, 2, 45, 71, []),
          ]),
        ]),
      ]),
    ]),
    (NodeType.RPAREN, 0, 2, 71, 72, []),
  ]),
]),
      ])

  def test_while(self):
    self.do_layout_test("""\
      while(forbarbaz arg1 arg2, arg3)
        message(hello ${foobarbaz})
      endwhile()
      """, [
# pylint: disable=bad-continuation
# noqa: E122
(NodeType.BODY, 0, 0, 0, 32, [
  (NodeType.FLOW_CONTROL, 0, 0, 0, 32, [
    (NodeType.STATEMENT, 0, 0, 0, 32, [
      (NodeType.FUNNAME, 0, 0, 0, 5, []),
      (NodeType.LPAREN, 0, 0, 5, 6, []),
      (NodeType.ARGGROUP, 0, 0, 6, 31, [
        (NodeType.PARGGROUP, 0, 0, 6, 31, [
          (NodeType.ARGUMENT, 0, 0, 6, 15, []),
          (NodeType.ARGUMENT, 0, 0, 16, 20, []),
          (NodeType.ARGUMENT, 0, 0, 21, 26, []),
          (NodeType.ARGUMENT, 0, 0, 27, 31, []),
        ]),
      ]),
      (NodeType.RPAREN, 0, 0, 31, 32, []),
    ]),
    (NodeType.BODY, 0, 1, 2, 29, [
      (NodeType.STATEMENT, 0, 1, 2, 29, [
        (NodeType.FUNNAME, 0, 1, 2, 9, []),
        (NodeType.LPAREN, 0, 1, 9, 10, []),
        (NodeType.ARGGROUP, 0, 1, 10, 28, [
          (NodeType.PARGGROUP, 0, 1, 10, 28, [
            (NodeType.ARGUMENT, 0, 1, 10, 15, []),
            (NodeType.ARGUMENT, 0, 1, 16, 28, []),
          ]),
        ]),
        (NodeType.RPAREN, 0, 1, 28, 29, []),
      ]),
    ]),
    (NodeType.STATEMENT, 0, 2, 0, 10, [
      (NodeType.FUNNAME, 0, 2, 0, 8, []),
      (NodeType.LPAREN, 0, 2, 8, 9, []),
      (NodeType.ARGGROUP, 0, 2, 9, 9, []),
      (NodeType.RPAREN, 0, 2, 9, 10, []),
    ]),
  ]),
]),
      ])

  def test_keyword_comment(self):
    self.do_layout_test("""\
      find_package(package REQUIRED
                   COMPONENTS # --------------------------------------
                              # @TODO: This has to be filled manually
                              # --------------------------------------
                              this_is_a_really_long_word_foo)
      """, [
# pylint: disable=bad-continuation
# noqa: E122
(NodeType.BODY, 0, 0, 0, 53, [
  (NodeType.STATEMENT, 4, 0, 0, 53, [
    (NodeType.FUNNAME, 0, 0, 0, 12, []),
    (NodeType.LPAREN, 0, 0, 12, 13, []),
    (NodeType.ARGGROUP, 4, 1, 2, 53, [
      (NodeType.PARGGROUP, 0, 1, 2, 18, [
        (NodeType.ARGUMENT, 0, 1, 2, 9, []),
        (NodeType.FLAG, 0, 1, 10, 18, []),
      ]),
      (NodeType.KWARGGROUP, 4, 2, 2, 53, [
        (NodeType.KEYWORD, 0, 2, 2, 12, []),
        (NodeType.ARGGROUP, 4, 2, 13, 53, [
          (NodeType.COMMENT, 0, 2, 13, 53, []),
          (NodeType.PARGGROUP, 0, 5, 13, 43, [
            (NodeType.ARGUMENT, 0, 5, 13, 43, []),
          ]),
        ]),
      ]),
    ]),
    (NodeType.RPAREN, 0, 5, 43, 44, []),
  ]),
]),
      ])

  def test_sortable_set(self):
    self.do_layout_test("""\
      set(SOURCES #[[cmf:sortable]] foo.cc bar.cc baz.cc)
      """, [
# pylint: disable=bad-continuation
# noqa: E122
(NodeType.BODY, 0, 0, 0, 51, [
  (NodeType.STATEMENT, 0, 0, 0, 51, [
    (NodeType.FUNNAME, 0, 0, 0, 3, []),
    (NodeType.LPAREN, 0, 0, 3, 4, []),
    (NodeType.ARGGROUP, 0, 0, 4, 50, [
      (NodeType.PARGGROUP, 0, 0, 4, 11, [
        (NodeType.ARGUMENT, 0, 0, 4, 11, []),
      ]),
      (NodeType.PARGGROUP, 0, 0, 12, 50, [
        (NodeType.COMMENT, 0, 0, 12, 29, []),
        (NodeType.ARGUMENT, 0, 0, 30, 36, []),
        (NodeType.ARGUMENT, 0, 0, 37, 43, []),
        (NodeType.ARGUMENT, 0, 0, 44, 50, []),
      ]),
    ]),
    (NodeType.RPAREN, 0, 0, 50, 51, []),
  ]),
]),
      ])


if __name__ == '__main__':
  format_str = '[%(levelname)-4s] %(filename)s:%(lineno)-3s: %(message)s'
  logging.basicConfig(level=logging.DEBUG,
                      format=format_str,
                      datefmt='%Y-%m-%d %H:%M:%S',
                      filemode='w')
  unittest.main()
