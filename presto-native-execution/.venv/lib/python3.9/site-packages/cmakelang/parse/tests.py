# -*- coding: utf-8 -*-
# pylint: disable=R1708
from __future__ import unicode_literals
import unittest

from cmakelang import configuration
from cmakelang import lex
from cmakelang import parse
from cmakelang.parse.printer import tree_string, test_string
from cmakelang.parse.common import NodeType


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

  while isinstance(item_a, lex.Token):
    item_a = next(iter_a, None)

  while item_a is not None and item_b is not None:
    yield(item_a, item_b)
    item_a = next(iter_a, None)
    while isinstance(item_a, lex.Token):
      item_a = next(iter_a, None)
    item_b = next(iter_b, None)

  while item_a is not None:
    yield(item_a, None)
    item_a = next(iter_a, None)
    while isinstance(item_a, lex.Token):
      item_a = next(iter_a, None)

  while item_b is not None:
    yield(None, item_b)
    item_b = next(iter_b, None)


def assert_tree_type(test, nodes, tups, tree=None, history=None):
  """
  Check the output tree structure against that of expect_tree: a nested tuple
  tree.
  """

  if tree is None:
    tree = nodes

  if history is None:
    history = []

  for node, tup in overzip(nodes, tups):
    if isinstance(node, lex.Token):
      continue
    message = ("For node {} at\n {} within \n{}. "
               "If this is infact correct, copy-paste this:\n\n{}"
               .format(node, tree_string([node]),
                       tree_string(tree, history),
                       test_string(tree)))
    test.assertIsNotNone(node, msg="Missing node " + message)
    test.assertIsNotNone(tup, msg="Extra node " + message)
    expect_type, expect_children = tup
    test.assertEqual(node.node_type, expect_type,
                     msg="Expected type={} ".format(expect_type) + message)
    assert_tree_type(test, node.children, expect_children, tree,
                     history + [node])


class TestCanonicalParse(unittest.TestCase):
  """
  Given a bunch of example inputs, ensure that they parse into the expected
  tree structure.
  """

  def __init__(self, *args, **kwargs):
    super(TestCanonicalParse, self).__init__(*args, **kwargs)
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

  def do_type_test(self, input_str, expect_tree):
    """
    Run the parser to get the fst, then compare the result to the types in the
    ``expect_tree`` tuple tree.
    """
    tokens = lex.tokenize(input_str)

    fst_root = parse.parse(tokens, self.parse_ctx)
    assert_tree_type(self, [fst_root], expect_tree)

  def test_collapse_additional_newlines(self):
    self.do_type_test("""\
      # The following multiple newlines should be collapsed into a single newline




      cmake_minimum_required(VERSION 2.8.11)
      project(cmakelang_test)
      """, [
          (NodeType.BODY, [
              (NodeType.WHITESPACE, []),
              (NodeType.COMMENT, []),
              (NodeType.WHITESPACE, []),
              (NodeType.STATEMENT, [
                  (NodeType.FUNNAME, []),
                  (NodeType.LPAREN, []),
                  (NodeType.ARGGROUP, [
                      (NodeType.KWARGGROUP, [
                          (NodeType.KEYWORD, []),
                          (NodeType.ARGGROUP, [
                              (NodeType.PARGGROUP, [
                                  (NodeType.ARGUMENT, []),
                              ]),
                          ]),
                      ]),
                  ]),
                  (NodeType.RPAREN, []),
              ]),
              (NodeType.WHITESPACE, []),
              (NodeType.STATEMENT, [
                  (NodeType.FUNNAME, []),
                  (NodeType.LPAREN, []),
                  (NodeType.ARGGROUP, [
                      (NodeType.PARGGROUP, [
                          (NodeType.ARGUMENT, []),
                      ]),
                  ]),
                  (NodeType.RPAREN, []),
              ]),
              (NodeType.WHITESPACE, []),
          ]),
      ])

  def test_multiline_comment(self):
    self.do_type_test("""\
      # This multiline-comment should be reflowed
      # into a single comment
      # on one line
      """, [
          (NodeType.BODY, [
              (NodeType.WHITESPACE, []),
              (NodeType.COMMENT, []),
              (NodeType.WHITESPACE, []),
          ])
      ])

  def test_nested_kwargs(self):
    self.do_type_test("""\
      add_custom_target(name ALL VERBATIM
        COMMAND echo hello world
        COMMENT "this is some text")
      """, [
          (NodeType.BODY, [
              (NodeType.WHITESPACE, []),
              (NodeType.STATEMENT, [
                  (NodeType.FUNNAME, []),
                  (NodeType.LPAREN, []),
                  (NodeType.ARGGROUP, [
                      (NodeType.PARGGROUP, [
                          (NodeType.ARGUMENT, []),
                          (NodeType.FLAG, []),
                      ]),
                      (NodeType.PARGGROUP, [
                          (NodeType.FLAG, []),
                      ]),
                      (NodeType.KWARGGROUP, [
                          (NodeType.KEYWORD, []),
                          (NodeType.ARGGROUP, [
                              (NodeType.PARGGROUP, [
                                  (NodeType.ARGUMENT, []),
                                  (NodeType.ARGUMENT, []),
                                  (NodeType.ARGUMENT, []),
                              ]),
                          ]),
                      ]),
                      (NodeType.KWARGGROUP, [
                          (NodeType.KEYWORD, []),
                          (NodeType.PARGGROUP, [
                              (NodeType.ARGUMENT, []),
                          ]),
                      ]),
                  ]),
                  (NodeType.RPAREN, []),
              ]),
              (NodeType.WHITESPACE, []),
          ]),
      ])

  def test_custom_command(self):
    self.do_type_test("""\
      # This very long command should be broken up along keyword arguments
      foo(nonkwarg_a nonkwarg_b HEADERS a.h b.h c.h d.h e.h f.h SOURCES a.cc b.cc d.cc DEPENDS foo bar baz)
      """, [
          (NodeType.BODY, [
              (NodeType.WHITESPACE, []),
              (NodeType.COMMENT, []),
              (NodeType.WHITESPACE, []),
              (NodeType.STATEMENT, [
                  (NodeType.FUNNAME, []),
                  (NodeType.LPAREN, []),
                  (NodeType.ARGGROUP, [
                      (NodeType.PARGGROUP, [
                          (NodeType.ARGUMENT, []),
                          (NodeType.ARGUMENT, []),
                      ]),
                      (NodeType.KWARGGROUP, [
                          (NodeType.KEYWORD, []),
                          (NodeType.ARGGROUP, [
                              (NodeType.PARGGROUP, [
                                  (NodeType.ARGUMENT, []),
                                  (NodeType.ARGUMENT, []),
                                  (NodeType.ARGUMENT, []),
                                  (NodeType.ARGUMENT, []),
                                  (NodeType.ARGUMENT, []),
                                  (NodeType.ARGUMENT, []),
                              ]),
                          ]),
                      ]),
                      (NodeType.KWARGGROUP, [
                          (NodeType.KEYWORD, []),
                          (NodeType.ARGGROUP, [
                              (NodeType.PARGGROUP, [
                                  (NodeType.ARGUMENT, []),
                                  (NodeType.ARGUMENT, []),
                                  (NodeType.ARGUMENT, []),
                              ]),
                          ]),
                      ]),
                      (NodeType.KWARGGROUP, [
                          (NodeType.KEYWORD, []),
                          (NodeType.ARGGROUP, [
                              (NodeType.PARGGROUP, [
                                  (NodeType.ARGUMENT, []),
                              ]),
                          ]),
                      ]),
                      (NodeType.PARGGROUP, [
                          (NodeType.FLAG, []),
                          (NodeType.FLAG, []),
                      ]),
                  ]),
                  (NodeType.RPAREN, []),
              ]),
              (NodeType.WHITESPACE, []),
          ]),
      ])

  def test_shellcommand_parse(self):
    self.do_type_test("""\
      add_test(NAME foo-test
               COMMAND cmdname -Bm -h --hello foo bar --world baz buck
               WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
      """, [
          (NodeType.BODY, [
              (NodeType.WHITESPACE, []),
              (NodeType.STATEMENT, [
                  (NodeType.FUNNAME, []),
                  (NodeType.LPAREN, []),
                  (NodeType.ARGGROUP, [
                      (NodeType.KWARGGROUP, [
                          (NodeType.KEYWORD, []),
                          (NodeType.ARGGROUP, [
                              (NodeType.PARGGROUP, [
                                  (NodeType.ARGUMENT, []),
                              ]),
                          ]),
                      ]),
                      (NodeType.KWARGGROUP, [
                          (NodeType.KEYWORD, []),
                          (NodeType.ARGGROUP, [
                              (NodeType.PARGGROUP, [
                                  (NodeType.ARGUMENT, []),
                                  (NodeType.ARGUMENT, []),
                                  (NodeType.ARGUMENT, []),
                                  (NodeType.ARGUMENT, []),
                                  (NodeType.ARGUMENT, []),
                                  (NodeType.ARGUMENT, []),
                                  (NodeType.ARGUMENT, []),
                                  (NodeType.ARGUMENT, []),
                                  (NodeType.ARGUMENT, []),
                              ]),
                          ]),
                      ]),
                      (NodeType.KWARGGROUP, [
                          (NodeType.KEYWORD, []),
                          (NodeType.ARGGROUP, [
                              (NodeType.PARGGROUP, [
                                  (NodeType.ARGUMENT, []),
                              ]),
                          ]),
                      ]),
                  ]),
                  (NodeType.RPAREN, []),
              ]),
              (NodeType.WHITESPACE, []),
          ]),
      ])


if __name__ == '__main__':
  unittest.main()
