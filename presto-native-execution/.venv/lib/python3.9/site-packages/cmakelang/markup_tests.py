from __future__ import unicode_literals
import unittest

from cmakelang.format import __main__
from cmakelang import markup


class MockMarkupConfig(object):
  def __init__(self, fence_pattern, ruler_pattern):
    self.fence_pattern = fence_pattern
    self.ruler_pattern = ruler_pattern


class MockRootConfig(object):
  def __init__(self, fence_pattern, ruler_pattern):
    self.markup = MockMarkupConfig(fence_pattern, ruler_pattern)


class TestSpecificParses(unittest.TestCase):

  def assert_item_types(self, input_str, expected_types, strip_indent=6,
                        config=None):
    """
    Run the lexer on the input string and assert that the result tokens match
    the expected
    """

    lines = [line[strip_indent:] for line in input_str.split('\n')]
    self.assertEqual(expected_types,
                     [item.kind for item in markup.parse(lines, config)])

  def test_paragraphs(self):
    self.assert_item_types("""\
      Hello world

      Hello wold""", [markup.CommentType.PARAGRAPH,
                      markup.CommentType.SEPARATOR,
                      markup.CommentType.PARAGRAPH])

  def test_lists(self):
    self.assert_item_types("""\
      This is a paragraph

      * this is a
      * bulleted list
      * of three items

        1. this is another list
        2. of two items

      This is a paragraph""",
                           [markup.CommentType.PARAGRAPH,
                            markup.CommentType.SEPARATOR,
                            markup.CommentType.BULLET_LIST,
                            markup.CommentType.SEPARATOR,
                            markup.CommentType.ENUM_LIST,
                            markup.CommentType.SEPARATOR,
                            markup.CommentType.PARAGRAPH])

  def test_list_indentation(self):
    lines = [line[6:] for line in """\
      * this is a
      * bulleted list
      * of three items

        * this is another list
        * of two items

          * this is a third list
          * of two items
    """.splitlines()]
    items = markup.parse(lines)
    self.assertEqual(items[0].kind, markup.CommentType.BULLET_LIST)
    self.assertEqual(items[0].indent, 0)
    self.assertEqual(items[2].kind, markup.CommentType.BULLET_LIST)
    self.assertEqual(items[2].indent, 2)
    self.assertEqual(items[4].kind, markup.CommentType.BULLET_LIST)
    self.assertEqual(items[4].indent, 4)

  def test_notes(self):
    self.assert_item_types("""\
      This is a comment
      that should be joined but
      TODO(josh): This todo should not be joined with the previous line.
      NOTE(josh): Also this should not be joined with the todo.
      """, [markup.CommentType.PARAGRAPH,
            markup.CommentType.NOTE,
            markup.CommentType.NOTE,
            markup.CommentType.SEPARATOR])

  def test_rulers(self):
    self.assert_item_types("""\
      --------------------
      This is some
      text that I expect
      to reflow
      --------------------
      """, [markup.CommentType.RULER,
            markup.CommentType.PARAGRAPH,
            markup.CommentType.RULER,
            markup.CommentType.SEPARATOR])

  def test_rulers_break_bullets(self):
    self.assert_item_types("""\
      --------------------
      * Bulleted item
      * Bulttied item
      --------------------
      """, [markup.CommentType.RULER,
            markup.CommentType.BULLET_LIST,
            markup.CommentType.RULER,
            markup.CommentType.SEPARATOR])

  def test_fences(self):
    self.assert_item_types("""\
      ~~~
      this is some
         verbatim text
      that should not
         be changed
      ~~~~~~
      """, [markup.CommentType.FENCE,
            markup.CommentType.VERBATIM,
            markup.CommentType.FENCE,
            markup.CommentType.SEPARATOR])

  def test_custom_fences(self):
    self.assert_item_types("""\
      ###
      this is some
         verbatim text
      that should not
         be changed
      ###
      """, [markup.CommentType.PARAGRAPH,
            markup.CommentType.SEPARATOR])

    config = MockRootConfig(
        fence_pattern=r'^\s*([#`~]{3}[#`~]*)(.*)$',
        ruler_pattern=r'^\s*[^\w\s]{3}.*[^\w\s]{3}$')
    self.assert_item_types(
        """\
      ###
      this is some
         verbatim text
      that should not
         be changed
      ###
      """,
        [markup.CommentType.FENCE,
         markup.CommentType.VERBATIM,
         markup.CommentType.FENCE,
         markup.CommentType.SEPARATOR],
        config=config)


if __name__ == '__main__':
  unittest.main()
