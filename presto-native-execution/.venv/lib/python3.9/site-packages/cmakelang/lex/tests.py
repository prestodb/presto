# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import unittest

from cmakelang.format import __main__
from cmakelang import lex
from cmakelang.lex import TokenType


class TestSpecificLexings(unittest.TestCase):

  def assert_tok_types(self, input_str, expected_types):
    """
    Run the lexer on the input string and assert that the result tokens match
    the expected
    """

    self.assertEqual(expected_types,
                     [tok.type for tok in lex.tokenize(input_str)])

  def test_bracket_arguments(self):
    self.assert_tok_types("foo(bar [=[hello world]=] baz)", [
        TokenType.WORD, TokenType.LEFT_PAREN, TokenType.WORD,
        TokenType.WHITESPACE, TokenType.BRACKET_ARGUMENT, TokenType.WHITESPACE,
        TokenType.WORD, TokenType.RIGHT_PAREN])

  def test_bracket_comments(self):
    self.assert_tok_types("foo(bar #[=[hello world]=] baz)", [
        TokenType.WORD, TokenType.LEFT_PAREN, TokenType.WORD,
        TokenType.WHITESPACE, TokenType.BRACKET_COMMENT, TokenType.WHITESPACE,
        TokenType.WORD, TokenType.RIGHT_PAREN])

    self.assert_tok_types("""\
      #[==[This is a bracket comment at some nested level
      #    it is preserved verbatim, but trailing
      #    whitespace is removed.]==]
      """, [TokenType.WHITESPACE, TokenType.BRACKET_COMMENT, TokenType.NEWLINE,
            TokenType.WHITESPACE])

    self.assert_tok_types("""\
      #[[First bracket comment]]
      # intervening comment
      #[[Second bracket comment]]
      """, [
          TokenType.WHITESPACE, TokenType.BRACKET_COMMENT,
          TokenType.NEWLINE, TokenType.WHITESPACE,
          TokenType.COMMENT, TokenType.NEWLINE, TokenType.WHITESPACE,
          TokenType.BRACKET_COMMENT, TokenType.NEWLINE, TokenType.WHITESPACE
      ])

  def test_string(self):
    self.assert_tok_types("""\
      foo(bar "this is a string")
      """, [TokenType.WHITESPACE, TokenType.WORD, TokenType.LEFT_PAREN,
            TokenType.WORD, TokenType.WHITESPACE, TokenType.QUOTED_LITERAL,
            TokenType.RIGHT_PAREN, TokenType.NEWLINE, TokenType.WHITESPACE])

  def test_string_with_quotes(self):
    self.assert_tok_types(r"""
      "this is a \"string"
      """, [TokenType.NEWLINE, TokenType.WHITESPACE, TokenType.QUOTED_LITERAL,
            TokenType.NEWLINE, TokenType.WHITESPACE])

    self.assert_tok_types(r"""
      'this is a \'string'
      """, [TokenType.NEWLINE, TokenType.WHITESPACE, TokenType.QUOTED_LITERAL,
            TokenType.NEWLINE, TokenType.WHITESPACE])

  def test_complicated_string_with_quotes(self):
    # NOTE(josh): compacted example from bug report
    # https://github.com/cheshirekow/cmakelang/issues/28
    self.assert_tok_types(r"""
      install(CODE "message(\"foo ${bar}/${baz}...\")
        subfun(COMMAND ${WHEEL_COMMAND}
                       ERROR_MESSAGE \"error ${bar}/${baz}\"
               )"
      )
      """, [TokenType.NEWLINE, TokenType.WHITESPACE, TokenType.WORD,
            TokenType.LEFT_PAREN, TokenType.WORD, TokenType.WHITESPACE,
            TokenType.QUOTED_LITERAL, TokenType.NEWLINE, TokenType.WHITESPACE,
            TokenType.RIGHT_PAREN, TokenType.NEWLINE, TokenType.WHITESPACE])

  def test_mixed_whitespace(self):
    """
    Ensure that if a newline is part of a whitespace sequence then it is
    tokenized separately.
    """
    self.assert_tok_types(" \n", [TokenType.WHITESPACE, TokenType.NEWLINE])
    self.assert_tok_types("\t\n", [TokenType.WHITESPACE, TokenType.NEWLINE])
    self.assert_tok_types("\f\n", [TokenType.WHITESPACE, TokenType.NEWLINE])
    self.assert_tok_types("\v\n", [TokenType.WHITESPACE, TokenType.NEWLINE])
    self.assert_tok_types("\r\n", [TokenType.NEWLINE])

  def test_indented_comment(self):
    self.assert_tok_types("""\
      # This multiline-comment should be reflowed
      # into a single comment
      # on one line
      """, [TokenType.WHITESPACE, TokenType.COMMENT, TokenType.NEWLINE,
            TokenType.WHITESPACE, TokenType.COMMENT, TokenType.NEWLINE,
            TokenType.WHITESPACE, TokenType.COMMENT, TokenType.NEWLINE,
            TokenType.WHITESPACE])

  def test_byteorder_mark(self):
    self.assert_tok_types(
        """\ufeffcmake_minimum_required(VERSION 2.8.11)""",
        [TokenType.BYTEORDER_MARK, TokenType.WORD,
         TokenType.LEFT_PAREN, TokenType.WORD, TokenType.WHITESPACE,
         TokenType.UNQUOTED_LITERAL, TokenType.RIGHT_PAREN])

  def test_generator_expression(self):
    self.assert_tok_types(
        r'$<JOIN:${ldgen_libraries},\n>',
        [TokenType.UNQUOTED_LITERAL]
    )

  def test_atword(self):
    self.assert_tok_types(
        r'@PACKAGE_INIT@',
        [TokenType.ATWORD]
    )


if __name__ == '__main__':
  unittest.main()
