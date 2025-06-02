# -*- coding: utf-8 -*-
"""
Parse ctest testfiles and re-emit the test specification in a more structured
format.
"""


import argparse
import collections
import io
import json
import logging
import os
import sys
from xml.etree import ElementTree as ET

from cmakelang.format import __main__
from cmakelang import lex
from cmakelang import parse


logger = logging.getLogger(__name__)


class TestSpec(object):
  """Stores agregated ctest specification, including the name, command, and
     any properties that are set.
  """

  def __init__(self, name, argv, cwd):
    self.name = name
    self.argv = argv
    self.cwd = cwd
    self.props = {}

  def as_odict(self):
    """Return a dictionary representation of the specification, suitable for
       serialization to JSON.
    """
    props = collections.OrderedDict()
    for key, value in sorted(self.props.items()):
      if key == "LABELS":
        value = value.split(";")
      if key == "TIMEOUT":
        value = int(value)
      if key.startswith("_"):
        continue
      props[key.lower()] = value

    out = collections.OrderedDict([
        ("name", self.name),
        ("argv", self.argv),
        ("cwd", self.cwd),
        ("props", props),
    ])
    return out

  def as_element(self):
    """Return an ElementTree representation of the specification, suitable
       for serialialization to XML.
    """
    elem = ET.Element("test")
    elem.set("name", self.name)
    elem.set("cwd", self.cwd)

    argv_elem = ET.Element("argv")
    elem.append(argv_elem)

    for value in self.argv:
      item = ET.Element("arg")
      argv_elem.append(item)
      item.set("value", value)

    for key, value in sorted(self.props.items()):
      if key.startswith("_"):
        continue

      if key == "LABELS":
        labels_elem = ET.Element("labels")
        elem.append(labels_elem)
        for label in value.split(";"):
          item = ET.Element("label")
          labels_elem.append(item)
          item.set("value", label)
        continue

      elem.set(key.lower(), value)
    return elem


class ParseContext(object):
  """Persistent context for testfile parsing. Stores a queue of directories
     to process, and callbacks for each of the different cmake statements.
  """

  def __init__(self):
    self.cwd = ""
    self.dirqueue = []
    self.tests = {}

  # pylint: disable=W0613
  def parse_add_test(self, tokens, _breakstack):
    """Parse an add_test() statement. This statement contains the test name
       and the command for the test.
    """
    if not tokens:
      raise RuntimeError("Ran out of tokens while processing add_test")
    if tokens[0].type not in (
        lex.TokenType.WORD, lex.TokenType.UNQUOTED_LITERAL):
      raise RuntimeError("Expected a WORD token but got {}".format(tokens[0]))
    test_name = tokens[0].spelling
    tokens.pop(0)
    test_argv = []
    while tokens and tokens[0].type != lex.TokenType.RIGHT_PAREN:
      token = tokens.pop(0)
      if token.type is lex.TokenType.WHITESPACE:
        continue
      if token.type is lex.TokenType.QUOTED_LITERAL:
        spelling = token.spelling[1:-1]
      else:
        spelling = token.spelling
      test_argv.append(spelling)

    logger.debug("Adding test %s", test_name)
    self.tests[test_name] = TestSpec(test_name, test_argv, self.cwd)

  def parse_set_tests_properties(self, tokens, _breakstack):
    """Parse a set_tests_properties() statement. This statement can set
       properties (key/value strings) on one or more tests.
    """
    test_names = []
    properties = {}

    while (tokens
           and tokens[0].spelling != "PROPERTIES"
           and tokens[0].type != lex.TokenType.RIGHT_PAREN):
      token = tokens.pop(0)
      if token.type == lex.TokenType.WHITESPACE:
        continue
      assert token.type in (
          lex.TokenType.WORD, lex.TokenType.UNQUOTED_LITERAL), (
              "Unexpected {}".format(token))
      test_names.append(token.spelling)

    token = tokens.pop(0)
    assert token.spelling == "PROPERTIES"

    while tokens and tokens[0].type != lex.TokenType.RIGHT_PAREN:
      token = tokens.pop(0)
      if token.type is lex.TokenType.WHITESPACE:
        token = tokens.pop(0)
      assert token.type in (
          lex.TokenType.WORD, lex.TokenType.UNQUOTED_LITERAL), (
              "Unexpected {}".format(token))
      key = token.spelling

      token = tokens.pop(0)
      if token.type is lex.TokenType.WHITESPACE:
        token = tokens.pop(0)

      if token.type is lex.TokenType.QUOTED_LITERAL:
        value = token.spelling[1:-1]
      else:
        value = token.spelling

      properties[key] = value

    for test_name in test_names:
      logger.debug("Updating properties for %s", test_name)
      self.tests[test_name].props.update(properties)

  def parse_subdirs(self, tokens, _breakstack):
    """Parse a subdirs() statement. This statement is actually deprecated in
       cmake but it appears it is still in use by ctest.
    """
    while tokens and tokens[0].type != lex.TokenType.RIGHT_PAREN:
      token = tokens.pop(0)
      if token.type is lex.TokenType.QUOTED_LITERAL:
        spelling = token.spelling[1:-1]
      else:
        spelling = token.spelling
      fullpath = os.path.join(self.cwd, spelling)
      logger.debug("pushing directory %s", fullpath)
      self.dirqueue.append(fullpath)

  def get_db(self):
    """Return the parse database: dictionary of statement names to parse
       functions. While cmake-format uses regular functions we map statement
       names to callbacks within this context.
    """
    return {
        "add_test": self.parse_add_test,
        "subdirs": self.parse_subdirs,
        "set_tests_properties": self.parse_set_tests_properties,
    }

  def parse_file(self, filepath):
    """Parse one file. Read the content, tokenize, and parse.
    """
    if not os.path.exists(filepath):
      logger.warning("%s does not exist", filepath)
      return
    with io.open(filepath, "r", encoding="utf-8") as infile:
      infile_content = infile.read()
    tokens = lex.tokenize(infile_content)
    _ = parse.parse(tokens, self.get_db())

  def start(self, firstdir):
    """Main entry-point into the parse. Pushes the first directory onto the
       queue, and then processes directories one-by-one.
    """
    self.dirqueue = [os.path.abspath(firstdir)]
    while self.dirqueue:
      self.cwd = self.dirqueue.pop(0)
      logger.debug("processing directory %s", self.cwd)
      self.parse_file(os.path.join(self.cwd, "CTestTestfile.cmake"))

  def get_json(self):
    """Return a JSON representation of the test specification."""
    return json.dumps(
        [spec.as_odict() for _, spec in sorted(self.tests.items())],
        indent=2)

  def get_xml(self):
    """Return an XML representation of the test specification."""
    root = ET.Element("ctest")
    for _, spec in sorted(self.tests.items()):
      root.append(spec.as_element())
    buf = io.BytesIO()
    ET.ElementTree(root).write(buf)
    return buf.getvalue().decode("utf-8")


def setup_argparse(argparser):
  argparser.add_argument(
      "--log-level", default="info",
      choices=["debug", "info", "warning", "error"])
  mgroup = argparser.add_mutually_exclusive_group()
  mgroup.add_argument(
      "--json", action="store_const", dest="out_type", const="json")
  mgroup.add_argument(
      "--xml", action="store_const", dest="out_type", const="xml")
  argparser.add_argument("directory", nargs="?", default=".")


def main():
  logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
  argparser = argparse.ArgumentParser(prog="ctest-to", description=__doc__)
  setup_argparse(argparser)
  args = argparser.parse_args()
  logger.setLevel(getattr(logging, args.log_level.upper()))
  ctx = ParseContext()
  ctx.start(args.directory)

  out_type = getattr(args, "out_type", "json")
  if out_type == "json":
    sys.stdout.write(ctx.get_json())
  elif out_type == "xml":
    xml_content = ctx.get_xml()
    try:
      import lxml.etree as etree
      sys.stdout.write(
          etree.tostring(etree.fromstring(xml_content), pretty_print=True)
          .decode("utf-8"))
    except ImportError:
      logger.warning("No lxml, cannot pretty-print")
      sys.stdout.write(xml_content)
  elif out_type is None:
    logger.info("No output requested")


if __name__ == "__main__":
  main()
