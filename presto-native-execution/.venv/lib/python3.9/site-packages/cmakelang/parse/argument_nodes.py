# -*- coding: utf-8 -*-
# pylint: disable=W0613
from __future__ import print_function
from __future__ import unicode_literals

import logging

from cmakelang import lex
from cmakelang.common import InternalError
from cmakelang.parse.printer import dump_tree_tostr
from cmakelang.parse.util import (
    comment_belongs_up_tree,
    get_normalized_kwarg,
    get_tag,
    npargs_is_exact,
    pargs_are_full,
    IMPLICIT_PARG_TYPES,
    CommandSpec,
    PositionalSpec,
    WHITESPACE_TOKENS,
    should_break,
)
from cmakelang.parse.common import (
    NodeType, ParenBreaker, KwargBreaker, TreeNode
)
from cmakelang.parse.simple_nodes import CommentNode, OnOffNode

logger = logging.getLogger(__name__)


DEFAULT_PSPEC = PositionalSpec("*")


class ArgGroupNode(TreeNode):
  """A group-of-groups. Top-level for the argument subtree of a statement, as
     well as the argument subtree of a keyword."""

  def __init__(self):
    super(ArgGroupNode, self).__init__(NodeType.ARGGROUP)
    self.spec = None


class StandardArgTree(ArgGroupNode):
  """Argument tree for most cmake-statement commands. Generically arguments
     are composed of a positional argument list, followed by one or more
     keyword arguments, followed by one or more flags::

      command_name(parg1 parg2 parg3...
              KEYWORD1 kwarg1 kwarg2...
              KEYWORD2 kwarg3 kwarg4...
              FLAG1 FLAG2 FLAG3)

  """

  def __init__(self):
    super(StandardArgTree, self).__init__()
    self.parg_groups = []
    self.kwarg_groups = []
    self.cmdspec = None

  def check_required_kwargs(self, lint_ctx, required_kwargs):
    for kwargnode in self.kwarg_groups:
      required_kwargs.pop(get_normalized_kwarg(kwargnode.keyword.token), None)

    if required_kwargs:
      location = self.get_location()
      for token in self.get_semantic_tokens():
        location = token.get_location()
        break

      # NOTE(josh): inner sorted() needed for stable sort.
      missing_kwargs = sorted(
          (lintid, word) for word, lintid in sorted(required_kwargs.items()))
      for lintid, word in missing_kwargs:
        lint_ctx.record_lint(lintid, word, location=location)

  @classmethod
  def parse2(cls, ctx, tokens, cmdspec, kwargs, breakstack):
    """
    Standard parser for the commands in the form of::

        command_name(parg1 parg2 parg3...
                    KEYWORD1 kwarg1 kwarg2...
                    KEYWORD2 kwarg3 kwarg4...
                    FLAG1 FLAG2 FLAG3)
    The parser starts off as a positional parser. If a keyword or flag is
    encountered the positional parser is popped off the parse stack. If it was
    a keyword then the keyword parser is pushed on the parse stack. If it was
    a flag than a new flag parser is pushed onto the stack.
    """

    # NOTE(josh): we will pop things off this list, so let's make a copy
    pargspecs = list(cmdspec.pargs)
    tree = cls()
    tree.cmdspec = cmdspec

    # If it is a whitespace token then put it directly in the parse tree at
    # the current depth
    while tokens and tokens[0].type in WHITESPACE_TOKENS:
      tree.children.append(tokens.pop(0))
      continue

    # NOTE(josh): if there is only one non-exact legacy specification then we
    # reuse that specification for any additional positional arguments that we
    # pick up. This is to maintain the current/legacy behavior of simple
    # positional argument specifications
    # TODO(josh): double check the reasoning for this. I think it might be
    # mistaken and unnecessary
    default_spec = DEFAULT_PSPEC
    if (len(pargspecs) == 1 and pargspecs[0].legacy
        and not npargs_is_exact(pargspecs[0].nargs)):
      default_spec = pargspecs.pop(0)

    all_flags = list(default_spec.flags)
    for pspec in pargspecs:
      all_flags.extend(pspec.flags)

    kwarg_breakstack = breakstack + [
        KwargBreaker(list(kwargs.keys()) + all_flags)]

    while tokens:
      # If it is a whitespace token then put it directly in the parse tree at
      # the current depth
      if tokens[0].type in WHITESPACE_TOKENS:
        tree.children.append(tokens.pop(0))
        continue

      # If it's a comment, then add it at the current depth
      if tokens[0].type in (lex.TokenType.COMMENT,
                            lex.TokenType.BRACKET_COMMENT):
        if comment_belongs_up_tree(ctx, tokens, tree, breakstack):
          break
        tree.children.append(CommentNode.consume(ctx, tokens))
        continue

      # If it's a sentinel comment, then add it at the current depth
      if tokens[0].type in (lex.TokenType.FORMAT_OFF,
                            lex.TokenType.FORMAT_ON):
        tree.children.append(OnOffNode.consume(ctx, tokens))
        continue

      # Break if the next token belongs to a parent parser, i.e. if it
      # matches a keyword argument of something higher in the stack, or if
      # it closes a parent group.
      if should_break(tokens[0], breakstack):
        # NOTE(josh): if spec.nargs is an exact number of arguments, then we
        # shouldn't break on kwarg match from a parent parser. Instead, we
        # should consume that many tokens. This is a hack to deal with
        # ```install(RUNTIME COMPONENT runtime)``. In this case the second
        # occurance of "runtime" should not match the ``RUNTIME`` keyword
        # and should not break the positional parser.
        # TODO(josh): this is kind of hacky because it will force the positional
        # parser to consume a right parenthesis and will lead to parse errors
        # in the event of a missing positional argument. Such errors will be
        # difficult to debug for the user.
        if pargspecs:
          pspec = pargspecs[0]
        else:
          pspec = default_spec

        if not npargs_is_exact(pspec.nargs) or pspec.nargs == 0:
          break

      ntokens = len(tokens)
      word = get_normalized_kwarg(tokens[0])
      if word in kwargs:
        with ctx.pusharg(tree):
          subtree = KeywordGroupNode.parse(
              ctx, tokens, word, kwargs[word], kwarg_breakstack)
        tree.kwarg_groups.append(subtree)
      else:
        if pargspecs:
          pspec = pargspecs.pop(0)
        else:
          pspec = default_spec

        other_flags = []
        for otherspec in pargspecs:
          for flag in otherspec.flags:
            if flag in pspec.flags:
              continue
            other_flags.append(flag)
        positional_breakstack = breakstack + [
            KwargBreaker(list(kwargs.keys()) + other_flags)]

        with ctx.pusharg(tree):
          subtree = PositionalGroupNode.parse2(
              ctx, tokens, pspec, positional_breakstack)
          tree.parg_groups.append(subtree)

      if len(tokens) >= ntokens:
        raise InternalError(
            "parsed an empty subtree at {}:\n  {}\n pspec: {}"
            .format(tokens[0], dump_tree_tostr([tree]), pspec))
      tree.children.append(subtree)
    return tree

  @classmethod
  def parse(cls, ctx, tokens, npargs, kwargs, flags, breakstack):
    """
    Standard parser for the commands in the form of::

        command_name(parg1 parg2 parg3...
                    KEYWORD1 kwarg1 kwarg2...
                    KEYWORD2 kwarg3 kwarg4...
                    FLAG1 FLAG2 FLAG3)

    The parser starts off as a positional parser. If a keyword or flag is
    encountered the positional parser is popped off the parse stack. If it was
    a keyword then the keyword parser is pushed on the parse stack. If it was
    a flag than a new flag parser is pushed onto the stack.
    """
    if isinstance(npargs, IMPLICIT_PARG_TYPES):
      pargspecs = [PositionalSpec(npargs, flags=flags, legacy=True)]
    else:
      assert isinstance(npargs, (list, tuple)), (
          "Invalid positional group specification of type {}"
          .format(type(npargs).__name__))
      assert flags is None, (
          "Invalid usage of old-style 'flags' parameter with new style"
          " positional group specifications")
      pargspecs = npargs

    return cls.parse2(
        ctx, tokens, CommandSpec("<none>", pargspecs), kwargs, breakstack)


class StandardParser(object):
  def __init__(self, npargs=None, kwargs=None, flags=None, doc=None):
    if npargs is None:
      npargs = "*"
    if flags is None:
      flags = []
    if kwargs is None:
      kwargs = {}

    self.npargs = npargs
    self.kwargs = kwargs
    self.flags = flags
    self.doc = doc

  def __call__(self, ctx, tokens, breakstack):
    return StandardArgTree.parse(
        ctx, tokens, self.npargs, self.kwargs, self.flags, breakstack)


class StandardParser2(object):
  def __init__(self, cmdspec=None, funtree=None, doc=None):
    if cmdspec is None:
      cmdspec = ("<none>", "*")
    if funtree is None:
      funtree = {}

    self.cmdspec = cmdspec
    self.funtree = funtree
    self.doc = doc

  @property
  def pspec(self):
    return self.cmdspec.pargs

  @property
  def kwargs(self):
    return self.funtree

  def __call__(self, ctx, tokens, breakstack):
    return StandardArgTree.parse2(
        ctx, tokens, self.cmdspec, self.funtree, breakstack)


class KeywordNode(TreeNode):
  """Node that stores a single keyword token and possibly it's associated
     comments."""

  def __init__(self):
    super(KeywordNode, self).__init__(NodeType.KEYWORD)
    self.token = None

  @classmethod
  def parse(cls, _ctx, tokens):
    node = cls()
    node.token = tokens.pop(0)
    node.children.append(node.token)
    # TODO(josh)[c490dba]: allow keywords to have trailing comments
    # consume_trailing_comment(node, tokens)
    return node


class KeywordGroupNode(TreeNode):
  """Argument subtree for a keyword and its arguments."""

  def __init__(self):
    super(KeywordGroupNode, self).__init__(NodeType.KWARGGROUP)
    self.keyword = None
    self.body = None

  @classmethod
  def parse(cls, ctx, tokens, word, subparser, breakstack):
    """
    Parse a standard `KWARG arg1 arg2 arg3...` style keyword argument list.
    """
    assert tokens[0].spelling.upper() == word.upper(), \
        "somehow dispatched wrong kwarg parse"

    tree = cls()
    keyword = KeywordNode.parse(ctx, tokens)
    tree.keyword = keyword
    tree.children.append(keyword)

    # If it is a whitespace token then put it directly in the parse tree at
    # the current depth
    while tokens and tokens[0].type in WHITESPACE_TOKENS:
      tree.children.append(tokens.pop(0))

    ntokens = len(tokens)
    with ctx.pusharg(tree):
      subtree = subparser(ctx, tokens, breakstack)
    if len(tokens) < ntokens:
      tree.body = subtree
      tree.children.append(subtree)
    return tree


class PositionalGroupNode(TreeNode):
  """Argument subtree for one or more single positional argument tokens."""

  def __init__(self, sortable=False, tags=None):
    super(PositionalGroupNode, self).__init__(NodeType.PARGGROUP)
    self.sortable = sortable
    self.spec = None
    self.tags = []
    if tags is not None:
      self.tags.extend(tags)

  @classmethod
  def parse2(cls, ctx, tokens, spec, breakstack):
    """
    Parse a continuous sequence of `npargs` positional arguments. If npargs is
    an integer we will consume exactly that many arguments. If it is not an
    integer then it is a string meaning:

    * "?": zero or one
    * "*": zero or more
    * "+": one or more
    """

    tree = cls(sortable=spec.sortable, tags=spec.tags)
    tree.spec = spec
    nconsumed = 0

    # Strip off any preceeding whitespace (note that in most cases this has
    # already been done but in some cases (such ask kwarg subparser) where
    # it hasn't
    while tokens and tokens[0].type in WHITESPACE_TOKENS:
      tree.children.append(tokens.pop(0))

    # If the first non-whitespace token is a cmake-format tag annotating
    # sortability, then parse it out here and record the annotation
    if tokens and get_tag(tokens[0]) in ("sortable", "sort"):
      tree.sortable = True
    elif tokens and get_tag(tokens[0]) in ("unsortable", "unsort"):
      tree.sortable = False

    while tokens:

      # Break if we have consumed   enough positional arguments
      if pargs_are_full(spec.nargs, nconsumed):
        break

      # Break if the next token belongs to a parent parser, i.e. if it
      # matches a keyword argument of something higher in the stack, or if
      # it closes a parent group.
      if should_break(tokens[0], breakstack):
        # NOTE(josh): if spec.nargs is an exact number of arguments, then we
        # shouldn't break on kwarg match from a parent parser. Instead, we
        # should consume the token. This is a hack to deal with
        # ```install(RUNTIME COMPONENT runtime)``. In this case the second
        # occurance of "runtime" should not match the ``RUNTIME`` keyword
        # and should not break the positional parser.
        # TODO(josh): this is kind of hacky because it will force the positional
        # parser to consume a right parenthesis and will lead to parse errors
        # in the event of a missing positional argument. Such errors will be
        # difficult to debug for the user.
        if not npargs_is_exact(spec.nargs):
          break

        if tokens[0].type == lex.TokenType.RIGHT_PAREN:
          break

      # If this is the start of a parenthetical group, then parse the group
      # NOTE(josh): syntatically this probably shouldn't be allowed here, but
      # cmake seems to accept it so we probably should too.
      if tokens[0].type == lex.TokenType.LEFT_PAREN:
        with ctx.pusharg(tree):
          subtree = ParenGroupNode.parse(ctx, tokens, breakstack)
        tree.children.append(subtree)
        continue

      # If it is a whitespace token then put it directly in the parse tree at
      # the current depth
      if tokens[0].type in WHITESPACE_TOKENS:
        tree.children.append(tokens.pop(0))
        continue

      # If it's a comment token not associated with an argument, then put it
      # directly into the parse tree at the current depth
      if tokens[0].type in (lex.TokenType.COMMENT,
                            lex.TokenType.BRACKET_COMMENT):
        if comment_belongs_up_tree(ctx, tokens, tree, breakstack):
          break

        child = CommentNode.consume(ctx, tokens)
        tree.children.append(child)
        continue

      # If it's a sentinel comment, then add it at the current depth
      if tokens[0].type in (lex.TokenType.FORMAT_OFF,
                            lex.TokenType.FORMAT_ON):
        tree.children.append(OnOffNode.consume(ctx, tokens))
        continue

      # Otherwise is it is a positional argument, so add it to the tree as such
      if get_normalized_kwarg(tokens[0]) in spec.flags:
        child = TreeNode(NodeType.FLAG)
      else:
        child = TreeNode(NodeType.ARGUMENT)

      child.children.append(tokens.pop(0))
      CommentNode.consume_trailing(ctx, tokens, child)
      tree.children.append(child)
      nconsumed += 1

    return tree

  @classmethod
  def parse(cls, ctx, tokens, npargs, flags, breakstack, sortable=False,
            tags=None):
    """
    Parse a continuous sequence of `npargs` positional arguments. If npargs is
    an integer we will consume exactly that many arguments. If it is not an
    integer then it is a string meaning:

    * "?": zero or one
    * "*": zero or more
    * "+": one or more
    """

    spec = PositionalSpec(npargs, sortable, tags, flags)
    return cls.parse2(ctx, tokens, spec, breakstack)


class PositionalParser(object):
  def __init__(self, npargs=None, flags=None, sortable=False):
    if npargs is None:
      npargs = "*"
    if flags is None:
      flags = []

    self.npargs = npargs
    self.flags = flags
    self.sortable = sortable

  def __call__(self, ctx, tokens, breakstack):
    return PositionalGroupNode.parse(
        ctx, tokens, self.npargs, self.flags, breakstack, self.sortable)


class ParenGroupNode(TreeNode):
  """Argument subtree for a parenthetical group."""

  def __init__(self):
    super(ParenGroupNode, self).__init__(NodeType.PARENGROUP)

  @classmethod
  def parse(cls, ctx, tokens, _breakstack):
    """
    Consume a parenthetical group of arguments from `tokens` and return the
    parse subtree rooted at this group.  `argstack` contains a stack of all
    early break conditions that are currently "opened".
    """

    assert tokens[0].type == lex.TokenType.LEFT_PAREN
    tree = cls()
    lparen = TreeNode(NodeType.LPAREN)
    lparen.children.append(tokens.pop(0))
    tree.children.append(lparen)

    with ctx.pusharg(tree):
      subtree = ConditionalGroupNode.parse(ctx, tokens, [ParenBreaker()])
    tree.children.append(subtree)

    if tokens[0].type != lex.TokenType.RIGHT_PAREN:
      raise ValueError(
          "Unexpected {} token at {}, expecting r-paren, got {}"
          .format(tokens[0].type.name, tokens[0].get_location(),
                  tokens[0].content))
    rparen = TreeNode(NodeType.RPAREN)
    rparen.children.append(tokens.pop(0))
    tree.children.append(rparen)

    # NOTE(josh): parenthetical groups can have trailing comments because
    # they have closing punctuation
    CommentNode.consume_trailing(ctx, tokens, tree)

    return tree


CONDITIONAL_FLAGS = [
    "COMMAND",
    "DEFINED",
    "EQUAL",
    "EXISTS",
    "GREATER",
    "LESS",
    "IS_ABSOLUTE",
    "IS_DIRECTORY",
    "IS_NEWER_THAN",
    "IS_SYMLINK",
    "MATCHES",
    "NOT",
    "POLICY",
    "STRLESS",
    "STRGREATER",
    "STREQUAL",
    "TARGET",
    "TEST",
    "VERSION_EQUAL",
    "VERSION_GREATER",
    "VERSION_LESS",
]


class ConditionalGroupNode(ArgGroupNode):
  @classmethod
  def parse(cls, ctx, tokens, breakstack):
    """
    Parser for the commands that take conditional arguments. Similar to the
    standard parser but it understands parentheses and can generate
    parenthentical groups::

        while(CONDITION1 AND (CONDITION2 OR CONDITION3)
              OR (CONDITION3 AND (CONDITION4 AND CONDITION5)
              OR CONDITION6)
    """
    kwargs = {
        'AND': cls.parse,
        'OR': cls.parse
    }
    flags = list(CONDITIONAL_FLAGS)
    tree = cls()

    # If it is a whitespace token then put it directly in the parse tree at
    # the current depth
    while tokens and tokens[0].type in WHITESPACE_TOKENS:
      tree.children.append(tokens.pop(0))
      continue

    flags = [flag.upper() for flag in flags]
    breaker = KwargBreaker(list(kwargs.keys()))
    child_breakstack = breakstack + [breaker]

    while tokens:
      # Break if the next token belongs to a parent parser, i.e. if it
      # matches a keyword argument of something higher in the stack, or if
      # it closes a parent group.
      if should_break(tokens[0], breakstack):
        break

      # If it is a whitespace token then put it directly in the parse tree at
      # the current depth
      if tokens[0].type in WHITESPACE_TOKENS:
        tree.children.append(tokens.pop(0))
        continue

      # If it's a comment, then add it at the current depth
      if tokens[0].type in (lex.TokenType.COMMENT,
                            lex.TokenType.BRACKET_COMMENT):
        # TODO(josh): not sure if we should check comment_belongs_up_tree
        # here
        child = CommentNode()
        tree.children.append(child)
        child.children.append(tokens.pop(0))
        continue

      # If it's a sentinel comment, then add it at the current depth
      if tokens[0].type in (lex.TokenType.FORMAT_OFF,
                            lex.TokenType.FORMAT_ON):
        tree.children.append(OnOffNode.consume(ctx, tokens))
        continue

      # If this is the start of a parenthetical group, then parse the group
      if tokens[0].type == lex.TokenType.LEFT_PAREN:
        with ctx.pusharg(tree):
          subtree = ParenGroupNode.parse(ctx, tokens, breakstack)
        tree.children.append(subtree)
        continue

      ntokens = len(tokens)
      word = get_normalized_kwarg(tokens[0])
      if word in kwargs:
        with ctx.pusharg(tree):
          subtree = KeywordGroupNode.parse(
              ctx, tokens, word, kwargs[word], child_breakstack)
        assert len(tokens) < ntokens, "parsed an empty subtree"
        tree.children.append(subtree)
        continue

      # Otherwise is it is a positional argument, so add it to the tree as such
      with ctx.pusharg(tree):
        child = PositionalGroupNode.parse(
            ctx, tokens, '+', flags, child_breakstack)
      # token = tokens.pop(0)
      # if get_normalized_kwarg(token) in flags:
      #   child = TreeNode(NodeType.FLAG)
      # else:
      #   child = TreeNode(NodeType.ARGUMENT)

      # child.children.append(token)
      # consume_trailing_comment(child, tokens)
      tree.children.append(child)

    return tree
