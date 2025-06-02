# pylint: disable=W0613
import enum
import logging
import re

from cmakelang.common import InternalError
from cmakelang.format.formatter import get_comment_lines
from cmakelang.lex import TokenType, Token
from cmakelang.parse.argument_nodes import (
    ArgGroupNode, ConditionalGroupNode, PositionalGroupNode
)
from cmakelang.parse.body_nodes import BodyNode, FlowControlNode
from cmakelang.parse.simple_nodes import CommentNode
from cmakelang.parse.common import NodeType, TreeNode
from cmakelang.parse.statement_node import StatementNode
from cmakelang.parse.util import get_min_npargs
from cmakelang.parse import variables
from cmakelang.parse.funs.set import SetFnNode


logger = logging.getLogger(__name__)


def find_statements_in_subtree(subtree, funnames):
  """
  Return a generator that yields all statements in the `subtree` which match
  the provided set of `funnames`.
  """
  if isinstance(subtree, (list, tuple)):
    queue = subtree
  else:
    queue = [subtree]

  while queue:
    node = queue.pop(0)
    if isinstance(node, StatementNode):
      if node.get_funname() in funnames:
        yield node
    for child in node.children:
      if isinstance(child, TreeNode):
        queue.append(child)


def find_nodes_in_subtree(subtree, nodetypes):
  """Return a generator that yields all nodes in the `subtree` which are of the
     given type(s) `nodetypes`.
  """
  if isinstance(subtree, (list, tuple)):
    queue = subtree
  else:
    queue = [subtree]

  while queue:
    node = queue.pop(0)
    if isinstance(node, nodetypes):
      yield node

    if isinstance(node, TreeNode):
      for child in node.children:
        queue.append(child)


def loop_contains_argn(loop_stmt):
  """Return true if the loop statement contains ${ARGN} as an argument"""
  for token in loop_stmt.argtree.get_semantic_tokens():
    if token.type is TokenType.DEREF and token.spelling == "${ARGN}":
      return True
  return False


def statement_is_fundef(node):
  """Return true if a statement node is for a function or macro definition.
  """
  tokens = node.get_semantic_tokens()
  if not tokens:
    return False
  funname = tokens[0].spelling.lower()
  return funname in ("function", "macro")


def get_prefix_comment(prevchild):
  """
  Expect a sequence of COMMENT, WHITESPACE, <node>. If this sequence is true,
  return the comment node.
  """
  for idx in range(2):
    if not isinstance(prevchild[idx], TreeNode):
      return None
  if not prevchild[0].node_type is NodeType.WHITESPACE:
    return None

  newline_count = 0
  for token in prevchild[0].get_tokens():
    newline_count += token.spelling.count("\n")
  if newline_count > 1:
    return None

  if not prevchild[1].node_type is NodeType.COMMENT:
    return None
  return prevchild[1]


def get_list_outvar(node):
  """Given a statement parse node for a `list()` command, inspect the
     subcommand and extract the token corresponding to the output
     variable name."""
  semtoks = node.argtree.parg_groups[0].get_tokens(kind="semantic")
  listcmd = semtoks[0].spelling
  if listcmd.upper() in ("LENGTH", "GET", "JOIN", "SUBLIST"):
    return semtoks[-1]
  return None


class Scope(enum.IntEnum):
  CACHE = 0  # global scope (public)
  INTERNAL = 1  # global scope (private)
  PARENT = 2  # indeterminate scope, but it can't be CACHE
  LOCAL = 3  # function local scope
  DIRECTORY = 4  # directory scope
  LOOP = 5  # loop variable name
  ARGUMENT = 6  # argument variable name


def get_scope_of_assignment(set_node):
  """Return the scope of the assignment."""
  args = set_node.argtree
  if isinstance(args, SetFnNode):
    if args.cache:
      if args.cache.type == "INTERNAL":
        return Scope.INTERNAL
      return Scope.CACHE

    if args.parent_scope:
      return Scope.PARENT

  # assume block (directory) scope
  prev = set_node
  parent = args.parent
  while parent:
    if isinstance(parent, FlowControlNode):
      block = parent.get_block_with(prev)
      if block.open_stmt.get_funname() == "function":
        # found ourselves in the body of a function, which means that we are
        # local scope
        return Scope.LOCAL
      if block.open_stmt.get_funname() == "macro":
        # found ourselves in the body of a macro, which means that we are
        # in parent scope
        return Scope.PARENT
    prev = parent
    parent = parent.parent
  return Scope.DIRECTORY


def make_varref_callback(repl):
  """Return a function for regex substitution to replace a variable
     substitution regex with a stub string."""

  def varref_callback(matchobj):
    if matchobj.group(1):
      return matchobj.group(1) + repl
    return repl

  return varref_callback


def mock_varrefs(tokenstr, repl=None):
  """Recursively replace variable references with a dummy string until all
     variable references are resolved.

  :see: https://cmake.org/cmake/help/v3.12/policy/CMP0053.html#policy:CMP0053
  """

  if repl is None:
    repl = "foo"

  varref = re.compile(
      r"(?<!\\)(\\\\)*"
      r"\$\{(?:(?:[A-Za-z0-9_./+-])|(?:\\[^A-Za-z0-9_./+-]))+\}")

  # NOTE(josh): in python2 we are not allowed to use \1 when the match is
  # empty, so we have to use a callback function to implement the replacement.
  while varref.search(tokenstr):
    tokenstr = varref.sub(make_varref_callback(repl), tokenstr)
  return tokenstr


class LintChecker(object):
  def __init__(self, cfg, local_ctx):
    self.cfg = cfg
    self.local_ctx = local_ctx
    self._node_stack = []
    self._indent_token = None

  @property
  def context(self):
    """
    Convenience shim to avoid a big diff in the code. Before this was a class
    each checker take in a `(cfg, local_ctx)` as input arguments. Instead of
    replacing each usage of these arguments, we just do
    `(cfg, local_ctx) = self.context` at the start of each method and keep the
    rest of the code the same.
    """
    return (self.cfg, self.local_ctx)

  def check_basics(self, infile_content):
    """Perform  basic checks before even lexing the file
    """
    (cfg, local_ctx) = self.context
    lines = infile_content.split("\n")
    indent_regex = re.compile(r"([ \t]*)(.*)")

    for lineno, line in enumerate(lines):
      # NOTE(josh): The tokenizer starts lineno at "1", so we must do the same
      # here in order to be consistent.
      lineno += 1
      if len(line) > cfg.format.line_width:
        local_ctx.record_lint(
            "C0301", len(line), cfg.format.line_width, location=(lineno,))

      if line.endswith("\r"):
        if cfg.format.line_ending == "unix":
          local_ctx.record_lint(
              "C0327", "windows", location=(lineno,))
        line = line[:-1]
      else:
        if cfg.format.line_ending == "windows":
          local_ctx.record_lint(
              "C0327", "unix", location=(lineno,))

      if len(line.rstrip()) != len(line):
        local_ctx.record_lint("C0303", location=(lineno,))

      match = indent_regex.match(line)
      indentation = match.group(1)
      if not cfg.format.use_tabchars:
        if "\t" in indentation:
          colno = indentation.find("\t")
          local_ctx.record_lint(
              "C0306", "tab", "space", location=(lineno, colno))
      elif cfg.format.fractional_tab_policy == "round-up":
        if " " in indentation:
          colno = indentation.find(" ")
          local_ctx.record_lint(
              "C0306", "space", "tab", location=(lineno, colno))
      else:
        subindent = indentation.lstrip("\t")
        if "\t" in subindent:
          colno = len(indentation) - len(subindent)
          local_ctx.record_lint(
              "C0306", "space", "tab", location=(lineno, colno))

    # check that the file ends with newline
    if not infile_content.endswith("\n"):
      local_ctx.record_lint("C0304", location=(len(lines),))

  def check_tokens(self, tokens):
    """Look for anything that looks like an incomplete variable substitution."""
    (_, local_ctx) = self.context

    missing_suffix = re.compile(
        r"(?<!\\)(?:\\\\)*"
        r"(\$\{)((?:(?:[A-Za-z0-9_./+-])|(?:\\[^A-Za-z0-9_./+-]))+)")

    missing_prefix = re.compile(
        r"(?<!\\)(?:\\\\)*"
        r"(\$|\{)((?:(?:[A-Za-z0-9_./+-])|(?:\\[^A-Za-z0-9_./+-]))+)")

    match_types = (
        TokenType.QUOTED_LITERAL,
        TokenType.UNQUOTED_LITERAL,
        TokenType.BRACKET_ARGUMENT
    )

    for token in tokens:
      if token.type not in match_types:
        continue

      resolved = mock_varrefs(token.spelling)

      match = missing_prefix.search(resolved)
      if match and variables.CASE_SENSITIVE_REGEX.match(match.group(2)):
        catmatch = "".join(match.group(1, 2))
        if catmatch == "$ENV":
          # This is an environment variable reference, so we don't try to
          # match it
          continue

        local_ctx.record_lint(
            "W0106", "open", catmatch, location=token.get_location())
        continue

      match = missing_suffix.search(resolved)
      if match and variables.CASE_SENSITIVE_REGEX.match(match.group(2)):
        local_ctx.record_lint(
            "W0106", "closing", catmatch, location=token.get_location())
        continue

  def check_for_custom_parse_logic(self, stmt_node):
    """Ensure that a function or macro definition doesn't contain custom parser
      logic. The check is heuristic, but what we look for is a loop over ARGN
      where the body of the loop contains multiple conditional checks against
      the string value of the arguments
    """
    (cfg, local_ctx) = self.context
    block = stmt_node.parent.get_block_with(stmt_node)
    for _ in find_statements_in_subtree(
        block.body, ("cmake_parse_arguments",)):
      # function/macro definition uses the std parser, so the check is complete
      return

    for loop_stmt in find_statements_in_subtree(
        block.body, ("foreach", "while")):
      if loop_contains_argn(loop_stmt):
        conditional_count = 0
        loopvar = loop_stmt.argtree.get_semantic_tokens()[0]
        loop_body = loop_stmt.parent.get_block_with(loop_stmt).body
        for conditional in find_nodes_in_subtree(
            loop_body, ConditionalGroupNode):
          tokens = conditional.get_semantic_tokens()
          if not tokens:
            continue
          if tokens[0].spelling == loopvar.spelling:
            if tokens[1].spelling in ("STREQUAL", "MATCHES"):
              conditional_count += 1
        if conditional_count > cfg.lint.max_conditionals_custom_parser:
          local_ctx.record_lint("C0201", location=loop_stmt.get_location())
          return

  def check_argument_names(self, defn_node):
    """Check that the argument names in a function or macro definition match
      the required pattern."""
    (cfg, local_ctx) = self.context
    tokens = defn_node.argtree.get_semantic_tokens()[1:]
    seen_names = set()
    uncase_names = set()
    for token in tokens:  # named arguments
      if token.type is TokenType.RIGHT_PAREN:
        break
      if token.type is not TokenType.WORD:
        local_ctx.record_lint(
            "E0109", token.spelling, location=token.get_location())
      else:
        if token.spelling in seen_names:
          local_ctx.record_lint(
              "E0108", token.spelling, location=token.get_location())
        elif token.spelling.lower() in uncase_names:
          local_ctx.record_lint(
              "C0202", token.spelling, location=token.get_location())
        elif not re.match(cfg.lint.argument_var_pattern, token.spelling):
          local_ctx.record_lint(
              "C0103", "argument", token.spelling,
              cfg.lint.argument_var_pattern, location=token.get_location())
        seen_names.add(token.spelling)
        uncase_names.add(token.spelling.lower())

    if len(tokens) > cfg.lint.max_arguments:
      local_ctx.record_lint("R0913", len(tokens), cfg.lint.max_arguments,
                            location=defn_node.get_location())

  def check_name_against_pattern(self, defn_node, pattern):
    """Check that a function or macro name matches the required pattern."""
    (_, local_ctx) = self.context
    tokens = defn_node.get_semantic_tokens()
    funname = tokens.pop(0).spelling  # statement name "function" or "macro"
    tokens.pop(0)  # lparen

    token = tokens.pop(0)  # function/macro name
    if not re.match(pattern, token.spelling):
      local_ctx.record_lint(
          "C0103", funname.lower(), token.spelling, pattern,
          location=token.get_location())

  def check_defn(self, defn_node, name_pattern):
    """Perform checks on a function or macro"""
    (cfg, local_ctx) = self.context
    self.check_for_custom_parse_logic(defn_node)
    self.check_name_against_pattern(defn_node, name_pattern)
    self.check_argument_names(defn_node)

    # TODO(josh): I guess logically the following checks should really be
    # part of check_body... in the case that the body within a function.
    # I'm not sure what the best way to organize that flow. In any case
    # we'll report the following errors at the first line following the
    # opening statement, which is where the body starts
    body_line = defn_node.get_tokens()[-1].get_location().line + 1

    block = defn_node.parent.get_block_with(defn_node)
    return_count = sum(
        1 for _ in find_statements_in_subtree(block.body, ("return",)))
    if return_count > cfg.lint.max_returns:
      local_ctx.record_lint("R0911", return_count, cfg.lint.max_returns,
                            location=(body_line,))

    branch_count = sum(
        1 for _ in find_statements_in_subtree(
            block.body, ("if", "elseif", "else")))
    if branch_count > cfg.lint.max_branches:
      local_ctx.record_lint("R0912", branch_count, cfg.lint.max_branches,
                            location=(body_line,))

    stmt_count = sum(
        1 for _ in find_nodes_in_subtree(block.body, StatementNode))
    if stmt_count > cfg.lint.max_statements:
      local_ctx.record_lint("R0915", stmt_count, cfg.lint.max_statements,
                            location=(body_line,))

  def check_fundef(self, node):
    """Perform checks on a function definition"""
    (cfg, _) = self.context
    self.check_defn(node, cfg.lint.function_pattern)

  def check_macrodef(self, node):
    """Perform checks on a macro definition"""
    (cfg, _) = self.context
    self.check_defn(node, cfg.lint.macro_pattern)

  def check_foreach(self, node):
    """Make sure that the loop variable matches the function argument
       pattern."""
    (cfg, local_ctx) = self.context

    tokens = node.get_semantic_tokens()
    tokens.pop(0)  # statement name "foreach"
    tokens.pop(0)  # lparen
    token = tokens.pop(0)  # loopvaraible
    if not re.match(cfg.lint.argument_var_pattern, token.spelling):
      local_ctx.record_lint(
          "C0103", "argument", token.spelling, cfg.lint.argument_var_pattern,
          location=token.get_location())

  def check_flow_control(self, node):
    """Perform checks on a flowcontrol node."""
    stmt = node.children[0]
    funname = stmt.children[0].children[0].spelling.lower()
    if funname == "function":
      self.check_fundef(stmt)
    elif funname == "macro":
      self.check_macrodef(stmt)
    elif funname == "foreach":
      self.check_foreach(stmt)

  def parse_pragmas_from_token(self, content, row, col, suppressions):
    """Parse any cmake-lint directives (pragmas) from line-comment
      tokens at the current scope."""

    (_, local_ctx) = self.context
    items = content.split()
    for item in items:
      if "=" not in item:
        local_ctx.record_lint("E0011", item, location=(row, col))
        col += len(item) + 1
        continue

      key, value = item.split("=", 1)
      if key == "disable":
        idlist = value.split(",")
        for idstr in idlist:
          if local_ctx.is_idstr(idstr):
            suppressions.append(idstr)
          else:
            local_ctx.record_lint(
                "E0012", idstr, location=(row, col))
      else:
        local_ctx.record_lint(
            "E0011", key, location=(row, col))

  def parse_pragmas_from_comment(self, node):
    """Parse any cmake-lint directives (pragmas) from line comment tokens within
      the comment node."""
    suppressions = []
    for child in node.children:
      if not isinstance(child, Token):
        continue
      if child.type is not TokenType.COMMENT:
        continue
      token = child
      pragma_prefix = "# cmake-lint: "
      if not token.spelling.startswith(pragma_prefix):
        continue

      row, col, _ = token.get_location()
      content = token.spelling[len(pragma_prefix):]
      col += len(pragma_prefix)
      self.parse_pragmas_from_token(content, row, col, suppressions)

    return suppressions

  def check_body(self, node):
    """Perform checks on a body node."""
    (cfg, local_ctx) = self.context
    suppressions = []
    prevchild = [None, None]
    for idx, child in enumerate(node.children):
      if not isinstance(child, TreeNode):
        # Should not be the case. Should we assert here?
        continue

      if child.node_type is NodeType.COMMENT:
        requested_suppressions = self.parse_pragmas_from_comment(child)
        if requested_suppressions:
          lineno = child.get_tokens()[0].get_location().line
          new_suppressions = local_ctx.suppress(lineno, requested_suppressions)
          suppressions.extend(new_suppressions)

      # Check for docstrings
      # TODO(josh): move into flow-control or fundef/macrodef checkers? Would
      # require an API to get siblings
      if child.node_type is NodeType.FLOW_CONTROL:
        stmt = child.children[0]
        if statement_is_fundef(stmt):
          prefix_comment = get_prefix_comment(prevchild)
          if not prefix_comment:
            local_ctx.record_lint(
                "C0111", location=child.get_semantic_tokens()[0].get_location())
          elif not "".join(get_comment_lines(cfg, prefix_comment)).strip():
            local_ctx.record_lint(
                "C0112", location=child.get_semantic_tokens()[0].get_location())

      # Check spacing between statements at block level
      # TODO(josh): move into flow-control and statement checkers? Would
      # require an API to get siblings
      if child.node_type in (NodeType.FLOW_CONTROL, NodeType.STATEMENT):
        if prevchild[0] is None:
          pass
        elif prevchild[0].node_type is not NodeType.WHITESPACE:
          local_ctx.record_lint("C0321", location=child.get_location())
        elif prevchild[0].count_newlines() < 1:
          local_ctx.record_lint("C0321", location=child.get_location())
        elif prevchild[0].count_newlines() < cfg.lint.min_statement_spacing:
          local_ctx.record_lint(
              "C0305", "not enough", location=child.get_location())
        elif prevchild[0].count_newlines() > cfg.lint.max_statement_spacing:
          local_ctx.record_lint(
              "C0305", "too many", location=child.get_location())

      if (isinstance(child, StatementNode) and
          child.get_funname() in ("break", "continue", "return")):
        for _ in find_nodes_in_subtree(node.children[idx + 1:], StatementNode):
          local_ctx.record_lint("W0101", location=child.get_location())
          break

      prevchild[1] = prevchild[0]
      prevchild[0] = child

    lineno = node.get_tokens()[-1].get_location().line
    if suppressions:
      local_ctx.unsuppress(lineno, suppressions)

  def check_arggroup(self, node):
    (_, local_ctx) = self.context
    kwargs_seen = set()
    for child in node.children:
      if isinstance(child, TreeNode) and child.node_type is NodeType.KWARGGROUP:
        kwarg_token = child.get_semantic_tokens()[0]
        kwarg = kwarg_token.spelling.upper()
        if kwarg in ("AND", "OR", "COMMAND", "PATTERN", "REGEX"):
          continue
        if kwarg in kwargs_seen:
          local_ctx.record_lint(
              "E1122", kwarg, location=kwarg_token.get_location())
        kwargs_seen.add(kwarg)

  def check_positional_group(self, node):
    """Perform checks on a positional group node."""
    (_, local_ctx) = self.context
    if node.spec is None:
      raise InternalError("Missing node.spec for {}".format(node))
    min_npargs = get_min_npargs(node.spec.npargs)
    semantic_tokens = node.get_semantic_tokens()
    if len(semantic_tokens) < min_npargs:
      location = ()
      if semantic_tokens:
        location = semantic_tokens[0].get_location()
      local_ctx.record_lint("E1120", location=location)

  def check_is_in_loop(self, node):
    """Ensure that a break() or continue() statement has a foreach() or
      while() node in it's ancestry."""
    (_, local_ctx) = self.context
    prevparent = node
    parent = node.parent

    while parent:
      if not isinstance(parent, FlowControlNode):
        prevparent = parent
        parent = parent.parent
        continue

      block = parent.get_block_with(prevparent)
      if block is None:
        prevparent = parent
        parent = parent.parent
        continue

      if block.open_stmt.get_funname() in ("foreach", "while"):
        return True

      prevparent = parent
      parent = parent.parent
    local_ctx.record_lint(
        "E0103", node.get_funname(), location=node.get_location())

  def check_assignment(self, node):
    """Checks on a variable assignment."""
    (cfg, local_ctx) = self.context
    scope = get_scope_of_assignment(node)

    if node.get_funname() == "set":
      varname = node.argtree.varname
    elif node.get_funname() == "list":
      varname = get_list_outvar(node)
      if varname is None:
        return
    else:
      logger.warning("Unexpected node %s (%s)", node, node.get_funname())
      return

    resolved = mock_varrefs(varname.spelling, "")

    # The variable name is hidden behind a variable reference so we can't
    # check it until we have a model for variables in scope.
    if not resolved:
      return

    if scope is Scope.CACHE:
      # variable is global scope and public
      pattern = cfg.lint.global_var_pattern
      if not re.match(pattern, resolved):
        local_ctx.record_lint(
            "C0103", "CACHE variable", varname.spelling, pattern,
            location=varname.get_location())
    elif scope is Scope.INTERNAL:
      # variable is global scope but private
      pattern = cfg.lint.internal_var_pattern
      if not re.match(pattern, resolved):
        local_ctx.record_lint(
            "C0103", "INTERNAL variable", varname.spelling,
            cfg.lint.internal_var_pattern,
            location=varname.get_location())
    elif scope is Scope.PARENT:
      # indeterminate scope, but it's not global
      pattern = "|".join([
          cfg.lint.public_var_pattern,
          cfg.lint.private_var_pattern,
          cfg.lint.local_var_pattern,
      ])

      if not re.match(pattern, resolved):
        local_ctx.record_lint(
            "C0103", "PARENT_SCOPE variable", varname.spelling, pattern,
            location=varname.get_location())
    elif scope is Scope.LOCAL:
      pattern = cfg.lint.local_var_pattern
      if not re.match(pattern, resolved):
        local_ctx.record_lint(
            "C0103", "local variable", varname.spelling,
            pattern, location=varname.get_location())
    elif scope is Scope.DIRECTORY:
      # We cannot tell by assignment whether it's meant to be public or private,
      # but it must match one of these patterns
      pattern = "|".join([
          cfg.lint.public_var_pattern,
          cfg.lint.private_var_pattern,
      ])
      if not re.match(pattern, resolved):
        local_ctx.record_lint(
            "C0103", "directory variable", varname.spelling, pattern,
            location=varname.get_location())

  def check_statement(self, node):
    """Perform checks on a statement."""
    if node.get_funname() in ("break", "continue"):
      self.check_is_in_loop(node)
    elif node.get_funname() in ["set", "list"]:
      self.check_assignment(node)

  def check_varname(self, varname, token, contextstr):
    """
    Record lint if the varname is a case-insensitive match to any builtin
    variable names, meaning that the author likely made a spelling mistake.
    """
    (_, local_ctx) = self.context

    imatch = variables.CASE_INSENSITIVE_REGEX.match(varname)
    if not imatch:
      # variable name isn't a match for any builtins
      return

    for groupstr in imatch.groups():
      if groupstr is not None:
        # variable name matches a dynamic pattern and we don't have the context
        # yet to actually compare against these
        return

    if not variables.CASE_SENSITIVE_REGEX.match(varname):
      # variable name is a match for a builtin except for case
      local_ctx.record_lint(
          "W0105", contextstr, varname,
          location=token.get_location())

  def check_variable_assignments(self, tree):
    """
    Check if any variable assignments are a case-insensitive match to any
    builtin variable names. This is probably a spelling error.
    """
    for stmt in find_statements_in_subtree(tree, ["set", "list"]):
      if stmt.get_funname() == "set":
        token = stmt.argtree.varname
      elif stmt.get_funname() == "list":
        token = stmt.argtree.parg_groups[0].get_tokens(kind="semantic")[1]
      else:
        continue
      self.check_varname(token.spelling, token, "Assignment to")

  def check_variable_references(self, tree):
    """
    Check if any variable references are a case-insensitive match to any
    builtin variable names. This is probably a spelling error.
    """
    # TODO(josh): replace with a stateful parser that builds up
    # global/directory/local namespaces and can check for usage before
    # assignment, shadowing, etc
    for token in tree.get_tokens(kind="semantic"):
      if token.type not in (
          TokenType.QUOTED_LITERAL, TokenType.DEREF):
        continue
      for varname in re.findall(r"\$\{([\w_]+)\}", token.spelling):
        self.check_varname(varname, token, "Reference to")

  def check_comment(self, node):
    if not self.am_in_statement():
      return

    requested_suppressions = self.parse_pragmas_from_comment(node)
    if not requested_suppressions:
      return

    (_cfg, local_ctx) = self.context
    lineno = node.get_tokens()[0].get_location().line
    new_suppressions = local_ctx.suppress(lineno, requested_suppressions)
    local_ctx.unsuppress(lineno + 1, new_suppressions)

  def am_in_statement(self):
    for node in self._node_stack:
      if isinstance(node, StatementNode):
        return True
    return False

  def am_in_arggroup(self):
    for node in self._node_stack:
      if isinstance(node, ArgGroupNode):
        return True
    return False

  def get_scope_depth(self):
    depth = 0
    for node in self._node_stack:
      if isinstance(node, BodyNode):
        depth += 1
    return depth

  def check_token(self, token):
    (cfg, local_ctx) = self.context

    if token.type is TokenType.WHITESPACE and token.get_location().col == 0:
      # A whitespace token at column zero is an intentation. It's spelling is
      # composed of whitespace characters (space, tab) composing the indent.
      self._indent_token = token
      return

    if self._indent_token:
      # This is the first non-whitespace token on this line
      desired_indentation = (self.get_scope_depth() - 1) * cfg.format.tab_size

      # Instead of using the column, we could get the previous token in the
      # token stream
      actual_indentation = (
          len(self._indent_token.spelling) +
          self._indent_token.spelling.count("\t") * (cfg.format.tab_size - 1))

      if self.am_in_arggroup():
        if actual_indentation < desired_indentation:
          local_ctx.record_lint(
              "C0307", self._indent_token.spelling, token.spelling,
              " " * desired_indentation, ">", location=token.get_location())
      else:
        if actual_indentation != desired_indentation:
          msg = "->".join([str(x) for x in self._node_stack])
          local_ctx.record_lint(
              "C0307", self._indent_token.spelling, token.spelling,
              " " * desired_indentation, msg, location=token.get_location())

      self._indent_token = None

  def check_tree(self, node):
    if isinstance(node, Token):
      self.check_token(node)
      return

    if not isinstance(node, TreeNode):
      return

    self._node_stack.append(node)
    if node.node_type is NodeType.BODY:
      self.check_body(node)
    elif node.node_type is NodeType.FLOW_CONTROL:
      self.check_flow_control(node)
    elif isinstance(node, ArgGroupNode):
      self.check_arggroup(node)
    elif isinstance(node, StatementNode):
      self.check_statement(node)
    elif isinstance(node, PositionalGroupNode):
      self.check_positional_group(node)
    elif isinstance(node, CommentNode):
      self.check_comment(node)

    for child in node.children:
      self.check_tree(child)
    self._node_stack.pop(-1)

  def check_parse_tree(self, node):
    self.check_variable_assignments(node)
    self.check_variable_references(node)
    self.check_tree(node)
