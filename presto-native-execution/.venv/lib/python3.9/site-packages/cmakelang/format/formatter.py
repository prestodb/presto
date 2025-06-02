# -*- coding: utf-8 -*-
# pylint: disable=too-many-lines

from __future__ import print_function
from __future__ import unicode_literals
import contextlib
import io
import logging
import re
import sys

from cmakelang import lex
from cmakelang import markup

from cmakelang.common import UserError
from cmakelang.lex import TokenType
from cmakelang.parse.argument_nodes import PositionalGroupNode
from cmakelang.parse.common import FlowType, NodeType, TreeNode
from cmakelang.parse.util import comment_is_tag
from cmakelang.parse import simple_nodes

logger = logging.getLogger(__name__)

BLOCK_TYPES = (NodeType.FLOW_CONTROL, NodeType.BODY, NodeType.COMMENT,
               NodeType.STATEMENT, NodeType.WHITESPACE, NodeType.ONOFFSWITCH,
               NodeType.ATWORDSTATEMENT)
GROUP_TYPES = (NodeType.ARGGROUP, NodeType.KWARGGROUP, NodeType.PARGGROUP,
               NodeType.FLAGGROUP, NodeType.PARENGROUP)
SCALAR_TYPES = (NodeType.FUNNAME, NodeType.ARGUMENT, NodeType.KEYWORD,
                NodeType.FLAG, NodeType.ATWORD)
PAREN_TYPES = (NodeType.LPAREN, NodeType.RPAREN)

MATCH_TYPES = BLOCK_TYPES + GROUP_TYPES + SCALAR_TYPES + PAREN_TYPES


def clamp(value, min_value, max_value):
  """Simple double-ended saturation function."""
  if value > max_value:
    return max_value
  if value < min_value:
    return min_value
  return value


def get_comment_lines(config, node):
  """Given a comment node, iterate through it's tokens and generate a list
  of textual lines."""
  inlines = []
  for token in node.children:
    assert isinstance(token, lex.Token), (
        "Unexpected object as child of comment node")
    if token.type == TokenType.COMMENT:
      inline = token.spelling.strip()

      if (isinstance(node, simple_nodes.CommentNode) and
          node.is_explicit_trailing):
        inlines.append(inline[len(config.markup.explicit_trailing_pattern):])
        continue

      if not config.markup.enable_markup:
        # If markup processing is disabled, preserve the entire line regardless
        # of content
        inlines.append(inline[1:])
      elif inline.startswith('#' * config.markup.hashruler_min_length):
        # If the comment starts with multiple hash chars and it is long enough
        # then treat it as a ruler and preserve it.
        inlines.append(inline[1:])
      else:
        # Otherwise strip off extra hash chars to keep things nice and
        # consistent (i.e. remove acciental `##`` prefixes)
        inlines.append(inline.lstrip('#'))
  return inlines


def format_comment_lines(node, stack_context, line_width):
  """
  Reflow comment lines into the given line width, parsing markup as necessary.
  """
  config = stack_context.config
  inlines = get_comment_lines(config, node)

  if (isinstance(node, simple_nodes.CommentNode) and node.is_explicit_trailing):
    prefix = config.markup.explicit_trailing_pattern
  else:
    prefix = "#"

  if not config.markup.enable_markup:
    return [prefix + line.rstrip() for line in inlines]

  if config.markup.literal_comment_pattern is not None:
    literal_comment_regex = re.compile(config.markup.literal_comment_pattern)
    if literal_comment_regex.match('\n'.join(inlines)):
      return [prefix + line.rstrip() for line in inlines]

  if node.children[0] is stack_context.first_token and (
      config.markup.first_comment_is_literal
      or stack_context.first_token.spelling.startswith("#!")):
    return [prefix + line.rstrip() for line in inlines]

  items = markup.parse(inlines, config)
  markup_lines = markup.format_items(config, max(10, line_width - 2), items)
  return [prefix + (" " * len(line[:1])) + line for line in markup_lines]


def normalize_line_endings(instr):
  """
  Remove trailing whitespace and replace line endings with unix line endings.
  They will be replaced with config.endl during output
  """
  return re.sub('[ \t\f\v]*((\r?\n)|(\r\n?))', '\n', instr)


def count_indentation(linestr):
  """return the number of spaces at the start of the line"""
  for idx, char in enumerate(linestr):
    if char != ' ':
      return idx
  return 0


def replace_with_tabs(content, config):
  """
  Apply the tab policy to `content`. If config.use_tabchars is true, then
  replace spaces at the beginning of every line with tabs according to
  config.tab_size and config.fractional_tab_policy.
  """
  if not config.format.use_tabchars:
    return content

  outlines = []
  for inline in content.split("\n"):
    num_spaces = count_indentation(inline)
    num_tabs, fractional = divmod(num_spaces, config.format.tab_size)
    outline = "\t" * num_tabs
    if fractional:
      if config.format.fractional_tab_policy == "use-space":
        outline += " " * fractional
      elif config.format.fractional_tab_policy == "round-up":
        outline += "\t"
      else:
        raise UserError(
            "Invalid fractional_tab_policy='{}'"
            .format(config.format.fractional_tab_policy))
    outline += inline[num_spaces:]
    outlines.append(outline)
  return "\n".join(outlines)


def need_paren_space(spelling, config):
  """
  Return whether or not we need a space between the statement name and the
  starting parenthesis. This aggregates the logic of the two configuration
  options `separate_ctrl_name_with_space` and `separate_fn_name_with_space`.
  """
  upper = spelling.upper()

  if FlowType.get(upper) is not None:
    return config.format.separate_ctrl_name_with_space
  if upper.startswith('END') and FlowType.get(upper[3:]) is not None:
    return config.format.separate_ctrl_name_with_space
  if upper in ["ELSE", "ELSEIF"]:
    return config.format.separate_ctrl_name_with_space

  return config.format.separate_fn_name_with_space


def is_line_comment(node):
  """
  Return true if the node is a pure parser node holding a line comment (i.e.
  not a bracket comment)
  """
  if isinstance(node, CommentNode):
    node = node.pnode

  if not isinstance(node, TreeNode):
    return False

  if not node.children:
    return False

  return node.children[-1].type == TokenType.COMMENT


def get_pathstr(node_path):
  """Given a list of nodes, construct a path string that can be used to
     identify that node in the tree."""

  pathcopy = []
  for node in node_path:
    if isinstance(node, BodyNode):
      continue
    if isinstance(node, ArgGroupNode):
      continue
    pathcopy.append(node)

  # Construct names
  names = []
  for node in pathcopy:
    if isinstance(node, PargGroupNode):
      count = 0
      # pylint: disable=protected-access
      for sibling in node._parent.children:
        if sibling is node:
          break
        if isinstance(sibling, PargGroupNode):
          count += 1
      name = "{}[{}]".format(node.name, count)
    else:
      name = node.name
    names.append(name)

  return "/".join(names)


class Cursor(object):
  """
  Lightweight class to encode integer positions in a 2d grid.

  * x = row
  * y = cols
  """

  def __init__(self, x, y):
    self.x = x  # row
    self.y = y  # col

  def __add__(self, other):
    """Cursor addition is element-wise (i.e. vector) addition"""
    return Cursor(self.x + other[0], self.y + other[1])

  def __sub__(self, other):
    """Cursor subtraction is element-wise (i.e. vector) subtraction"""
    return Cursor(self.x - other[0], self.y - other[1])

  def __getitem__(self, idx):
    """
    A cursor can be accessed as a two-element array. For a cursor `c`,
    `c[0]` refers to the row (`x`) and `c[1]` refers to the column (`y`).
    """

    if idx == 0:
      return self.x
    if idx == 1:
      return self.y

    raise IndexError('Cursor indices must be 0 or 1')

  def __setitem__(self, idx, value):
    """
    Cursor elements can be assigned as a two-element array. For a cursor `c`,
    `c[0]` refers to the row (`x`) and `c[1]` refers to the column (`y`).
    """

    if idx == 0:
      self.x = value
      return
    if idx == 1:
      self.y = value
      return

    raise IndexError('Cursor indices must be 0 or 1')

  def __repr__(self):
    """
    String representation is like "Cursor(12,34)"
    """
    return "Cursor({},{})".format(self.x, self.y)

  def clone(self):
    """
    Return a new `Cursor` object with the same value as this one.
    """
    return Cursor(*self)


class StackContext(object):
  """
  Aggregate information about the current stack. This object is passed down
  through all of the nested :code:`reflow()` function calls.
  """

  def __init__(self, config, first_token=None):
    self.config = config
    self.node_path = []
    self.first_token = first_token

  @contextlib.contextmanager
  def push_node(self, node):
    """
    Push `node` onto the `node_path` and yield a context manager. Pop `node`
    off of `node_path` when the context manager `__exit__()s`
    """
    self.node_path.append(node)
    yield None
    self.node_path.pop(-1)


class AssertTypeDescriptor(object):
  def __init__(self, assert_type, hidden_name):
    self._assert_type = assert_type
    self._hidden_name = hidden_name

  def __get__(self, obj, objtype):
    return getattr(obj, self._hidden_name, None)

  def __set__(self, obj, value):
    assert isinstance(value, self._assert_type)
    setattr(obj, self._hidden_name, value)


class LayoutNode(object):
  """
  An element in the format/layout tree. The structure of the layout tree
  mirrors that of the parse tree. We could store this info the nodes of the
  parse tree itself but it's a little cleaner to keep the functionality
  separate I think.
  """

  _position = AssertTypeDescriptor(Cursor, "__position")
  _size = AssertTypeDescriptor(Cursor, "__size")

  def __init__(self, pnode):
    self.pnode = pnode
    self._position = Cursor(0, 0)  # NOTE(josh): (row, col)
    self._size = Cursor(0, 0)      # NOTE(josh): (rows, cols)

    # If false, reflow was aborted due to algorithmic constraint: e.g.
    # attempt to horiziontally pack a non-bracket comment, which is
    # syntatically invalid.
    self._reflow_valid = False

    # Set to true if this node is the final node of a statement at
    # the current depth
    self.statement_terminal = False

    self._parent = None
    self._children = []
    self._passno = -1

    # The first column past the last column covered by this node. This is
    # different than just "position" + "size" in the case that the node
    # contains multiline string or bracket arguments
    self._colextent = 0
    self._rowextent = 0

    # Depth of this node in the subtree starting at the statement node.
    # If this node is not in a statement subtree the value is zero.
    self._stmt_depth = 0

    # Depth of the deepest descendant.
    self._subtree_depth = 0

    # The tree structure is locked and immutable. `stmt_depth` and
    # `subtree_depth` have been computed and stored.
    self._locked = False

    # Map subpass number to (passno, wrap) decisions
    self._layout_passes = [(0, False)]

    # Set to true if this node's wrap is activated. Note that _wrap may
    # refer to nesting or vertical layout, depending on what type of node
    # we are. This is value only really needs to be communicated between
    # `reflow()` and `_reflow()` but we store it so that we can render it
    # when viewing the tree for debugging.
    self._wrap = False

    assert isinstance(pnode, TreeNode)

  def _index_in_parent(self):
    for idx, child in enumerate(self._parent.children):
      if child is self:
        return idx
    return -1

  def next_sibling(self):
    if self._parent is None:
      return None
    next_idx = self._index_in_parent() + 1
    if next_idx >= len(self._parent.children):
      return None
    return self._parent.children[next_idx]

  @property
  def name(self):
    """
    The class name of the derived node type.
    """
    return self.__class__.__name__

  @property
  def passno(self):
    """
    The active pass-number which contributed the current layout of the
    subtree rooted at this node.
    """
    return self._passno

  @property
  def colextent(self):
    """
    The column index of the right-most character in the layout of the
    subtree rooted at this node. In other words, the width of the
    bounding box for the subtree rooted at this node.
    """
    return self._colextent

  @property
  def reflow_valid(self):
    """
    A boolean flag indicating whether or not the current layout is accepted.
    If False, then further layout passes are required.
    """
    return self._reflow_valid

  @property
  def position(self):
    """
    A cursor with the (row,col) of the first (i.e. top,left) character in the
    subtree rooted at this node.
    """
    return Cursor(*self._position)

  @property
  def node_type(self):
    """
    Return the `NodeType` of the corresponding parser node that generated
    this layout node.
    """
    return self.pnode.node_type

  # NOTE(josh): making children a property disallows direct assignment
  @property
  def children(self):
    """
    A list of children layout nodes
    """
    return self._children

  @property
  def rowextent(self):
    return self._rowextent

  def __repr__(self):
    boolmap = {True: "T", False: "F"}
    return "{}({}),(passno={},wrap={},ok={}) pos:({},{}) ext:({},{})".format(
        self.__class__.__name__, self.node_type.name,
        self._passno, boolmap[self._wrap], boolmap[self._reflow_valid],
        self.position[0], self.position[1],
        self.rowextent, self.colextent)

  def has_terminal_comment(self):
    """
    Return true if this node has a terminal line comment. In particular, this
    implies that no other node may be packed at the output cursor of this
    node's layout, and a line-wrap is required.
    """
    return False

  def get_depth(self):
    """
    Compute and return the depth of the subtree rooted at this node. The
    depth of the tree is the depth of the deepest (leaf) descendant.
    """

    if self._children:
      return 1 + max(child.get_depth() for child in self._children)
    return 1

  # Lock the tree structure and prevent further updates.
  # Compute `stmt_depth` and `subtree_depth`. Replace the children list with
  # a tuple.
  def lock(self, config, stmt_depth=0):
    """
    Lock the tree structure (topology) and prevent further updates. This is
    mostly for sanity checking. It also computes topological statistics such
    as `stmt_depth` and `subtree_depth`, and replaces the mutable list of
    children with an immuatable tuple.
    """
    self._stmt_depth = stmt_depth
    self._subtree_depth = self.get_depth()
    self._children = tuple(self._children)
    self._locked = True

    for child in self._children:
      # pylint: disable=protected-access
      child._parent = self

    if self.node_type == NodeType.STATEMENT:
      nextdepth = 1
    elif stmt_depth > 0:
      nextdepth = stmt_depth + 1
    else:
      nextdepth = 0

    for child in self._children:
      child.lock(config, nextdepth)

  def _reflow(self, stack_context, cursor, passno):
    """
    Overridden by concrete classes to implement the layout of characters.
    """
    raise NotImplementedError()

  def _validate_layout(
      self, stack_context, start_extent, end_extent):
    """
    Return true if the layout is acceptable according to several checks. For
    example, returns false if the content overflows the columnt limit.
    """

    config = stack_context.config

    # If the bounding box overflows the column limit then the layout is
    # automatically voided
    if end_extent[1] > config.format.linewidth:
      return False

    size = end_extent - start_extent

    if not self._wrap:
      # Regardless of nesting, if the content is wrapped horizontally but it
      # exceeds the configured maximum number of lines we must reject it
      # TODO(josh): figure out how to subtract out any terminal comment
      # contributions to the size, as noted in the algorithm doc.
      if size[0] > config.format.max_lines_hwrap:
        if not isinstance(self, (BodyNode, CommentNode, FlowControlNode)):
          return False

      # Or if this nodepath is marked to always be vertical layout
      pathstr = get_pathstr(stack_context.node_path)
      if pathstr in config.format.always_wrap:
        return False

    return True

  def reflow(self, stack_context, cursor, parent_passno=0):
    """
    (re-)compute the layout of this node under the assumption that it should
    be placed at the given `cursor` on the current `parent_passno`.
    """

    assert self._locked
    assert isinstance(self.pnode, TreeNode)

    self._position = cursor.clone()
    outcursor = None

    layout_passes = \
        stack_context.config.format.layout_passes.get(
            self.__class__.__name__, self._layout_passes)

    with stack_context.push_node(self):
      for passno, wrap in layout_passes:
        if passno > parent_passno:
          break
        self._passno = passno
        self._wrap = wrap
        self._reflow_valid = True
        start_extent = cursor.clone()
        outcursor = self._reflow(
            stack_context, cursor.clone(), passno)
        end_extent = Cursor(outcursor[0], self._colextent)
        self._reflow_valid &= self._validate_layout(
            stack_context, start_extent, end_extent)
        if self._reflow_valid:
          break
    assert outcursor is not None
    return outcursor

  def write(self, config, ctx):
    """
    Output text content given the currently configured layout.
    """
    for child in self._children:
      child.write(config, ctx)

  @staticmethod
  def create(pnode):
    """
    Create a new layout node associated with then given parser node.
    """
    # pylint: disable=too-many-return-statements
    if pnode.node_type in SCALAR_TYPES:
      return ScalarNode(pnode)
    if pnode.node_type == NodeType.ONOFFSWITCH:
      return OnOffSwitchNode(pnode)
    if pnode.node_type == NodeType.STATEMENT:
      return StatementNode(pnode)
    if pnode.node_type == NodeType.ATWORDSTATEMENT:
      return AtWordStatementNode(pnode)
    if pnode.node_type == NodeType.KWARGGROUP:
      return KwargGroupNode(pnode)
    if pnode.node_type == NodeType.ARGGROUP:
      return ArgGroupNode(pnode)
    if pnode.node_type in (NodeType.PARGGROUP,
                           NodeType.FLAGGROUP):
      return PargGroupNode(pnode)
    if pnode.node_type == NodeType.PARENGROUP:
      return ParenGroupNode(pnode)
    if pnode.node_type == NodeType.BODY:
      return BodyNode(pnode)
    if pnode.node_type == NodeType.FLOW_CONTROL:
      return FlowControlNode(pnode)
    if pnode.node_type == NodeType.COMMENT:
      return CommentNode(pnode)
    if pnode.node_type == NodeType.WHITESPACE:
      return WhitespaceNode(pnode)
    if pnode.node_type in PAREN_TYPES:
      return ParenNode(pnode)

    raise RuntimeError("Unexpected node type")


class ParenNode(LayoutNode):
  """Holds parenthesis '(' or ')' for statements or boolean groups."""

  @property
  def name(self):
    return self.node_type.name

  def _reflow(self, stack_context, cursor, passno):
    """There is only one possible layout for this node."""
    self._colextent = cursor[1] + 1
    return cursor + (0, 1)

  def write(self, config, ctx):
    if self.node_type == NodeType.LPAREN:
      ctx.outfile.write_at(self.position, '(')
    elif self.node_type == NodeType.RPAREN:
      ctx.outfile.write_at(self.position, ')')
    else:
      raise ValueError("Unrecognized paren type")


class ScalarNode(LayoutNode):
  """
  Holds scalar tokens such as statement names, parentheses, or keyword or
  positional arguments.
  """

  def has_terminal_comment(self):
    if not self.children:
      return False

    if not self.children[-1].pnode.children:
      return False

    # Note: bracket comments do not count as terminal comments
    return self.children[-1].pnode.children[0].type == TokenType.COMMENT

  def _reflow(self, stack_context, cursor, passno):
    """
    Reflow is pretty trivial for a scalar node. We don't have any choices to
    make, there is only one possible rendering.
    """
    assert self.pnode.children
    token = self.pnode.children[0]
    assert isinstance(token, lex.Token)

    # This might be a multiline string or a multiline bracket argument. In
    # that case we need to normalize line endings and flow each line
    lines = normalize_line_endings(token.spelling).split('\n')
    line = lines.pop(0)
    cursor[1] += len(line)
    self._colextent = cursor[1]

    while lines:
      cursor[0] += 1
      cursor[1] = 0
      line = lines.pop(0)
      cursor[1] += len(line)
      self._colextent = max(self._colextent, cursor[1])

    # Scalar nodes might have terminal comments associated with them. They show
    # up as children in the layout graph. This is the only possible child of
    # a scalar node.
    if self.children:
      # We should not have more than one terminal comment associated with a
      # given scalar node
      assert len(self.children) == 1

      # The only kind of children we store for a scalar node are argument
      # comments.
      child = self.children[0]
      assert child.node_type == NodeType.COMMENT

      # Reflow the comment after the scalar
      cursor = child.reflow(stack_context, cursor + (0, 1), passno)
      self._reflow_valid &= child.reflow_valid
      self._colextent = max(self._colextent, child.colextent)
    return cursor

  def write(self, config, ctx):
    if not ctx.is_active():
      return

    assert self.pnode.children
    token = self.pnode.children[0]
    assert isinstance(token, lex.Token)

    spelling = normalize_line_endings(token.spelling)
    if self.node_type == NodeType.FUNNAME:
      command_case = config.resolve_for_command(
          token.spelling, "format.command_case")
      if command_case in ("lower", "upper"):
        spelling = getattr(token.spelling, command_case)()
      elif command_case == "canonical":
        if (self._parent.pnode.cmdspec is not None
            and self._parent.pnode.cmdspec.spelling is not None):
          spelling = self._parent.pnode.cmdspec.spelling
        else:
          spelling = config.resolve_for_command(
              token.spelling, "spelling", token.spelling.lower())
      else:
        assert command_case == "unchanged", (
            "Unrecognized command case {}".format(command_case))
    elif (self.node_type in (NodeType.KEYWORD, NodeType.FLAG)
          and config.format.keyword_case in ("lower", "upper")):
      spelling = getattr(token.spelling, config.format.keyword_case)()

    ctx.outfile.write_at(self.position, spelling)
    children = list(self.children)
    if children:
      child = children.pop(0)
      assert child.node_type == NodeType.COMMENT
      child.write(config, ctx)

    assert not children


class OnOffSwitchNode(LayoutNode):
  """
  Holds a special-case line comment token such as ``# cmake-format: off`` or
  ``# cmake-format: on``
  """

  @property
  def name(self):
    return self.pnode.children[0].type.name

  def has_terminal_comment(self):
    return True

  def _reflow(self, stack_context, cursor, passno):
    """
    There is only one possible flow as this is a single-token
    """
    assert self.pnode.children
    token = self.pnode.children[0]
    assert isinstance(token, lex.Token)

    # Canonicalize the output. The input regex may be more generous than this
    # exact string.
    if token.type == TokenType.FORMAT_ON:
      spelling = "# cmake-format: on"
    elif token.type == TokenType.FORMAT_OFF:
      spelling = "# cmake-format: off"

    cursor = cursor + (0, len(spelling))
    self._colextent = cursor[1]
    return cursor

  def write(self, config, ctx):
    assert self.pnode.children
    token = self.pnode.children[0]
    assert isinstance(token, lex.Token)

    if token.type == TokenType.FORMAT_ON:
      spelling = "# cmake-format: on"
      if ctx.offswitch_location is None:
        logging.warning(
            "'#cmake-format: on' with no corresponding 'off' at %d:%d",
            token.begin.line, token.begin.col)
      else:
        ctx.infile.seek(ctx.offswitch_location.offset, 0)
        copy_size = token.begin.offset - ctx.offswitch_location.offset
        copy_bytes = ctx.infile.read(copy_size)
        copy_text = copy_bytes.decode('utf-8')
        ctx.outfile.write(copy_text)
        ctx.offswitch_location = None
        ctx.outfile.forge_cursor(self.position)
    elif token.type == TokenType.FORMAT_OFF:
      spelling = "# cmake-format: off"
      ctx.offswitch_location = token.end

    ctx.outfile.write_at(self.position, spelling)


class StatementNode(LayoutNode):
  """
  Top-level node for a statement.
  """

  def __init__(self, pnode):
    super(StatementNode, self).__init__(pnode)
    self._layout_passes = [
        (0, False),
        (1, True),
        (2, True),
        (3, True),
        (4, True),
        (5, True),
    ]

  def reflow(self, stack_context, cursor, _=0):  # pylint: disable=unused-argument
    return super(StatementNode, self).reflow(
        stack_context, cursor,
        max(passno for passno, _ in self._layout_passes))

  def get_prefix_width(self, config):
    prefix_width = len(self.name) + 1
    if need_paren_space(self.name, config):
      prefix_width += 1  # For the space before the paren
    return prefix_width

  @property
  def name(self):
    return self.children[0].pnode.children[0].spelling.lower()

  def _validate_layout(
      self, stack_context, start_extent, end_extent):
    config = stack_context.config

    # If the bounding box overflows the column limit then the layout is
    # automatically voided
    if end_extent[1] > config.format.linewidth:
      return False

    size = end_extent - start_extent
    if not self._wrap:
      # If the statement spelling is very long and there is enough content that
      # the content is forced to wrap, then we require the statement content to
      # nest.
      if (size[0] > 1 and
          self.get_prefix_width(config) > config.format.max_prefix_chars):
        return False
    return True

  def _reflow(self, stack_context, cursor, passno):
    # pylint: disable=too-many-statements
    config = stack_context.config
    start_cursor = cursor.clone()
    self._colextent = cursor[1]

    # Layout of the statement name is always the same, we just write it out
    # at the current cursor
    children = list(self.children)
    assert children
    child = children.pop(0)
    assert child.node_type == NodeType.FUNNAME

    cursor = child.reflow(stack_context, cursor, passno)
    self._reflow_valid &= child.reflow_valid
    self._colextent = max(self._colextent, child.colextent)

    funname = child
    token = funname.pnode.children[0]

    assert isinstance(token, lex.Token)
    if need_paren_space(token.spelling.lower(), config):
      cursor[1] += 1

    if self.get_prefix_width(config) <= config.format.min_prefix_chars:
      # If the statement or keyword spelling is too short, then nesting doesn't
      # make sense because (nest + tab-width) will take us right back to the
      # same column as without nesting.
      self._wrap = False

    assert children
    child = children.pop(0)
    assert child.node_type == NodeType.LPAREN
    cursor = child.reflow(stack_context, cursor, passno)
    self._reflow_valid &= child.reflow_valid
    self._colextent = max(self._colextent, child.colextent)

    if self._wrap:
      column_cursor = start_cursor + (1, config.format.tab_size)
      cursor = Cursor(*column_cursor)
    else:
      column_cursor = cursor.clone()

    # NOTE(josh): STATEMENTs should have at most one ARGGROUP child between
    # parentheses.
    child = children.pop(0)
    assert child.node_type is NodeType.ARGGROUP, (
        "Expected ARGGROUP node, but got {} at {}"
        .format(child.node_type, child.pnode))
    # Whether or not an ARGGROUP is statement terminal is kind of a weird
    # notion, but as a sentinel for the future "number of open parens" we'll
    # go ahead and mark it.
    child.statement_terminal = True
    cursor = child.reflow(stack_context, cursor, passno)
    self._reflow_valid &= child.reflow_valid
    # We must keep updating the extent after each child because
    # the child might be an argument with a multiline string or a bracket
    # argument... in which case HPACK might actually wrap to a newline.
    self._colextent = max(self._colextent, child.colextent)
    column_cursor[0] = cursor[0]

    assert children
    prev = child
    child = children.pop(0)
    assert child.node_type == NodeType.RPAREN, \
        "Expected RPAREN but got {}".format(child.node_type)

    # NOTE(josh): dangle parens if it wont fit on the current line or
    # if the user has requested us to always do so
    dangle_parens = False
    if config.format.dangle_parens and cursor[0] > start_cursor[0]:
      # If the configuration requests dangling parentheses, then honor that
      # request so long as the statement doesn't fit on a single line
      dangle_parens = True
    elif cursor[1] >= config.format.linewidth:
      # If the child reflow was unable to reserve a column for us to place our
      # parenthesis, then we must dangle it
      dangle_parens = True
      # But we really want to nest first in this case
      if not self._wrap:
        self._reflow_valid = False
    elif prev.has_terminal_comment():
      # If the final token in an argument list is a line comment, then we must
      # dangle the parenthesis, or it will become part of the line comment
      dangle_parens = True

    column_cursor[0] += 1
    if config.format.dangle_align == "prefix":
      dangle_cursor = Cursor(column_cursor[0], start_cursor[1])
    elif config.format.dangle_align == "prefix-indent":
      dangle_cursor = Cursor(
          column_cursor[0], start_cursor[1] + config.format.tab_size)
    elif config.format.dangle_align == "child":
      dangle_cursor = Cursor(*column_cursor)
    else:
      raise ValueError(
          "Unexpected config.format.dangle_align: {}"
          .format(config.format.dangle_align))

    if dangle_parens:
      cursor = dangle_cursor.clone()

    rparen = child
    initial_rparen_cursor = cursor.clone()
    cursor = rparen.reflow(stack_context, initial_rparen_cursor, passno)
    self._reflow_valid &= rparen.reflow_valid
    # NOTE(josh): don't max rparen.colextent here, since we may decide to
    # dangle it below

    # Trailing comment
    if children:
      cursor[1] += 1
      child = children.pop(0)
      assert child.node_type == NodeType.COMMENT, \
          "Expected COMMENT after RPAREN but got {}".format(child.node_type)
      assert not children
      savecursor = cursor.clone()
      cursor = child.reflow(stack_context, cursor, passno)

      # If the statement trailing comment does not fit in the column after the
      # rparen, then dangle the rparen and try again.
      if not dangle_parens and child.colextent > config.format.linewidth:
        cursor = rparen.reflow(stack_context, dangle_cursor, passno)
        self._reflow_valid &= rparen.reflow_valid
        # NOTE(josh): don't max rparen.colextent here, since we may reverse our
        # dangle decision in the next if block
        savecursor = cursor.clone()
        cursor = child.reflow(stack_context, cursor, passno)

      # If the statement trailing comment still does not fit in the current
      # column then just move it to the next line.
      if child.colextent > config.format.linewidth:
        # NOTE(josh): potentially undangle the paren: if the only reason to
        # dangle it was due to the oversized comment line, then we need to
        # undangle it since the oversized comment didn't fit anyway.
        cursor = rparen.reflow(stack_context, initial_rparen_cursor, passno)
        cursor = child.reflow(
            stack_context, Cursor(savecursor[0] + 1, start_cursor[1]),
            passno)

      self._reflow_valid &= child.reflow_valid
      self._colextent = max(self._colextent, child.colextent)

    self._colextent = max(self._colextent, rparen.colextent)
    return cursor

  def write(self, config, ctx):
    if not ctx.is_active():
      return
    super(StatementNode, self).write(config, ctx)


class AtWordStatementNode(LayoutNode):
  """
  Top-level node for an atword representing a statement. Such as
  @PACKAGE_INIT@
  """

  def __init__(self, pnode):
    super(AtWordStatementNode, self).__init__(pnode)
    self._layout_passes = [
        (0, False),
    ]

  def reflow(self, stack_context, cursor, _=0):  # pylint: disable=unused-argument
    return super(AtWordStatementNode, self).reflow(
        stack_context, cursor,
        max(passno for passno, _ in self._layout_passes))

  def _reflow(self, stack_context, cursor, passno):
    # pylint: disable=too-many-statements
    config = stack_context.config
    start_cursor = cursor.clone()
    self._colextent = cursor[1]

    # Layout of the statement name is always the same, we just write it out
    # at the current cursor
    children = list(self.children)
    assert children
    child = children.pop(0)
    assert child.node_type == NodeType.ATWORD

    cursor = child.reflow(stack_context, cursor, passno)
    self._reflow_valid &= child.reflow_valid
    self._colextent = max(self._colextent, child.colextent)

    # Trailing comment
    if children:
      cursor[1] += 1
      child = children.pop(0)
      assert child.node_type == NodeType.COMMENT, \
          "Expected COMMENT after RPAREN but got {}".format(child.node_type)
      assert not children
      savecursor = cursor.clone()
      cursor = child.reflow(stack_context, cursor, passno)

      # If the statement trailing comment still does not fit in the current
      # column then just move it to the next line.
      if child.colextent > config.format.linewidth:
        # NOTE(josh): potentially undangle the paren: if the only reason to
        # dangle it was due to the oversized comment line, then we need to
        # undangle it since the oversized comment didn't fit anyway.
        cursor = child.reflow(
            stack_context, Cursor(savecursor[0] + 1, start_cursor[1]),
            passno)

      self._reflow_valid &= child.reflow_valid
      self._colextent = max(self._colextent, child.colextent)

    return cursor

  def write(self, config, ctx):
    if not ctx.is_active():
      return
    super(AtWordStatementNode, self).write(config, ctx)


class KwargGroupNode(LayoutNode):
  """
  A keyword argument group. Contains a keyword, followed by an argument group.
  """

  def __init__(self, pnode):
    super(KwargGroupNode, self).__init__(pnode)
    self._layout_passes = [
        (0, False),
        (1, False),
        (2, False),
        (3, False),
        (4, False),
        (5, True),
    ]

  def has_terminal_comment(self):
    if self.children[-1].node_type is NodeType.COMMENT:
      return True
    return self.children[-1].has_terminal_comment()

  @property
  def name(self):
    return self.children[0].pnode.children[0].spelling.upper()

  def _validate_layout(
      self, stack_context, start_extent, end_extent):
    config = stack_context.config

    # If the bounding box overflows the column limit then the layout is
    # automatically voided
    if end_extent[1] > config.format.linewidth:
      return False

    size = end_extent - start_extent
    if not self._wrap:
      # If the statement spelling is very long and there is enough content that
      # the content is forced to wrap, then we require the statement content to
      # nest.
      if size[0] > 1 and len(self.name) > config.format.max_prefix_chars:
        return False
    return True

  def _reflow(self, stack_context, cursor, passno):
    config = stack_context.config
    start_cursor = cursor.clone()
    self._colextent = cursor[1]

    # Keyword node should have exactly two children, the keyword and the
    # argument group.
    assert len(self.children) <= 2, (
        "Expected at most 2 children in KWargGroup, got {}"
        .format(len(self.children))
    )

    # Layout of the keyword is always the same, we just write it out
    # at the current cursor
    child = self.children[0]
    assert child.node_type == NodeType.KEYWORD

    if len(self.name) <= config.format.min_prefix_chars:
      # If the statement or keyword spelling is too short, then nesting doesn't
      # make sense because (nest + tab-width) will take us right back to the
      # same column as without nesting.
      self._wrap = False

    cursor = child.reflow(stack_context, cursor, passno)
    self._reflow_valid &= child.reflow_valid
    self._colextent = child.colextent

    # NOTE(josh): Empty kwarg. These are rare, and I think they might actually
    # be syntax errors, but we currently support it so let's not limit the
    # possibility for now
    if len(self.children) == 1:
      return cursor

    # keyword = get_normalized_kwarg(child.pnode.children[0])
    if self._wrap:
      column_cursor = Cursor(
          cursor[0] + 1, start_cursor[1] + config.format.tab_size)
    else:
      column_cursor = cursor + (0, 1)

    child = self.children[1]
    cursor = Cursor(*column_cursor)

    if self.statement_terminal:
      child.statement_terminal = True

    cursor = child.reflow(stack_context, cursor, passno)
    self._reflow_valid &= child.reflow_valid
    self._colextent = max(self._colextent, child.colextent)
    column_cursor[0] = cursor[0]
    return cursor


def filename_node_key(layout_node):
  """
  Return the sort key for sortable arguments nodes. This is the
  case-insensitive spelling of the first token in the node.
  """
  return layout_node.pnode.children[0].spelling.lower()


def sort_arguments(children):
  argument_nodes = []
  for child in children:
    if child.pnode.node_type is NodeType.ARGUMENT:
      argument_nodes.append(child)
  argument_nodes = sorted(argument_nodes, key=filename_node_key)
  nodemap = {
      id(node): idx + 1 for idx, node in enumerate(argument_nodes)
  }
  sortlist = []

  keyidx = 0
  posidx = 0
  for child in children:
    if id(child) in nodemap:
      keyidx = nodemap[id(child)]
      posidx = 0

    sortlist.append((keyidx, posidx, child))
    posidx += 1
  return [child for _, _, child in sorted(sortlist)]


def count_arguments(children):
  """
  Count the number of positional arguments (excluding line comments and
  whitespace) within a parg group.
  """
  count = 0
  for child in children:
    if child.node_type is NodeType.COMMENT:
      continue

    count += 1
  return count


class PargGroupNode(LayoutNode):
  """
  A group of positional arguments.
  """

  def __init__(self, pnode):
    super(PargGroupNode, self).__init__(pnode)
    self._max_pargs_hwrap = None
    self._layout_passes = [
        (0, False),
        (1, False),
        (2, False),
        (3, False),
        (4, True),
    ]

  def has_terminal_comment(self):
    if not self.children:
      return False
    if self.children[-1].node_type is NodeType.COMMENT:
      return True
    return self.children[-1].has_terminal_comment()

  def lock(self, config, stmt_depth=0):
    if (config.format.autosort and
        isinstance(self.pnode, PositionalGroupNode) and
        self.pnode.sortable):
      self._children = sort_arguments(self._children)

    super(PargGroupNode, self).lock(config, stmt_depth)

    self._max_pargs_hwrap = config.format.max_pargs_hwrap
    if (isinstance(self.pnode, PositionalGroupNode)
        and self.pnode.spec is not None
        and self.pnode.spec.max_pargs_hwrap is not None):
      self._max_pargs_hwrap = self.pnode.spec.max_pargs_hwrap

  def _reflow(self, stack_context, cursor, passno):
    config = stack_context.config
    children = list(self.children)
    self._colextent = cursor.y

    prev = None
    child = None
    column_cursor = cursor.clone()
    numpargs = count_arguments(children)

    # "COMMAND" arguments are never columnized, since there is no way for us
    # to make that look good
    is_cmdline = "cmdline" in self.pnode.tags
    if is_cmdline:
      self._wrap = False

    rowcount = 0
    while children:
      prev = child
      child = children.pop(0)
      cursor_is_at_column = True

      if prev is None:
        # This is the first child of the arg group so the cursor is already
        # at the right location and theres nothing for us to update
        rowcount = 1
      elif (is_line_comment(prev)
            or prev.has_terminal_comment()
            or self._wrap):
        column_cursor[0] = cursor[0] + 1
        cursor = Cursor(*column_cursor)
        rowcount += 1
      else:
        cursor_is_at_column = False
        cursor[1] += 1

      if self.statement_terminal and not children:
        child.statement_terminal = True

      input_cursor = cursor
      cursor = child.reflow(stack_context, input_cursor, passno)

      if (not cursor_is_at_column) and (not self._wrap):
        # If we are in horizontal wrapping mode, then we need to look at a
        # couple of things thaty may cause us to wrap.
        needs_wrap = False

        # The obvious case is overflow:
        # If the realized extent overflows the column limit then we need to
        # insert a newline and try again
        if child.colextent > config.format.linewidth:
          needs_wrap = True

        # If this is the last node before the closing parenthesis of the
        # statement then we need to extra character (the closing parenthesis)
        # on the last line. If we overlow that extra character slot, then
        # wrap to a new line
        if self.statement_terminal:
          if cursor[1] + 1 > config.format.linewidth:
            needs_wrap = True

        # If the current node is a comment, then we must have parsed it as a
        # line comment (not an argument comment) and so we should preserve
        # it's status as a line comment and not put it after another argument.
        # Note that tag comments are always line comments, and never consumed
        # as argument comments.
        if ((prev and prev.node_type in SCALAR_TYPES) and
            child.node_type is NodeType.COMMENT and
            not child.is_tag()):
          needs_wrap = True

        if needs_wrap:
          rowcount += 1
          column_cursor[0] = input_cursor[0] + 1
          cursor = child.reflow(stack_context, column_cursor, passno)

      # NOTE(josh): we must keep updating the extent after each child because
      # the child might be an argument with a multiline string or a bracket
      # argument... in which case HPACK might actually wrap to a newline.
      self._reflow_valid &= child.reflow_valid
      self._colextent = max(self._colextent, child.colextent)

    self._rowextent = rowcount
    # NOTE(josh): there is a subtle distinction between invalidating a reflow
    # and forcing mode=vertical. The difference is whether or not a parent
    # node has to advance it's decision state. If we force to vertical at
    # the start of this function, the parent Statement wont nest this
    # ArgGroup. Therefore, we must invalidate here, rather than forcing
    # _vertical above.
    if is_cmdline:
      self._reflow_valid &= (rowcount <= config.format.max_rows_cmdline)
    elif numpargs > self._max_pargs_hwrap:
      self._reflow_valid &= self._wrap

    return cursor


def count_subgroups(children):
  """
  Count the number of positional or kwarg sub groups in an argument group.
  Ignore comments, and assert that no other types of children are found.
  """
  numgroups = 0
  for child in children:
    if child.node_type in (NodeType.KWARGGROUP, NodeType.PARGGROUP,
                           NodeType.PARENGROUP):
      numgroups += 1
    elif child.node_type in (NodeType.COMMENT, NodeType.ONOFFSWITCH):
      continue
    else:
      raise ValueError(
          "Unexpected node type {} as child of ArgGroupNode"
          .format(child.node_type))
  return numgroups


class ArgGroupNode(LayoutNode):
  """
  A group of arguments. This is the single child node of either a
  `StatementNode` or `KwargGroupNode` which then contains any further
  group nodes.
  """

  def __init__(self, pnode):
    super(ArgGroupNode, self).__init__(pnode)
    self._max_subgroups_hwrap = None
    self._layout_passes = [
        (0, False),
        (1, False),
        (2, False),
        (3, False),
        (4, True),
        (5, True),
    ]

  def lock(self, config, stmt_depth=0):
    super(ArgGroupNode, self).lock(config, stmt_depth)
    self._max_subgroups_hwrap = config.format.max_subgroups_hwrap
    if (hasattr(self.pnode, "cmdspec")
        and getattr(self.pnode, "cmdspec") is not None
        and getattr(self.pnode, "cmdspec").max_subgroups_hwrap is not None):
      self._max_subgroups_hwrap = (
          getattr(self.pnode, "cmdspec").max_subgroups_hwrap)

  def has_terminal_comment(self):
    """
    An ArgGroup is a container for one or more PARGGROUP, FLAGGROUP, or
    KWARGGROUP subtrees. Any terminal comment will belong to one of
    it's children.
    """
    return self.children and (self.children[-1].node_type is NodeType.COMMENT or
                              self.children[-1].has_terminal_comment())

  def _reflow(self, stack_context, cursor, passno):
    config = stack_context.config
    children = list(self.children)
    self._colextent = cursor.y

    prev = None
    child = None

    column_cursor = cursor.clone()
    numgroups = count_subgroups(children)

    while children:
      prev = child
      child = children.pop(0)

      if prev is None:
        # This is the first child of the arg group so the cursor is already
        # at the right location and theres nothing for us to update
        is_first_in_row = True
      elif (is_line_comment(prev)
            or prev.has_terminal_comment()
            or self._wrap):
        column_cursor[0] += 1
        cursor = column_cursor.clone()
        is_first_in_row = True
      else:
        cursor[1] += 1
        is_first_in_row = False

      if self.statement_terminal and not children:
        child.statement_terminal = True

      start_cursor = cursor
      cursor = child.reflow(stack_context, cursor, passno)
      if not is_first_in_row and not self._wrap:
        # If we are in horizontal wrapping mode, then we need to check if the
        # child has overflowed the available column width. If so, then we need
        # to wrap to the next line and try again
        needs_wrap = False
        if child.statement_terminal:
          # If this is the last node before the parenthesis then we need to
          # account for one extra character (the closing parenthesis)
          if cursor[1] + 1 > config.format.linewidth:
            needs_wrap = True

        if child.colextent > config.format.linewidth:
          # If the realized extent overflows the column limit then we need to
          # insert a newline and try again
          needs_wrap = True

        if (cursor - start_cursor)[0] > 1:
          # If the argument is wrapped internally (i.e. has more than two lines)
          # then move it to it's own line
          # TODO(josh): Instead of needs_wrap = True unconditionally in this
          # case, let's layout both options and pick which ever one is best
          needs_wrap = True

        if needs_wrap:
          column_cursor[0] += 1
          cursor = Cursor(*column_cursor)
          cursor = child.reflow(stack_context, cursor, passno)

      # NOTE(josh): we must keep updating the extent after each child because
      # the child might be an argument with a multiline string or a bracket
      # argument... in which case HPACK might actually wrap to a newline.
      self._reflow_valid &= child.reflow_valid
      self._colextent = max(self._colextent, child.colextent)
      column_cursor[0] = cursor[0]

    # NOTE(josh): there is a subtle distinction between invalidating a reflow
    # and forcing mode=vertical. The difference is whether or not a parent
    # node has to advance it's decision state. If we force to vertical at
    # the start of this function, the parent Statement wont nest this
    # ArgGroup. Therefore, we must invalidate here, rather than forcing
    # _vertical above.
    if numgroups > self._max_subgroups_hwrap:
      self._reflow_valid &= self._wrap

    return cursor

  def write(self, config, ctx):
    if not ctx.is_active():
      return
    super(ArgGroupNode, self).write(config, ctx)


class ParenGroupNode(LayoutNode):
  """
  A parenthetical group. According to cmake syntax rules, this necessarily
  implies a boolean logical expression.
  """

  def __init__(self, pnode):
    super(ParenGroupNode, self).__init__(pnode)
    self._layout_passes = [
        (0, False),
        (1, False),
        (2, False),
        (3, False),
        (4, False),
        (5, True),
    ]

  def has_terminal_comment(self):
    children = list(self.children)
    while children and children[0].node_type != NodeType.RPAREN:
      children.pop(0)

    if children:
      children.pop(0)

    return (children
            and children[-1].pnode.children[0].type == TokenType.COMMENT)

  def _reflow(self, stack_context, cursor, passno):
    config = stack_context.config
    children = list(self.children)
    self._colextent = cursor.y

    # ParenGroupNode is composed of:
    #  * a left parenthensis
    #  * an argument group
    #  * a right parenthesis
    #  * an optional trailing comment
    assert len(children) in (3, 4)

    prev = None
    child = None
    # Paren groups do not themselves nest or wrap, they only have one real
    # child: the primary argument group.
    column_cursor = cursor.clone()

    while children:
      prev = child
      child = children.pop(0)

      if prev is None:
        # This is the first child of the arg group so the cursor is already
        # at the right location and theres nothing for us to update
        pass
      elif prev.node_type == NodeType.LPAREN:
        # No space after LParen
        pass
      elif (is_line_comment(prev)
            or prev.has_terminal_comment()
            or self._wrap):
        cursor[1] = column_cursor[1]
        cursor[0] += 1
      elif child.node_type == NodeType.RPAREN:
        # No space before RPAREN
        pass
      else:
        cursor[1] += 1

      if self.statement_terminal and not children:
        child.statement_terminal = True

      cursor = child.reflow(stack_context, cursor, passno)
      if not self._wrap:
        # If we are in horizontal wrapping mode, then we need to check if the
        # child has overflowed the available columnt width. If so, then we need
        # to wrap to the next line and try again

        needs_wrap = False
        if child.statement_terminal:
          # If this is the last node before the parenthesis then we need to
          # account for one extra character (the closing parenthesis)
          if cursor[1] + 1 > config.format.linewidth:
            needs_wrap = True

        if child.colextent > config.format.linewidth:
          # If the realized extent overflows the column limit then we need to
          # insert a newline and try again
          needs_wrap = True

        if not child.reflow_valid:
          needs_wrap = True

        if needs_wrap:
          column_cursor[0] += 1
          cursor = Cursor(*column_cursor)
          cursor = child.reflow(stack_context, cursor, passno)

      self._reflow_valid &= child.reflow_valid
      self._colextent = max(self._colextent, child.colextent)
      column_cursor[0] = cursor[0]
    return cursor

  def write(self, config, ctx):
    if not ctx.is_active():
      return
    super(ParenGroupNode, self).write(config, ctx)


class BodyNode(LayoutNode):
  """
  Top-level node for a given "scope" depth. This node is the root of a document,
  or the root of any nested statement scopes.
  """

  def _reflow(self, stack_context, cursor, passno):
    """
    Compute the size of a body block
    """
    column_cursor = cursor.clone()
    self._colextent = 0
    for child in self.children:
      cursor = child.reflow(stack_context, column_cursor, passno)
      self._reflow_valid &= child.reflow_valid
      self._colextent = max(self._colextent, child.colextent)
      column_cursor[0] = cursor[0] + 1

    return cursor


class FlowControlNode(LayoutNode):
  """
  Top-Level node composed of a flow-control statement and it's associated
  `BodyNodes`.
  """

  def _reflow(self, stack_context, cursor, passno):
    """
    Compute the size of a flowcontrol block
    """
    config = stack_context.config
    self._colextent = 0
    column_cursor = cursor.clone()

    children = list(self.children)
    assert children
    child = children.pop(0)
    assert child.node_type == NodeType.STATEMENT
    cursor = child.reflow(stack_context, column_cursor, passno)
    self._reflow_valid &= child.reflow_valid
    self._colextent = max(self._colextent, child.colextent)
    column_cursor[0] = cursor[0] + 1

    assert children
    child = children.pop(0)
    assert child.node_type == NodeType.BODY
    cursor = child.reflow(
        stack_context, column_cursor + (0, config.format.tab_size), passno)
    self._reflow_valid &= child.reflow_valid
    self._colextent = max(self._colextent, child.colextent)
    column_cursor[0] = cursor[0] + 1

    while True:
      assert children
      child = children.pop(0)
      assert child.node_type == NodeType.STATEMENT
      cursor = child.reflow(stack_context, column_cursor, passno)
      self._reflow_valid &= child.reflow_valid
      self._colextent = max(self._colextent, child.colextent)
      column_cursor[0] = cursor[0] + 1

      if not children:
        break
      child = children.pop(0)
      assert child.node_type == NodeType.BODY
      cursor = child.reflow(stack_context, column_cursor +
                            (0, config.format.tab_size), passno)
      self._reflow_valid &= child.reflow_valid
      self._colextent = max(self._colextent, child.colextent)
      column_cursor[0] = cursor[0] + 1

    return cursor


class CommentNode(LayoutNode):
  """
  A line comment or bracket comment. If parented by a group node then this
  comment acts as an argument. If parented by a scalar node, then this comment
  acts like an argument comment.
  """

  def __init__(self, pnode):
    super(CommentNode, self).__init__(pnode)
    self._lines = []

  def is_tag(self):
    return comment_is_tag(self.pnode.children[0])

  def _reflow(self, stack_context, cursor, passno):
    """
    Compute the size of a comment block
    """
    config = stack_context.config
    if (len(self.pnode.children) == 1
        and isinstance(self.pnode.children[0], lex.Token)
        and self.pnode.children[0].type == TokenType.BRACKET_COMMENT):
      # TODO(josh): cache content and lines and re-use in _write
      content = normalize_line_endings(self.pnode.children[0].spelling)

      # TODO(josh): dedup with ScalarNode logic
      lines = content.split('\n')
      line = lines.pop(0)
      cursor[1] += len(line)
      self._colextent = cursor[1]

      while lines:
        cursor[0] += 1
        cursor[1] = 0
        line = lines.pop(0)
        cursor[1] += len(line)
        self._colextent = max(self._colextent, cursor[1])
      return cursor

    allocation = config.format.linewidth - cursor[1]
    with stack_context.push_node(self):
      self._lines = lines = list(
          format_comment_lines(self.pnode, stack_context, allocation))
    self._colextent = cursor[1] + max(len(line) for line in lines)

    increment = (len(lines) - 1, len(lines[-1]))
    return cursor + increment

  def write(self, config, ctx):
    if not ctx.is_active():
      return

    if (len(self.pnode.children) == 1
        and isinstance(self.pnode.children[0], lex.Token)
        and self.pnode.children[0].type == TokenType.BRACKET_COMMENT):
      content = normalize_line_endings(self.pnode.children[0].spelling)
      ctx.outfile.write_at(self.position, content)
    else:
      for idx, line in enumerate(self._lines):
        ctx.outfile.write_at(self.position + (idx, 0), line)


class WhitespaceNode(LayoutNode):
  """
  A series of newlines
  """

  def _reflow(self, stack_context, cursor, passno):
    """
    Compute the size of a whitespace block
    """
    return cursor.clone()

  def write(self, config, ctx):
    return


def get_scalar_sequence_len(box_children):
  length = 0
  for child in box_children:
    # Child is not a scalar type
    if child.node_type not in SCALAR_TYPES:
      return length
    length += 1
  return length


def create_box_tree(pnode):
  """
  Recursively construct a layout tree from the given parse tree
  """

  layout_root = LayoutNode.create(pnode)
  child_queue = list(pnode.children)

  while child_queue:
    pchild = child_queue.pop(0)
    if not isinstance(pchild, TreeNode):
      continue

    if (pchild.node_type == NodeType.WHITESPACE
        and pchild.count_newlines() < 2):
      continue

    if pchild.node_type in MATCH_TYPES:
      layout_root.children.append(create_box_tree(pchild))
  return layout_root


def layout_tree(parsetree_root, config, linewidth=None, first_token=None):
  """
  Top-level function to construct a layout tree from a parse tree, and then
  iterate through layout passes until the entire tree is satisfactory. Returns
  the root of the layout tree.
  """

  if linewidth is None:
    linewidth = config.format.linewidth

  root_box = create_box_tree(parsetree_root)
  root_box.lock(config)
  stack_context = StackContext(config, first_token)
  root_box.reflow(stack_context, Cursor(0, 0))

  return root_box


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

    if sys.version_info[0] < 3:
      outfile.write(repr(node).decode('utf-8'))
    else:
      outfile.write(repr(node))
    outfile.write('\n')

    if not hasattr(node, 'children'):
      continue

    if idx + 1 == len(nodes):
      dump_tree(node.children, outfile, indent + '    ')
    else:
      dump_tree(node.children, outfile, indent + '│   ')


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


def dump_tree_for_test(nodes, outfile=None, indent=None, increment=None):
  """
  Print a tree of node objects for debugging purposes
  """

  if indent is None:
    indent = ''

  if increment is None:
    increment = '  '

  if outfile is None:
    outfile = sys.stdout

  for node in nodes:
    outfile.write(indent)
    outfile.write("({}, {}, {}, {}, {}, [".format(
        node.node_type, node.passno,
        node.position[0], node.position[1], node.colextent))

    if hasattr(node, 'children') and node.children:
      outfile.write('\n')
      dump_tree_for_test(node.children, outfile, indent + increment, increment)
      outfile.write(indent)
    outfile.write("]),\n")


def test_string(nodes, indent=None, increment=None):
  if indent is None:
    indent = ' ' * 0
  if increment is None:
    increment = ' ' * 2

  outfile = io.StringIO()
  dump_tree_for_test(nodes, outfile, indent, increment)
  return outfile.getvalue()


class CursorFile(object):
  def __init__(self, config):
    self._fobj = io.StringIO()
    self._cursor = Cursor(0, 0)
    self._config = config

  @property
  def cursor(self):
    return Cursor(*self._cursor)

  def assert_at(self, cursor):
    assert (self._cursor[0] == cursor[0]
            and self._cursor[1] == cursor[1]), \
        "self._cursor=({},{}), write_at=({}, {}):\n{}".format(
            self._cursor[0], self._cursor[1], cursor[0], cursor[1],
            self.getvalue())

  def assert_lt(self, cursor):
    assert ((self._cursor[0] < cursor[0]) or (
        self._cursor[0] == cursor[0] and self._cursor[1] <= cursor[1])), \
        "self._cursor=({},{}), write_at=({}, {}):\n{}".format(
            self._cursor[0], self._cursor[1], cursor[0], cursor[1],
            self.getvalue().encode('utf-8', errors='replace'))

  def forge_cursor(self, cursor):
    self._cursor = cursor

  def write_at(self, cursor, text):
    if sys.version_info[0] < 3 and isinstance(text, str):
      text = text.decode('utf-8')
    self.assert_lt(cursor)

    rows = (cursor[0] - self._cursor[0])
    if rows:
      self._fobj.write(self._config.format.endl * rows)
      self._cursor[0] += rows
      self._cursor[1] = 0

    cols = (cursor[1] - self._cursor[1])
    if cols:
      self._fobj.write(' ' * cols)
      self._cursor[1] += cols

    lines = text.split('\n')
    line = lines.pop(0)
    self._fobj.write(line)
    self._cursor[1] += len(line)

    while lines:
      self._fobj.write(self._config.format.endl)
      self._cursor[0] += 1
      self._cursor[1] = 0
      line = lines.pop(0)
      self._fobj.write(line)
      self._cursor[1] += len(line)

  def write(self, copy_text):
    if sys.version_info[0] < 3 and isinstance(copy_text, str):
      copy_text = copy_text.decode('utf-8')
    self._fobj.write(copy_text)

    if '\n' not in copy_text:
      self._cursor[1] += len(copy_text)
    else:
      self._cursor[0] += copy_text.count('\n')
      self._cursor[1] = len(copy_text.split('\n')[-1])

  def getvalue(self):
    return self._fobj.getvalue() + self._config.format.endl


class WriteContext(object):
  """
  Global state for the writing functions
  """

  def __init__(self, config, infile_content):
    # The offset within the sourcefile of the end of the offswitch token

    self.offswitch_location = None
    if sys.version_info[0] < 3:
      assert isinstance(infile_content, unicode)

    # NOTE(josh): must be BytesIO because we use byte-offsets for raw
    # transcription (i.e. between cmake-format: off and cmake-format: on)
    self.infile = io.BytesIO(bytearray(infile_content, 'utf-8'))
    self.outfile = CursorFile(config)

  def is_active(self):
    return self.offswitch_location is None


def write_tree(root_box, config, infile_content):
  """
  Format the tree for size only, then print all of the boxes to outfile
  """
  ctx = WriteContext(config, infile_content)
  root_box.write(config, ctx)

  if not ctx.is_active():
    logging.warning("'# cmake-format: off' is never turned back 'on'"
                    "at %d:%d", ctx.offswitch_location.line,
                    ctx.offswitch_location.col)
  return ctx.outfile.getvalue()
