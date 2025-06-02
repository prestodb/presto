"""
Database of lint

Categories mirror pylint:

* (C) convention, for programming standard violation
* (R) refactor, for bad code smell
* (W) warning, for cmake specific problems
* (E) error, for most probably bugs in the code
* (F) fatal, if an error occurred which prevented cmake-lint from doing further
      processing.
"""
from __future__ import unicode_literals


class Lint(object):
  """Basically a named-tuple to store a lint id, message format, and metadata
  """

  def __init__(self, idstr, msgfmt, description=None, explain=None):
    self.idstr = idstr
    self.msgfmt = msgfmt
    self.description = description
    self.explain = explain


def get_database():
  return {idstr: Lint(idstr, msgfmt, **kwargs)
          for idstr, msgfmt, kwargs in LINT_DB}


LINT_DB = [
# pylint:disable=bad-continuation
# noqa: E122
(
"C0102", "Black listed name \"{:s}\"", {
"description": """\
Used when the name is listed in the "bad-names" black list.

This message belongs to the basic checker.
""",
"explain": """\
cmake-lint can be customized to help enforce coding guidelines that discourage
or forbid use of certain names for variables, functions, etc.. These names are
specified with the bad-names option. This message is raised whenever a name is
in the list of names defined with the bad-names option.
"""
}), (
"C0103", "Invalid {:s} name \"{:s}\" doesn't match `{:s}`", {
"description": """\
Used when a name doesn't doesn't fit the naming convention associated to its
type (function, macro, variable, ...).

This message belongs to the basic checker.
""",
"explain": """\
The naming convention is defined with a regular expression, and the naming
convention is satisfied if the name matches the regular expression.
"""
}), (
"C0111", "Missing docstring on function or macro declaration", {
"description": """\
Used when a function or macro is defined without a documentation comment
immediately preceeding it.

This message belongs to the basic checker.
""",
"explain": """\
So, you've written a some fancy function that makes it "easier" to declare build
steps. Congratulations. You probably shouldn't have, but thats OK. Now that you
did, how should people use it? What arguments does it take? What are the
semantics of those arguements? You should include documentation in a comment
block prior to the function declaration with this information.
"""
}), (
"C0112", "Empty docstring on function or macro declaration", {
"description": """\
Used when a function or macro is preceeded by an empty comment string, rather
that one with useful documentation.

This message belongs to the basic checker.
""",
"explain": """\
Ok so you saw C0111 and figured you'd be clever right? Sorry, no dice. Please
include some useful documentation so that code readers know what your
function/macro does and how to use it.
"""
}), (
"C0113", "Missing {:s} in statement which allows it", {
}), (
"C0114", "Form descriminator hidden behind variable dereference", {
"description": """\
Used when a keyword used to descriminate betwen different forms of a command is
hidden behind a variable dereference.

This message is implemented by individual command checkers.
""",
"explain": """\
Some cmake commands have very different behavior depending on the presence of
a particular keyword (see e.g. the `file` command). And because cmake is a
macro languge that keyword can actually be held inside a variable. Thus the
keyword might not actually be visible to cmake-lint (or humans). In general
there is no reason to do this and it really hurts readability since different
descriminator keywords yield essentially different commands.
"""
}), (
"C0201", "Consider replacing custom parser logic with cmake_parse_arguments", {
"description": """\
Used when custom parse logic is detected.
"""
}), (
"C0202", "Argument name {:s} differs from existing argument only in case", {
}), (
"C0301", "Line too long ({:d}/{:d})", {
"description": """\
Used when a line is longer than the limit specified in the line-length
option.
""",
"explain": """\
It is a good idea to keep each line within a maximum length to keep it from
wrapping past the edge of an editing window. This improves readability and
tempers other developers' irritability!

The default value of the line-length option is 80, the customary width of a
terminal window.

Note that the line length and the limit are counted in characters, not in Bytes
needed to represent these characters.
"""
}), (
"C0303", "Trailing whitespace", {
"description": """\
Used when a line has one or more whitespace characters directly before the line
end character(s).

This message belongs to the basic checker.
""",
"explain": """\
Such trailing whitespace is visually indistinguishable and some editors will
trim them.
"""
}), (
"C0304", "Final newline missing", {
"description": """\
Used when a listfile has no line end character(s) on its last line.

This message belongs to the basic checker.
""",
"explain": """\
While cmake itself does not require line end character(s) on the last line,
is simply good practice to have it.
"""
}), (
"C0305", "{:s} newlines between statements", {
}), (
"C0306", "Tab-policy violation. Found {:s} but should be {:s}", {
}), (
"C0307", "Bad indentation:\n{:s}{:s}\n{:s}^----{}\n", {
}), (
"C0321", "Multiple statements on a single line", {
}), (
"C0327", "Wrong line ending ({:s})", {
"description": """\
Used when a line ends with the wrong line ending character. e.g. A line ends
with "\\r\\n" when configured for "\\n".

This message belongs to the basic checker.
""",
"explain": """\
While cmake itself does not enforce a particular line ending, it is good
practice for a project to be consist with their line endings.
"""
}), (
"E0011", "Unrecognized file option {:s}", {
"description": """\
Used when an unrecognized pragma is encountered.
""",
"explain": """\
cmake-lint allows for some inline comments to supress warnings (among other
things). This lint is emitted if a bad option key is provided in such a pragma
"""
}), (
"E0012", "Bad option value {:s}", {
"description": """\
Used when a cmake-lint pragma is encountered which attempts to alter some option
in an invalid way.

This message belongs to the basic checker.
""",
"explain": """\
cmake-lint allows for some inline comments to supress warnings (among other
things). This lint is emitted if a bad option is provided to one of these
pragmas.
"""
}), (
"E0103", "{:s} outside of loop", {
"description": """\
Used when a break() or continue() statement is used outside a loop.

This message belongs to the basic checker.
""",
}), (
"E0108", "Duplicate argument name {:s} in function/macro definition", {
}), (
"E0109", "Invalid argument name {:s} in function/macro definition", {
}), (
"E1120", "Missing required positional argument", {
"description": """\
Used when a positional argument group expecting an exact number of arguments
is closed (by a parenthesis) before that number of arguments is found.

This message belongs to the basic checker.
""",
}), (
"E1121", "Too many positional arguments", {
"description": """\
Used when a positional argument is found when no argument group is expected.

This message is implemented by individual command checkers
""",
}), (
"E1122", "Duplicate keyword argument {:s}", {
"description": """
Used when a keyword shows up more than once within an argument group. In
general, only COMMAND is allowed more than once.
"""
}), (
"E1125", "Missing required keyword argument {:s}", {
}), (
"E1126", "Invalid form descriminator", {
"description": """
Used when a keyword used to descriminate between different command forms is
ommitted.
"""
}), (
"R0911", "Too many return statements {:d}/{:d}", {
  }), (
"R0912", "Too many branches {:d}/{:d}", {
  }), (
"R0913", "Too many named arguments {:d}/{:d}", {
  }), (
"R0914", "Too many local variables {:d}/{:d}", {
  }), (
"R0915", "Too many statements {:d}/{:d}", {
}), (
"W0101", "Unreachable code", {
}), (
"W0104", "Use of deprecated command {:s}", {
}), (
"W0105",
"{:s} variable '{:s}' which matches a built-in except for case", {
"description": """
This warning means that you are using a variable such as,
for example, `cmake_cxx_standard` which matches a builtin variable
(`CMAKE_CXX_STANDARD`) except for the case. If this was intentional, then it's
bad practice as it causes confusion (there are two variables in the namespace
with identical name except for case), though it was probably not intentional
and you probably aren't assigning to the correct variable.

This warning may be emitted for assignment (e.g. `set()` or `list()`) as
well as for variable expansion in an argument (e.g. `"${CMAKE_Cxx_STANDARD}"`).
"""
}), (
"W0106",
"String looks like a variable reference missing an {:s} tag '{:s}'", {
})
]
