"""
Command specifications for cmake built-in commands.
"""

from __future__ import unicode_literals

from cmakelang.parse.funs import standard_builtins, standard_modules
from cmakelang.parse.util import CommandSpec

CANONICAL_SPELLINGS = {
    "FetchContent_Declare",
    "FetchContent_MakeAvailable",
    "FetchContent_GetProperties",
    "FetchContent_Populate",
    "ExternalProject_Add",
    "ExternalProject_Add_Step",
    "ExternalProject_Add_StepDependencies",
    "ExternalProject_Add_StepTargets",
    "ExternalProject_Get_Property",
}


def get_default_config():
  """
  Return the default per-command configuration database
  """
  per_command = {}
  for spelling in CANONICAL_SPELLINGS:
    per_command[spelling.lower()] = {
        "spelling": spelling
    }

  return per_command


def get_fn_spec():
  """
  Return a dictionary mapping cmake function names to a dictionary containing
  kwarg specifications.
  """

  fn_spec = CommandSpec('<root>')
  for grouping in (standard_builtins, standard_modules):
    for funname, spec in sorted(grouping.FUNSPECS.items()):
      fn_spec.add(funname, **spec)
  return fn_spec


# pylint: disable=bad-continuation
# pylint: disable=too-many-lines
