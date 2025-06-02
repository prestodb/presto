from cmakelang.parse.argument_nodes import (
    PositionalParser,
    StandardArgTree,
)
from cmakelang.parse.util import (
    get_nth_semantic_token
)


def parse_build_name(ctx, tokens, breakstack):
  """
  ::

    build_name(variable)

  :see: https://cmake.org/cmake/help/latest/command/build_name.html
  """
  return StandardArgTree.parse(ctx, tokens, 1, {}, [], breakstack)


def parse_exec_program(ctx, tokens, breakstack):
  """
  ::

    exec_program(Executable [directory in which to run]
                 [ARGS <arguments to executable>]
                 [OUTPUT_VARIABLE <var>]
                 [RETURN_VALUE <var>])

  :see: https://cmake.org/cmake/help/latest/command/exec_program.html
  """
  kwargs = {
      "ARGS": PositionalParser("+"),
      "OUTPUT_VARIABLE": PositionalParser(1),
      "RETURN_VALUE": PositionalParser(1)
  }

  return StandardArgTree.parse(ctx, tokens, 2, kwargs, [], breakstack)


def parse_install_files(ctx, tokens, breakstack):
  """
  ::

    install_files(<dir> extension file file ...)
    install_files(<dir> regexp)
    install_files(<dir> FILES file file ...)

  :see: https://cmake.org/cmake/help/latest/command/install_files.html
  """
  second_token = get_nth_semantic_token(tokens, 1)
  third_token = get_nth_semantic_token(tokens, 2)

  if second_token is not None and second_token.spelling.upper() == "FILES":
    return StandardArgTree.parse(ctx, tokens, "3+", {}, ["FILES"], breakstack)

  if third_token is None:
    return StandardArgTree.parse(ctx, tokens, 2, {}, [], breakstack)

  return StandardArgTree.parse(ctx, tokens, "3+", {}, [], breakstack)


def parse_install_programs(ctx, tokens, breakstack):
  """
  ::

    install_programs(<dir> file1 file2 [file3 ...])
    install_programs(<dir> FILES file1 [file2 ...])
    install_programs(<dir> regexp)

  :see: https://cmake.org/cmake/help/latest/command/install_programs.html
  """
  second_token = get_nth_semantic_token(tokens, 1)
  third_token = get_nth_semantic_token(tokens, 2)

  if second_token is not None and second_token.spelling.upper() == "FILES":
    return StandardArgTree.parse(ctx, tokens, "3+", {}, ["FILES"], breakstack)

  if third_token is None:
    return StandardArgTree.parse(ctx, tokens, 2, {}, [], breakstack)

  return StandardArgTree.parse(ctx, tokens, "3+", {}, [], breakstack)


def parse_install_targets(ctx, tokens, breakstack):
  """
  ::

    install_targets(<dir> [RUNTIME_DIRECTORY dir] target target)

  :see: https://cmake.org/cmake/help/latest/command/install_targets.html
  """
  kwargs = {
      "RUNTIME_DIRECTORY": PositionalParser(1)
  }
  return StandardArgTree.parse(ctx, tokens, "2+", kwargs, [], breakstack)


def parse_make_directory(ctx, tokens, breakstack):
  """
  ::

    make_directory(directory)

  :see: https://cmake.org/cmake/help/latest/command/make_directory.html
  """
  return StandardArgTree.parse(ctx, tokens, 1, {}, [], breakstack)


def parse_remove(ctx, tokens, breakstack):
  """
  ::

    remove(VAR VALUE VALUE ...)

  :see: https://cmake.org/cmake/help/latest/command/remove.html
  """
  return StandardArgTree.parse(ctx, tokens, "2+", {}, [], breakstack)


def parse_subdir_depends(ctx, tokens, breakstack):
  """
  ::

    subdir_depends(subdir dep1 dep2 ...)

  :see: https://cmake.org/cmake/help/latest/command/subdir_depends.html
  """
  return StandardArgTree.parse(ctx, tokens, "2+", {}, [], breakstack)


def parse_subdirs(ctx, tokens, breakstack):
  """
  ::

    subdirs(dir1 dir2 ...[EXCLUDE_FROM_ALL exclude_dir1 exclude_dir2 ...]
            [PREORDER] )

  :see: https://cmake.org/cmake/help/latest/command/subdirs.html
  """
  kwargs = {
      "EXCLUDE_FROM_ALL": PositionalParser("+"),
  }
  flags = ["PREORDER"]
  return StandardArgTree.parse(ctx, tokens, "+", kwargs, flags, breakstack)


def parse_use_mangled_mesa(ctx, tokens, breakstack):
  """
  ::

    use_mangled_mesa(PATH_TO_MESA OUTPUT_DIRECTORY)

  :see: https://cmake.org/cmake/help/latest/command/use_mangled_mesa.html
  """
  return StandardArgTree.parse(ctx, tokens, 2, {}, [], breakstack)


def parse_utility_source(ctx, tokens, breakstack):
  """
  ::

    utility_source(cache_entry executable_name
                   path_to_source [file1 file2 ...])

  :see: https://cmake.org/cmake/help/latest/command/utility_source.html
  """
  return StandardArgTree.parse(ctx, tokens, "3+", {}, [], breakstack)


def parse_variable_requires(ctx, tokens, breakstack):
  """
  ::

    variable_requires(TEST_VARIABLE RESULT_VARIABLE
                      REQUIRED_VARIABLE1
                      REQUIRED_VARIABLE2 ...)

  :see: https://cmake.org/cmake/help/latest/command/variable_requires.html
  """
  return StandardArgTree.parse(ctx, tokens, "3+", {}, [], breakstack)


def parse_write_file(ctx, tokens, breakstack):
  """
  ::

    write_file(filename "message to write"... [APPEND])

  :see: https://cmake.org/cmake/help/latest/command/write_file.html
  """
  return StandardArgTree.parse(ctx, tokens, "2+", {}, ["APPEND"], breakstack)


def populate_db(parse_db):
  parse_db["build_name"] = parse_build_name
  parse_db["exec_program"] = parse_exec_program
  parse_db["install_files"] = parse_install_files
  parse_db["install_programs"] = parse_install_programs
  parse_db["install_targets"] = parse_install_targets
  parse_db["make_directory"] = parse_make_directory
  parse_db["remove"] = parse_remove
  parse_db["subdir_depends"] = parse_subdir_depends
  parse_db["subdirs"] = parse_subdirs
  parse_db["use_mangled_mesa"] = parse_use_mangled_mesa
  parse_db["utility_source"] = parse_utility_source
  parse_db["variable_requires"] = parse_variable_requires
  parse_db["write_file"] = parse_write_file
