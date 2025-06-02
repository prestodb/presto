from cmakelang import lex
from cmakelang.parse.argument_nodes import (
    PositionalParser,
    StandardArgTree,
)
from cmakelang.parse.additional_nodes import (
    TupleParser
)
from cmakelang.parse.util import (
    get_nth_semantic_token
)


def parse_build_command(ctx, tokens, breakstack):
  """
  ::

    build_command(<variable>
                  [CONFIGURATION <config>]
                  [TARGET <target>]
                  [PROJECT_NAME <projname>] # legacy, causes warning
                 )

  :see: https://cmake.org/cmake/help/latest/command/build_command.html
  """
  kwargs = {
      "CONFIGURATION": PositionalParser(1),
      "TARGET": PositionalParser(1),
      "PROJECT_NAME": PositionalParser(1)
  }

  second_token = get_nth_semantic_token(tokens, 1)
  if second_token is not None and second_token.spelling.upper not in kwargs:
    ctx.lint_ctx.record_lint(
        "W0106", "build_command(<cachevariable> <makecommand>)",
        location=tokens[0].get_location())
    return StandardArgTree.parse(ctx, tokens, "2", {}, [], breakstack)

  return StandardArgTree.parse(ctx, tokens, "1", kwargs, [], breakstack)


def parse_cmake_parse_arguments(ctx, tokens, breakstack):
  """
  ::

  cmake_parse_arguments(<prefix> <options> <one_value_keywords>
                      <multi_value_keywords> <args>...)

  cmake_parse_arguments(PARSE_ARGV <N> <prefix> <options>
                        <one_value_keywords> <multi_value_keywords>)

  :see: https://cmake.org/cmake/help/latest/command/cmake_parse_arguments.html
  """

  first_token = get_nth_semantic_token(tokens, 0)
  if first_token is not None and first_token.spelling.upper == "PARSE_ARGV":
    return StandardArgTree.parse(
        ctx, tokens, "6", {}, ["PARSE_ARGV"], breakstack)
  return StandardArgTree.parse(
      ctx, tokens, "5+", {}, [], breakstack)


def parse_cmake_policy(ctx, tokens, breakstack):
  """
  ::

    cmake_policy(VERSION <min>[...<max>])
    cmake_policy(SET CMP<NNNN> NEW)
    cmake_policy(SET CMP<NNNN> OLD)
    cmake_policy(GET CMP<NNNN> <variable>)
    cmake_policy(PUSH)
    cmake_policy(POP)

  :see: https://cmake.org/cmake/help/latest/command/cmake_policy.html
  """
  first_token = get_nth_semantic_token(tokens, 0)
  if first_token is None:
    return StandardArgTree.parse(ctx, tokens, "+", {}, [], breakstack)

  if first_token.type is lex.TokenType.DEREF:
    ctx.lint_ctx.record_lint("C0114", location=first_token.get_location())
    return StandardArgTree.parse(ctx, tokens, "+", {}, [], breakstack)

  descriminator = first_token.spelling.upper()
  if descriminator == "VERSION":
    return StandardArgTree.parse(ctx, tokens, "2+", {}, ["VERSION"], breakstack)

  if descriminator == "SET":
    return StandardArgTree.parse(
        ctx, tokens, 3, {}, ["SET", "OLD", "NEW"], breakstack)

  if descriminator == "GET":
    return StandardArgTree.parse(ctx, tokens, 3, {}, ["GET"], breakstack)

  if descriminator in ("PUSH", "POP"):
    return StandardArgTree.parse(
        ctx, tokens, 1, {}, ["PUSH", "POP"], breakstack)

  ctx.lint_ctx.record_lint("E1126", location=first_token.get_location())
  return StandardArgTree.parse(
      ctx, tokens, 1, {}, ["PUSH", "POP"], breakstack)


def parse_create_test_sourcelist(ctx, tokens, breakstack):
  """
  ::

    create_test_sourcelist(sourceListName driverName
                           test1 test2 test3
                           EXTRA_INCLUDE include.h
                           FUNCTION function)

  :see: https://cmake.org/cmake/help/latest/command/create_test_sourcelist.html
  """

  kwargs = {
      "EXTRA_INCLUDE": PositionalParser(1),
      "FUNCTION": PositionalParser(1)
  }
  return StandardArgTree.parse(
      ctx, tokens, "3+", kwargs, [], breakstack)


def parse_export_library_dependencies(ctx, tokens, breakstack):
  """
  ::

    export_library_dependencies(<file> [APPEND])

  :see: https://cmake.org/cmake/help/latest/command/export_library_dependencies.html
  """
  return StandardArgTree.parse(
      ctx, tokens, "1+", {}, ["APPEND"], breakstack)


def parse_fltk_wrap_ui(ctx, tokens, breakstack):
  """
  ::

      fltk_wrap_ui(resultingLibraryName source1
                   source2 ... sourceN )

  :see: https://cmake.org/cmake/help/latest/command/fltk_wrap_ui.html
  """
  return StandardArgTree.parse(
      ctx, tokens, "2+", {}, [], breakstack)


def parse_get_cmake_property(ctx, tokens, breakstack):
  """
  ::

      get_cmake_property(VAR property)

  :see: https://cmake.org/cmake/help/latest/command/get_cmake_property.html
  """
  return StandardArgTree.parse(ctx, tokens, 2, {}, [], breakstack)


def parse_get_filename_component(ctx, tokens, breakstack):
  """
  ::

      get_filename_component(<VAR> <FileName> <COMP> [CACHE])

      get_filename_component(<VAR> FileName
                             PROGRAM [PROGRAM_ARGS <ARG_VAR>]
                             [CACHE])

  :see: https://cmake.org/cmake/help/latest/command/get_filename_component.html
  """
  descriminator = get_nth_semantic_token(tokens, 2)
  if descriminator is not None and descriminator.spelling.upper() == "PROGRAM":
    flags = ["PROGRAM", "PROGRAM_ARGS", "CACHE"]
    return StandardArgTree.parse(ctx, tokens, "3+", {}, flags, breakstack)

  flags = [
      "DIRECTORY",
      "NAME",
      "EXT",
      "NAME_WE",
      "ABSOLUTE",
      "REALPATH",
      "PATH",
  ]
  return StandardArgTree.parse(ctx, tokens, "3+", {}, flags, breakstack)


def parse_get_source_file_property(ctx, tokens, breakstack):
  """
  ::

    get_source_file_property(VAR file property)

  :see: https://cmake.org/cmake/help/latest/command/get_source_file_property.html
  """
  return StandardArgTree.parse(ctx, tokens, 3, {}, [], breakstack)


def parse_get_target_property(ctx, tokens, breakstack):
  """
  ::

    get_target_property(VAR target property)

  :see: https://cmake.org/cmake/help/latest/command/get_target_property.html
  """
  return StandardArgTree.parse(ctx, tokens, 3, {}, [], breakstack)


def parse_get_test_property(ctx, tokens, breakstack):
  """
  ::

    get_test_property(test property VAR)

  :see: https://cmake.org/cmake/help/latest/command/get_test_property.html
  """
  return StandardArgTree.parse(ctx, tokens, 3, {}, [], breakstack)


def parse_include_external_msproject(ctx, tokens, breakstack):
  """
  ::

    include_external_msproject(projectname location
                           [TYPE projectTypeGUID]
                           [GUID projectGUID]
                           [PLATFORM platformName]
                           dep1 dep2 ...)

  :see: https://cmake.org/cmake/help/latest/command/include_external_msproject.html
  """
  kwargs = {
      "TYPE": PositionalParser(1),
      "GUID": PositionalParser(1),
      "PLATFORM": PositionalParser(1)
  }
  return StandardArgTree.parse(ctx, tokens, "2+", kwargs, [], breakstack)


def parse_include_guard(ctx, tokens, breakstack):
  """
  ::

    include_guard([DIRECTORY|GLOBAL])

  :see: https://cmake.org/cmake/help/latest/command/include_guard.html
  """
  return StandardArgTree.parse(ctx, tokens, "*", {}, [], breakstack)


def parse_include_regular_expression(ctx, tokens, breakstack):
  """
  ::

    include_regular_expression(regex_match [regex_complain])

  :see: https://cmake.org/cmake/help/latest/command/include_regular_expression.html
  """
  return StandardArgTree.parse(ctx, tokens, "*", {}, [], breakstack)


def parse_link_directories(ctx, tokens, breakstack):
  """
  ::

    link_directories([AFTER|BEFORE] directory1 [directory2 ...])

  :see: https://cmake.org/cmake/help/latest/command/link_directories.html
  """
  return StandardArgTree.parse(
      ctx, tokens, "+", {}, ["AFTER", "BEFORE"], breakstack)


def parse_link_libraries(ctx, tokens, breakstack):
  """
  ::

    link_libraries([item1 [item2 [...]]]
                   [[debug|optimized|general] <item>] ...)

  :see: https://cmake.org/cmake/help/latest/command/link_libraries.html
  """
  return StandardArgTree.parse(
      ctx, tokens, "+", {}, ["DEBUG", "OPTIMIZED", "GENERAL"], breakstack)


def parse_load_cache(ctx, tokens, breakstack):
  """
  ::

    load_cache(pathToBuildDirectory READ_WITH_PREFIX prefix entry1...)
    load_cache(pathToBuildDirectory [EXCLUDE entry1...]
               [INCLUDE_INTERNALS entry1...])

  :see: https://cmake.org/cmake/help/latest/command/load_cache.html
  """
  kwargs = {
      "READ_WITH_PREFIX": PositionalParser("2+"),
      "EXCLUDE": PositionalParser("+"),
      "INCLUDE_INTERNALS": PositionalParser("+")
  }
  return StandardArgTree.parse(ctx, tokens, 1, kwargs, [], breakstack)


def parse_load_command(ctx, tokens, breakstack):
  """
  ::

    load_command(COMMAND_NAME <loc1> [loc2 ...])

  :see: https://cmake.org/cmake/help/latest/command/load_command.html
  """
  return StandardArgTree.parse(
      ctx, tokens, "2+", {}, ["COMMAND_NAME"], breakstack)


def parse_math(ctx, tokens, breakstack):
  """
  ::

    math(EXPR <variable> "<expression>" [OUTPUT_FORMAT <format>])

  :see: https://cmake.org/cmake/help/latest/command/math.html
  """
  kwargs = {
      "OUTPUT_FORMAT": PositionalParser(1)
  }
  return StandardArgTree.parse(ctx, tokens, "3", kwargs, [], breakstack)


def parse_option(ctx, tokens, breakstack):
  """
  ::

    option(<variable> "<help_text>" [value])

  :see: https://cmake.org/cmake/help/latest/command/option.html
  """
  return StandardArgTree.parse(ctx, tokens, "2+", {}, [], breakstack)


def parse_output_required_files(ctx, tokens, breakstack):
  """
  ::

    output_required_files(srcfile outputfile)

  :see: https://cmake.org/cmake/help/latest/command/output_required_files.html
  """
  return StandardArgTree.parse(ctx, tokens, 2, {}, [], breakstack)


def parse_qt_wrap_cpp(ctx, tokens, breakstack):
  """
  ::

    qt_wrap_cpp(resultingLibraryName DestName SourceLists ...)

  :see: https://cmake.org/cmake/help/latest/command/qt_wrap_cpp.html
  """
  return StandardArgTree.parse(ctx, tokens, "3+", {}, [], breakstack)


def parse_qt_wrap_ui(ctx, tokens, breakstack):
  """
  ::

    qt_wrap_ui(resultingLibraryName HeadersDestName
               SourcesDestName SourceLists ...)

  :see: https://cmake.org/cmake/help/latest/command/qt_wrap_ui.html
  """
  return StandardArgTree.parse(ctx, tokens, "4+", {}, [], breakstack)


def parse_remove_definitions(ctx, tokens, breakstack):
  """
  ::

    remove_definitions(-DFOO -DBAR ...)

  :see: https://cmake.org/cmake/help/latest/command/remove_definitions.html
  """
  return StandardArgTree.parse(ctx, tokens, "+", {}, [], breakstack)


def parse_separate_arguments(ctx, tokens, breakstack):
  """
  ::

    separate_arguments(<variable> <mode> <args>)

  :see: https://cmake.org/cmake/help/latest/command/separate_arguments.html
  """
  flags = ["UNIX_COMMAND", "WINDOWS_COMMAND", "NATIVE_COMMAND"]
  return StandardArgTree.parse(ctx, tokens, 3, {}, flags, breakstack)


def parse_set_source_files_properties(ctx, tokens, breakstack):
  """
  ::

    set_source_files_properties([file1 [file2 [...]]]
                                PROPERTIES prop1 value1
                                [prop2 value2 [...]])

  :see: https://cmake.org/cmake/help/latest/command/set_source_files_properties.html
  """
  kwargs = {
      "PROPERTIES": TupleParser(2, "+")
  }
  return StandardArgTree.parse(ctx, tokens, "+", kwargs, [], breakstack)


def parse_site_name(ctx, tokens, breakstack):
  """
  ::

    site_name(variable)

  :see: https://cmake.org/cmake/help/latest/command/site_name.html
  """
  return StandardArgTree.parse(ctx, tokens, 1, {}, [], breakstack)


def parse_source_group(ctx, tokens, breakstack):
  """
  ::

    source_group(<name> [FILES <src>...] [REGULAR_EXPRESSION <regex>])
    source_group(TREE <root> [PREFIX <prefix>] [FILES <src>...])
    source_group(<name> <regex>)

  :see: https://cmake.org/cmake/help/latest/command/source_group.html
  """
  descriminator = get_nth_semantic_token(tokens, 0)
  if descriminator is not None and descriminator.spelling.upper() == "TREE":
    kwargs = {
        "PREFIX": PositionalParser(1),
        "FILES": PositionalParser("+")
    }
    return StandardArgTree.parse(ctx, tokens, 2, kwargs, [], breakstack)

  kwargs = {
      "FILES": PositionalParser("+"),
      "REGULAR_EXPRESSION": PositionalParser(1)
  }

  descriminator = get_nth_semantic_token(tokens, 1)
  if descriminator is not None and descriminator.spelling.upper() not in kwargs:
    return StandardArgTree.parse(ctx, tokens, 2, {}, [], breakstack)

  return StandardArgTree.parse(ctx, tokens, 1, kwargs, [], breakstack)


def populate_db(parse_db):
  parse_db["build_command"] = parse_build_command
  parse_db["cmake_parse_arguments"] = parse_cmake_parse_arguments
  parse_db["cmake_policy"] = parse_cmake_policy
  parse_db["create_test_sourcelist"] = parse_create_test_sourcelist
  parse_db["export_library_dependencies"] = parse_export_library_dependencies
  parse_db["fltk_wrap_ui"] = parse_fltk_wrap_ui
  parse_db["get_cmake_property"] = parse_get_cmake_property
  parse_db["get_filename_component"] = parse_get_filename_component
  parse_db["get_source_file_property"] = parse_get_source_file_property
  parse_db["get_target_property"] = parse_get_target_property
  parse_db["get_test_property"] = parse_get_test_property
  parse_db["include_external_msproject"] = parse_include_external_msproject
  parse_db["include_guard"] = parse_include_guard
  parse_db["include_regular_expression"] = parse_include_regular_expression
  parse_db["link_directories"] = parse_link_directories
  parse_db["link_libraries"] = parse_link_libraries
  parse_db["load_cache"] = parse_load_cache
  parse_db["load_command"] = parse_load_command
  parse_db["math"] = parse_math
  parse_db["option"] = parse_option
  parse_db["output_required_files"] = parse_output_required_files
  parse_db["qt_wrap_cpp"] = parse_qt_wrap_cpp
  parse_db["qt_wrap_ui"] = parse_qt_wrap_ui
  parse_db["remove_definitions"] = parse_remove_definitions
  parse_db["separate_arguments"] = parse_separate_arguments
  parse_db["set_source_files_properties"] = parse_set_source_files_properties
  parse_db["site_name"] = parse_site_name
  parse_db["source_group"] = parse_source_group
