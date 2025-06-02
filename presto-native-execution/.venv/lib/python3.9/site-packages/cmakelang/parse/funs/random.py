from cmakelang.parse.argument_nodes import (
    PositionalParser,
    StandardArgTree,
)
from cmakelang.parse.util import (
    iter_semantic_tokens,
)


def parse_ctest_build(ctx, tokens, breakstack):
  """
  ::

    ctest_build([BUILD <build-dir>] [APPEND]
                [CONFIGURATION <config>]
                [FLAGS <flags>]
                [PROJECT_NAME <project-name>]
                [TARGET <target-name>]
                [NUMBER_ERRORS <num-err-var>]
                [NUMBER_WARNINGS <num-warn-var>]
                [RETURN_VALUE <result-var>]
                [CAPTURE_CMAKE_ERROR <result-var>]
                )

  :see: https://cmake.org/cmake/help/latest/command/ctest_build.html
  """
  kwargs = {
      "BUILD": PositionalParser(1),
      "CONFIGURATION": PositionalParser(1),
      "FLAGS": PositionalParser(1),
      "PROJECT_NAME": PositionalParser(1),
      "TARGET": PositionalParser(1),
      "NUMBER_ERRORS": PositionalParser(1),
      "NUMBER_WARNINGS": PositionalParser(1),
      "RETURN_VALUE": PositionalParser(1),
      "CAPTURE_CMAKE_ERROR": PositionalParser(1)
  }
  return StandardArgTree.parse(ctx, tokens, "+", kwargs, ["APPEND"], breakstack)


def parse_ctest_configure(ctx, tokens, breakstack):
  """
  ::

    ctest_configure([BUILD <build-dir>] [SOURCE <source-dir>] [APPEND]
                    [OPTIONS <options>] [RETURN_VALUE <result-var>] [QUIET]
                    [CAPTURE_CMAKE_ERROR <result-var>])

  :see: https://cmake.org/cmake/help/latest/command/ctest_configure.html
  """
  kwargs = {
      "BUILD": PositionalParser(1),
      "SOURCE": PositionalParser(1),
      "OPTIONS": PositionalParser(1),
      "RETURN_VALUE": PositionalParser(1),
      "CAPTURE_CMAKE_ERROR": PositionalParser(1)
  }
  return StandardArgTree.parse(
      ctx, tokens, "+", kwargs, ["APPEND", "QUIET"], breakstack)


def parse_ctest_coverage(ctx, tokens, breakstack):
  """
  ::

    ctest_coverage([BUILD <build-dir>] [APPEND]
                   [LABELS <label>...]
                   [RETURN_VALUE <result-var>]
                   [CAPTURE_CMAKE_ERROR <result-var>]
                   [QUIET]
                   )

  :see: https://cmake.org/cmake/help/latest/command/ctest_coverage.html
  """
  kwargs = {
      "BUILD": PositionalParser(1),
      "LABELS": PositionalParser("+"),
      "RETURN_VALUE": PositionalParser(1),
      "CAPTURE_CMAKE_ERROR": PositionalParser(1),
  }
  return StandardArgTree.parse(
      ctx, tokens, "+", kwargs, ["APPEND", "QUIET"], breakstack)


def parse_ctest_empty_binary_directory(ctx, tokens, breakstack):
  """
  ::

    ctest_empty_binary_directory( directory )

  :see: https://cmake.org/cmake/help/latest/command/ctest_empty_binary_directory.html
  """
  return StandardArgTree.parse(ctx, tokens, 1, {}, [], breakstack)


def parse_ctest_memcheck(ctx, tokens, breakstack):
  """
  ::

    ctest_memcheck([BUILD <build-dir>] [APPEND]
                   [START <start-number>]
                   [END <end-number>]
                   [STRIDE <stride-number>]
                   [EXCLUDE <exclude-regex>]
                   [INCLUDE <include-regex>]
                   [EXCLUDE_LABEL <label-exclude-regex>]
                   [INCLUDE_LABEL <label-include-regex>]
                   [EXCLUDE_FIXTURE <regex>]
                   [EXCLUDE_FIXTURE_SETUP <regex>]
                   [EXCLUDE_FIXTURE_CLEANUP <regex>]
                   [PARALLEL_LEVEL <level>]
                   [TEST_LOAD <threshold>]
                   [SCHEDULE_RANDOM <ON|OFF>]
                   [STOP_TIME <time-of-day>]
                   [RETURN_VALUE <result-var>]
                   [DEFECT_COUNT <defect-count-var>]
                   [QUIET]
                   )

  :see: https://cmake.org/cmake/help/latest/command/ctest_memcheck.html
  """
  kwargs = {
      "BUILD": PositionalParser(1),
      "START": PositionalParser(1),
      "END": PositionalParser(1),
      "STRIDE": PositionalParser(1),
      "EXCLUDE": PositionalParser(1),
      "INCLUDE": PositionalParser(1),
      "EXCLUDE_LABEL": PositionalParser(1),
      "INCLUDE_LABEL": PositionalParser(1),
      "EXCLUDE_FIXTURE": PositionalParser(1),
      "EXCLUDE_FIXTURE_SETUP": PositionalParser(1),
      "EXCLUDE_FIXTURE_CLEANUP": PositionalParser(1),
      "PARALLEL_LEVEL": PositionalParser(1),
      "TEST_LOAD": PositionalParser(1),
      "SCHEDULE_RANDOM": PositionalParser(1, flags=["ON", "OFF"]),
      "STOP_TIME": PositionalParser(1),
      "RETURN_VALUE": PositionalParser(1),
      "DEFECT_COUNT": PositionalParser(1),
  }
  flags = ["APPEND", "QUIET"]
  return StandardArgTree.parse(ctx, tokens, 0, kwargs, flags, breakstack)


def parse_ctest_read_custom_files(ctx, tokens, breakstack):
  """
  ::

    ctest_read_custom_files( directory ... )

  :see: https://cmake.org/cmake/help/latest/command/ctest_read_custom_files.html
  """
  return StandardArgTree.parse(ctx, tokens, "+", {}, [], breakstack)


def parse_ctest_run_script(ctx, tokens, breakstack):
  """
  ::

    ctest_run_script([NEW_PROCESS] script_file_name script_file_name1
                     script_file_name2 ... [RETURN_VALUE var])

  :see: https://cmake.org/cmake/help/latest/command/ctest_run_script.html
  """
  kwargs = {"RETURN_VALUE": PositionalParser(1)}
  flags = ["NEW_PROCESS"]
  return StandardArgTree.parse(ctx, tokens, "+", kwargs, flags, breakstack)


def parse_ctest_sleep(ctx, tokens, breakstack):
  """
  ::

    ctest_sleep(<seconds>)
    ctest_sleep(<time1> <duration> <time2>)

  :see: https://cmake.org/cmake/help/latest/command/ctest_sleep.html
  """
  semantic_tokens = list(iter_semantic_tokens(tokens))
  if len(semantic_tokens) == 1:
    return StandardArgTree.parse(ctx, tokens, 1, {}, [], breakstack)
  return StandardArgTree.parse(ctx, tokens, 3, {}, [], breakstack)


def parse_ctest_start(ctx, tokens, breakstack):
  """
  ::

    ctest_start(<model> [<source> [<binary>]] [GROUP <group>] [QUIET])
    ctest_start([<model> [<source> [<binary>]]] [GROUP <group>] APPEND [QUIET])

  :see: https://cmake.org/cmake/help/latest/command/ctest_start.html
  """
  kwargs = {
      "GROUP": PositionalParser(1)
  }
  flags = ["QUIET", "APPEND"]
  return StandardArgTree.parse(ctx, tokens, "+", kwargs, flags, breakstack)


def parse_ctest_submit(ctx, tokens, breakstack):
  """
  ::

    ctest_submit([PARTS <part>...] [FILES <file>...]
                 [SUBMIT_URL <url>]
                 [BUILD_ID <result-var>]
                 [HTTPHEADER <header>]
                 [RETRY_COUNT <count>]
                 [RETRY_DELAY <delay>]
                 [RETURN_VALUE <result-var>]
                 [CAPTURE_CMAKE_ERROR <result-var>]
                 [QUIET]
                 )

  :see: https://cmake.org/cmake/help/latest/command/ctest_submit.html
  """
  kwargs = {
      "PARTS": PositionalParser("+"),
      "FILES": PositionalParser("+"),
      "SUBMIT_URL": PositionalParser(1),
      "BUILD_ID": PositionalParser(1),
      "HTTPHEADER": PositionalParser(1),
      "RETRY_COUNT": PositionalParser(1),
      "RETRY_DELAY": PositionalParser(1),
      "RETURN_VALUE": PositionalParser(1),
      "CAPTURE_CMAKE_ERROR": PositionalParser(1),
  }
  flags = ["QUIET"]
  return StandardArgTree.parse(ctx, tokens, "+", kwargs, flags, breakstack)


def parse_ctest_test(ctx, tokens, breakstack):
  """
  ::

    ctest_test([BUILD <build-dir>] [APPEND]
               [START <start-number>]
               [END <end-number>]
               [STRIDE <stride-number>]
               [EXCLUDE <exclude-regex>]
               [INCLUDE <include-regex>]
               [EXCLUDE_LABEL <label-exclude-regex>]
               [INCLUDE_LABEL <label-include-regex>]
               [EXCLUDE_FIXTURE <regex>]
               [EXCLUDE_FIXTURE_SETUP <regex>]
               [EXCLUDE_FIXTURE_CLEANUP <regex>]
               [PARALLEL_LEVEL <level>]
               [RESOURCE_SPEC_FILE <file>]
               [TEST_LOAD <threshold>]
               [SCHEDULE_RANDOM <ON|OFF>]
               [STOP_TIME <time-of-day>]
               [RETURN_VALUE <result-var>]
               [CAPTURE_CMAKE_ERROR <result-var>]
               [QUIET]
               )

  :see: https://cmake.org/cmake/help/latest/command/ctest_test.html
  """
  kwargs = {
      "BUILD": PositionalParser(1),
      "START": PositionalParser(1),
      "END": PositionalParser(1),
      "STRIDE": PositionalParser(1),
      "EXCLUDE": PositionalParser(1),
      "INCLUDE": PositionalParser(1),
      "EXCLUDE_LABEL": PositionalParser(1),
      "INCLUDE_LABEL": PositionalParser(1),
      "EXCLUDE_FIXTURE": PositionalParser(1),
      "EXCLUDE_FIXTURE_SETUP": PositionalParser(1),
      "EXCLUDE_FIXTURE_CLEANUP": PositionalParser(1),
      "PARALLEL_LEVEL": PositionalParser(1),
      "RESOURCE_SPEC_FILE": PositionalParser(1),
      "TEST_LOAD": PositionalParser(1),
      "SCHEDULE_RANDOM": PositionalParser(1, flags=["ON", "OFF"]),
      "STOP_TIME": PositionalParser(1),
      "RETURN_VALUE": PositionalParser(1),
      "CAPTURE_CMAKE_ERROR": PositionalParser(1),
  }
  flags = ["APPEND", "QUIET"]
  return StandardArgTree.parse(ctx, tokens, "+", kwargs, flags, breakstack)


def parse_ctest_update(ctx, tokens, breakstack):
  """
  ::

    ctest_update([SOURCE <source-dir>]
                 [RETURN_VALUE <result-var>]
                 [CAPTURE_CMAKE_ERROR <result-var>]
                 [QUIET])

  :see: https://cmake.org/cmake/help/latest/command/ctest_update.html
  """
  kwargs = {
      "SOURCE": PositionalParser(1),
      "RETURN_VALUE": PositionalParser(1),
      "CAPTURE_CMAKE_ERROR": PositionalParser(1)
  }
  flags = ["QUIET"]
  return StandardArgTree.parse(ctx, tokens, "+", kwargs, flags, breakstack)


def parse_ctest_upload(ctx, tokens, breakstack):
  """
  ::

    ctest_upload(FILES <file>... [QUIET] [CAPTURE_CMAKE_ERROR <result-var>])

  :see: https://cmake.org/cmake/help/latest/command/ctest_upload.html
  """
  kwargs = {
      "FILES": PositionalParser("+"),
      "CAPTURE_CMAKE_ERROR": PositionalParser(1)
  }
  flags = ["QUIET"]
  return StandardArgTree.parse(ctx, tokens, "+", kwargs, flags, breakstack)


def parse_target_compile_definitions(ctx, tokens, breakstack):
  """
  ::

    target_compile_definitions(<target>
        <INTERFACE|PUBLIC|PRIVATE> [items1...]
        [<INTERFACE|PUBLIC|PRIVATE> [items2...] ...])

  :see: https://cmake.org/cmake/help/latest/command/target_compile_definitions.html
  """
  kwargs = {
      "INTERFACE": PositionalParser("+"),
      "PUBLIC": PositionalParser("+"),
      "PRIVATE": PositionalParser("+")
  }
  return StandardArgTree.parse(ctx, tokens, 1, kwargs, [], breakstack)


def parse_target_compile_features(ctx, tokens, breakstack):
  """
  ::

    target_compile_features(<target> <PRIVATE|PUBLIC|INTERFACE> <feature> [...])

  :see: https://cmake.org/cmake/help/latest/command/target_compile_features.html
  """
  kwargs = {
      "INTERFACE": PositionalParser("+"),
      "PUBLIC": PositionalParser("+"),
      "PRIVATE": PositionalParser("+"),
  }
  return StandardArgTree.parse(ctx, tokens, 1, kwargs, [], breakstack)


def parse_target_sources(ctx, tokens, breakstack):
  """
  ::

    target_sources(<target>
      <INTERFACE|PUBLIC|PRIVATE> [items1...]
      [<INTERFACE|PUBLIC|PRIVATE> [items2...] ...])

  :see: https://cmake.org/cmake/help/latest/command/target_sources.html
  """
  kwargs = {
      "INTERFACE": PositionalParser("+"),
      "PUBLIC": PositionalParser("+"),
      "PRIVATE": PositionalParser("+"),
  }
  return StandardArgTree.parse(ctx, tokens, 1, kwargs, [], breakstack)


def parse_unset(ctx, tokens, breakstack):
  """
  ::

    unset(<variable> [CACHE | PARENT_SCOPE])
    unset(ENV{<variable>})

  :see: https://cmake.org/cmake/help/latest/command/unset.html
  """
  flags = ["CACHE", "PARENT_SCOPE"]
  return StandardArgTree.parse(ctx, tokens, "+", {}, flags, breakstack)


def parse_variable_watch(ctx, tokens, breakstack):
  """
  ::

    variable_watch(<variable> [<command>])

  :see: https://cmake.org/cmake/help/latest/command/variable_watch.html
  """
  return StandardArgTree.parse(ctx, tokens, "+", {}, [], breakstack)


def parse_add_compile_definitions(ctx, tokens, breakstack):
  """
  ::

  add_compile_definitions(<definition> ...)

  :see: https://cmake.org/cmake/help/latest/command/add_compile_definitions.html
  """
  return StandardArgTree.parse(ctx, tokens, "+", {}, [], breakstack)


def populate_db(parse_db):
  parse_db["ctest_build"] = parse_ctest_build
  parse_db["ctest_configure"] = parse_ctest_configure
  parse_db["ctest_coverage"] = parse_ctest_coverage
  parse_db["ctest_empty_binary_directory"] = parse_ctest_empty_binary_directory
  parse_db["ctest_memcheck"] = parse_ctest_memcheck
  parse_db["ctest_read_custom_files"] = parse_ctest_read_custom_files
  parse_db["ctest_run_script"] = parse_ctest_run_script
  parse_db["ctest_sleep"] = parse_ctest_sleep
  parse_db["ctest_start"] = parse_ctest_start
  parse_db["ctest_submit"] = parse_ctest_submit
  parse_db["ctest_test"] = parse_ctest_test
  parse_db["ctest_update"] = parse_ctest_update
  parse_db["ctest_upload"] = parse_ctest_upload
  parse_db["target_compile_definitions"] = parse_target_compile_definitions
  parse_db["target_compile_features"] = parse_target_compile_features
  parse_db["target_sources"] = parse_target_sources
  parse_db["unset"] = parse_unset
  parse_db["variable_watch"] = parse_variable_watch
  parse_db["add_compile_definitions"] = parse_add_compile_definitions
