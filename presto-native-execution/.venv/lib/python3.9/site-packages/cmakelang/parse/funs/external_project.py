from cmakelang.parse.additional_nodes import ShellCommandNode
from cmakelang.parse.argument_nodes import (
    PositionalParser, StandardArgTree, StandardParser)


def parse_external_project_add(ctx, tokens, breakstack):
  """
  ::

    ExternalProject_Add(<name> [<option>...])

  :see: https://cmake.org/cmake/help/v3.14/module/ExternalProject.html
  """
  return StandardArgTree.parse(
      ctx, tokens,
      npargs=1,
      kwargs={
          # Directory Options
          "PREFIX": PositionalParser(1),
          "TMP_DIR": PositionalParser(1),
          "STAMP_DIR": PositionalParser(1),
          "LOG_DIR": PositionalParser(1),
          "DOWNLOAD_DIR": PositionalParser(1),
          "SOURCE_DIR": PositionalParser(1),
          "BINARY_DIR": PositionalParser(1),
          "INSTALL_DIR": PositionalParser(1),
          # Download Step Options
          "DOWNLOAD_COMMAND": ShellCommandNode.parse,
          "URL": PositionalParser('+'),
          "URL_HASH": PositionalParser(1),
          "URL_MD5": PositionalParser(1),
          "DOWNLOAD_NAME": PositionalParser(1),
          "DOWNLOAD_NO_EXTRACT": PositionalParser(1),
          "DOWNLOAD_NO_PROGRESS": PositionalParser(1),
          "TIMEOUT": PositionalParser(1),
          "HTTP_USERNAME": PositionalParser(1),
          "HTTP_PASSWORD": PositionalParser(1),
          "HTTP_HEADER": PositionalParser('+'),
          "TLS_VERIFY": PositionalParser(1),
          "TLS_CAINFO": PositionalParser(1),
          "NETRC": PositionalParser(
              1, flags=["CMAKE_NETRC", "IGNORED", "OPTIONAL", "REQUIRED"]),
          "NETRC_FILE": PositionalParser(1),
          # Git
          "GIT_REPOSITORY": PositionalParser(1),
          "GIT_TAG": PositionalParser(1),
          "GIT_REMOTE_NAME": PositionalParser(1),
          "GIT_SUBMODULES": PositionalParser('+'),
          "GIT_SHALLOW": PositionalParser(1),
          "GIT_PROGRESS": PositionalParser(1),
          "GIT_CONFIG": PositionalParser('+'),
          # Subversion
          "SVN_REPOSITORY": PositionalParser(1),
          "SVN_REVISION": PositionalParser(1),
          "SVN_USERNAME": PositionalParser(1),
          "SVN_PASSWORD": PositionalParser(1),
          "SVN_TRUST_CERT": PositionalParser(1),
          # Mercurial
          "HG_REPOSITORY": PositionalParser(1),
          "HG_TAG": PositionalParser(1),
          # CVS
          "CVS_REPOSITORY": PositionalParser(1),
          "CVS_MODULE": PositionalParser(1),
          "CVS_TAG": PositionalParser(1),
          # Update/Patch Step Options
          "UPDATE_COMMAND": ShellCommandNode.parse,
          "UPDATE_DISCONNECTED": PositionalParser(1),
          "PATCH_COMMAND": ShellCommandNode.parse,
          # Configure Step Options
          "CONFIGURE_COMMAND": ShellCommandNode.parse,
          "CMAKE_COMMAND": PositionalParser(1),
          "CMAKE_GENERATOR": PositionalParser(1),
          "CMAKE_GENERATOR_PLATFORM": PositionalParser(1),
          "CMAKE_GENERATOR_TOOLSET": PositionalParser(1),
          "CMAKE_GENERATOR_INSTANCE": PositionalParser(1),
          "CMAKE_ARGS": PositionalParser('+'),
          "CMAKE_CACHE_ARGS": PositionalParser('+'),
          "CMAKE_CACHE_DEFAULT_ARGS": PositionalParser('+'),
          "SOURCE_SUBDIR": PositionalParser(1),
          # Build Step Options
          "BUILD_COMMAND": ShellCommandNode.parse,
          "BUILD_IN_SOURCE": PositionalParser(1),
          "BUILD_ALWAYS": PositionalParser(1),
          "BUILD_BYPRODUCTS": PositionalParser('+'),
          # Install Step Options
          "INSTALL_COMMAND": ShellCommandNode.parse,
          # Test Step Options
          "TEST_COMMAND": ShellCommandNode.parse,
          "TEST_BEFORE_INSTALL": PositionalParser(1),
          "TEST_AFTER_INSTALL": PositionalParser(1),
          "TEST_EXCLUDE_FROM_MAIN": PositionalParser(1),
          # Output Logging Options
          "LOG_DOWNLOAD": PositionalParser(1),
          "LOG_UPDATE": PositionalParser(1),
          "LOG_PATCH": PositionalParser(1),
          "LOG_CONFIGURE": PositionalParser(1),
          "LOG_BUILD": PositionalParser(1),
          "LOG_INSTALL": PositionalParser(1),
          "LOG_TEST": PositionalParser(1),
          "LOG_MERGED_STDOUTERR": PositionalParser(1),
          "LOG_OUTPUT_ON_FAILURE": PositionalParser(1),
          # Terminal Access Options
          "USES_TERMINAL_DOWNLOAD": PositionalParser(1),
          "USES_TERMINAL_UPDATE": PositionalParser(1),
          "USES_TERMINAL_CONFIGURE": PositionalParser(1),
          "USES_TERMINAL_BUILD": PositionalParser(1),
          "USES_TERMINAL_INSTALL": PositionalParser(1),
          "USES_TERMINAL_TEST": PositionalParser(1),
          # Target Options
          "DEPENDS": PositionalParser('+'),
          "EXCLUDE_FROM_ALL": PositionalParser(1),
          "STEP_TARGETS": PositionalParser('+'),
          "INDEPENDENT_STEP_TARGETS": PositionalParser('+'),
          # Misc Options
          "LIST_SEPARATOR": PositionalParser(1),
          "COMMAND": ShellCommandNode.parse,
      },
      flags=[],
      breakstack=breakstack)


def parse_external_project_add_step(ctx, tokens, breakstack):
  """
  ::

    ExternalProject_Add_Step(<name> [<option>...])

  :see: https://cmake.org/cmake/help/v3.14/module/ExternalProject.html#command:externalproject_add_step
  """
  return StandardArgTree.parse(
      ctx, tokens,
      npargs=2,
      kwargs={
          "COMMAND": ShellCommandNode.parse,
          "COMMENT": PositionalParser('+'),
          "DEPENDEES": PositionalParser('+'),
          "DEPENDERS": PositionalParser('+'),
          "DEPENDS": PositionalParser('+'),
          "BYPRODUCTS": PositionalParser('+'),
          "ALWAYS": PositionalParser(1),
          "EXCLUDE_FROM_MAIN": PositionalParser(1),
          "WORKING_DIRECTORY": PositionalParser(1),
          "LOG": PositionalParser(1),
          "USES_TERMINAL": PositionalParser(1),
      },
      flags=[],
      breakstack=breakstack)


def populate_db(parse_db):
  parse_db["externalproject_add"] = parse_external_project_add
  parse_db["externalproject_get_property"] = StandardParser('+')
  parse_db["externalproject_add_step"] = parse_external_project_add_step
  parse_db["externalproject_add_steptargets"] = \
      StandardParser('+', flags=["NO_DEPENDS"])
  parse_db["externalproject_add_stepdependencies"] = StandardParser('+')
