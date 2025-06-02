# pylint: disable=bad-continuation
# pylint: disable=too-many-lines

FUNSPECS = {
  "add_test": {
    "kwargs": {
      "COMMAND": {
        "pargs": "+",
        "kwargs": {
          "ARGS": "*"
        }
      },
      "CONFIGURATIONS": "*",
      "NAME": "*",
      "WORKING_DIRECTORY": "*"
    }
  },
  "cmake_host_system_information": {
    "kwargs": {
      "QUERY": "*",
      "RESULT": "*"
    }
  },
  "cmake_minimum_required": {
    "flags": [
      "FATAL_ERROR"
    ],
    "kwargs": {
      "VERSION": "*"
    }
  },
  "cmake_minimum_required_version": {
    "kwargs": {
      "VERSION": "*"
    }
  },
  "configure_file": {
    "flags": [
      "@ONLY",
      "COPYONLY",
      "ESCAPE_QUOTES"
    ],
    "kwargs": {
      "NEWLINE_STYLE": "*"
    }
  },
  "define_property": {
    "flags": [
      "CACHED_VARIABLE",
      "DIRECTORY",
      "GLOBAL",
      "INHERITED",
      "SOURCE",
      "TARGET",
      "TEST",
      "VARIABLE"
    ],
    "kwargs": {
      "BRIEF_DOCS": "*",
      "FULL_DOCS": "*",
      "PROPERTY": "*"
    }
  },
  "enable_language": {
    "flags": [
      "OPTIONAL"
    ],
    "kwargs": {}
  },
  "execute_process": {
    "flags": [
      "ERROR_QUIET",
      "ERROR_STRIP_TRAILING_WHITESPACE",
      "OUTPUT_QUIET",
      "OUTPUT_STRIP_TRAILING_WHITESPACE"
    ],
    "kwargs": {
      "COMMAND": "*",
      "ERROR_FILE": "*",
      "ERROR_VARIABLE": "*",
      "INPUT_FILE": "*",
      "OUTPUT_FILE": "*",
      "OUTPUT_VARIABLE": "*",
      "RESULT_VARIABLE": "*",
      "TIMEOUT": "*",
      "WORKING_DIRECTORY": "*"
    }
  },
  "export": {
    "flags": [
      "APPEND",
      "EXPORT_LINK_INTERFACE_LIBRARIES"
    ],
    "kwargs": {
      "ANDROID_MK": 1,
      "EXPORT": 1,
      "FILE": 1,
      "NAMESPACE": 1,
      "PACKAGE": 1,
      "TARGETS": "+"
    }
  },
  "find_file": {
    "flags": [
      "CMAKE_FIND_ROOT_PATH_BOTH",
      "NO_CMAKE_ENVIRONMENT_PATH",
      "NO_CMAKE_FIND_ROOT_PATH",
      "NO_CMAKE_PATH",
      "NO_CMAKE_SYSTEM_PATH",
      "NO_DEFAULT_PATH",
      "NO_SYSTEM_ENVIRONMENT_PATH",
      "ONLY_CMAKE_FIND_ROOT_PATH"
    ],
    "kwargs": {
      "DOC": "*",
      "HINTS": "*",
      "NAMES": "*",
      "PATHS": "*",
      "PATH_SUFFIXES": "*"
    }
  },
  "find_library": {
    "flags": [
      "CMAKE_FIND_ROOT_PATH_BOTH",
      "NO_CMAKE_ENVIRONMENT_PATH",
      "NO_CMAKE_FIND_ROOT_PATH",
      "NO_CMAKE_PATH",
      "NO_CMKE_SYSTEM_PATH",
      "NO_DEFAULT_PATH",
      "NO_SYSTEM_ENVIRONMENT_PATH",
      "ONLY_CMAKE_FIND_ROOT_PATH"
    ],
    "kwargs": {
      "DOC": "*",
      "HINTS": "*",
      "NAMES": "*",
      "PATHS": "*",
      "PATH_SUFFIXES": "*"
    }
  },
  "find_package": {
    "pargs": "*",
    "flags": [
      "EXACT",
      "MODULE",
      "REQUIRED",
      "NO_POLICY_SCOPE",
      "QUIET"
    ],
    "kwargs": {
      "COMPONENTS": "*",
      "OPTIONAL_COMPONENTS": "*"
    }
  },
  "find_path": {
    "flags": [
      "CMAKE_FIND_ROOT_PATH_BOTH",
      "NO_CMAKE_ENVIRONMENT_PATH",
      "NO_CMAKE_FIND_ROOT_PATH",
      "NO_CMAKE_PATH",
      "NO_CMKE_SYSTEM_PATH",
      "NO_DEFAULT_PATH",
      "NO_SYSTEM_ENVIRONMENT_PATH",
      "ONLY_CMAKE_FIND_ROOT_PATH"
    ],
    "kwargs": {
      "DOC": "*",
      "HINTS": "*",
      "NAMES": "*",
      "PATHS": "*",
      "PATH_SUFFIXES": "*"
    }
  },
  "find_program": {
    "flags": [
      "CMAKE_FIND_ROOT_PATH_BOTH",
      "NO_CMAKE_ENVIRONMENT_PATH",
      "NO_CMAKE_FIND_ROOT_PATH",
      "NO_CMAKE_PATH",
      "NO_CMAKE_SYSTEM_PATH",
      "NO_DEFAULT_PATH",
      "NO_SYSTEM_ENVIRONMENT_PATH",
      "ONLY_CMAKE_FIND_ROOT_PATH"
    ],
    "kwargs": {
      "DOC": "*",
      "HINTS": "*",
      "NAMES": "*",
      "PATHS": "*",
      "PATH_SUFFIXES": "*"
    }
  },
  "get_directory_property": {
    "kwargs": {
      "DIRECTORY": "*"
    }
  },
  "get_property": {
    "flags": [
      "BRIEF_DOCS",
      "DEFINED",
      "FULL_DOCS",
      "GLOBAL",
      "SET",
      "VARIABLE"
    ],
    "kwargs": {
      "CACHE": "*",
      "DIRECTORY": "*",
      "PROPERTY": "*",
      "SOURCE": "*",
      "TARGET": "*",
      "TEST": "*"
    }
  },
  "include": {
    "flags": [
      "NO_POLICY_SCOPE",
      "OPTIONAL"
    ],
    "kwargs": {
      "RESULT_VARIABLE": "*"
    }
  },
  "include_directories": {
    "flags": [
      "AFTER",
      "BEFORE",
      "SYSTEM"
    ],
    "kwargs": {}
  },
  "mark_as_advanced": {
    "flags": [
      "CLEAR",
      "FORCE"
    ],
    "kwargs": {}
  },
  "message": {
    "kwargs": {
      "AUTHOR_WARNING": "*",
      "DEPRECATION": "*",
      "FATAL_ERROR": "*",
      "SEND_ERROR": "*",
      "STATUS": "*",
      "WARNING": "*"
    }
  },
  "project": {
    "kwargs": {
      "DESCRIPTION": 1,
      "HOMEPAGE_URL": 1,
      "LANGUAGES": "*",
      "VERSION": "*"
    }
  },
  "set_directory_properties": {
    "kwargs": {
      "PROPERTIES": "*"
    }
  },
  "set_property": {
    "flags": [
      "APPEND",
      "APPEND_STRING",
      "GLOBAL"
    ],
    "kwargs": {
      "CACHE": "*",
      "DIRECTORY": "*",
      "PROPERTY": "*",
      "SOURCE": "*",
      "TARGET": "*",
      "TEST": "*"
    }
  },
  "set_tests_properties": {
    "kwargs": {
      "PROPERTIES": "*"
    }
  },
  "string": {
    "flags": [
      "@ONLY",
      "ESCAPE_QUOTES",
      "REVERSE",
      "UTC"
    ],
    "kwargs": {
      "ALPHABET": "*",
      "ASCII": "*",
      "COMPARE": "*",
      "CONCAT": "*",
      "CONFIGURE": "*",
      "FIND": "*",
      "LENGTH": "*",
      "MAKE_C_IDENTIFIER": "*",
      "MD5": "*",
      "RANDOM": "*",
      "RANDOM_SEED": "*",
      "REGEX": "*",
      "REPLACE": "*",
      "SHA1": "*",
      "SHA256": "*",
      "SHA384": "*",
      "SHA512": "*",
      "STRIP": "*",
      "SUBSTRING": "*",
      "TIMESTAMP": "*",
      "TOLOWER": "*",
      "TOUPPER": "*"
    }
  },
  "try_compile": {
    "pargs": [2, "+"],
    "kwargs": {
      "CMAKE_FLAGS": "*",
      "COMPILE_DEFINITIONS": "*",
      "COPY_FILE": "*",
      "LINK_OPTIONS": "*",
      "LINK_LIBRARIES": "*",
      "OUTPUT_VARIABLE": "*",
      "RESULT_VAR": "*",
      "SOURCES": "*"
    }
  },
  "target_compile_options": {
    "kwargs": {
      "PRIVATE": "+",
      "PUBLIC": "+",
      "INTERFACE": "+"
    }
  },
  "target_include_directories": {
    "kwargs": {
      "PRIVATE": "+",
      "PUBLIC": "+",
      "INTERFACE": "+"
    }
  },
  "target_link_libraries": {
    "kwargs": {
      "PRIVATE": "+",
      "PUBLIC": "+",
      "INTERFACE": "+"
    }
  },
  "try_run": {
    "kwargs": {
      "ARGS": "*",
      "CMAKE_FLAGS": "*",
      "COMPILE_DEFINITIONS": "*",
      "COMPILE_OUTPUT_VARIABLE": "*",
      "OUTPUT_VARIABLE": "*",
      "RUN_OUTPUT_VARIABLE": "*"
    }
  }
}
