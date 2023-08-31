include(FindPackageHandleStandardArgs)

find_library(ANTLR4_RUNTIME_LIBRARY antlr4-runtime)
find_path(
  ANTLR4_RUNTIME_INCLUDE_DIR
  NAMES antlr4-runtime.h
  PATH_SUFFIXES antlr4-runtime)

find_package_handle_standard_args(
  antlr4-runtime REQUIRED_VARS ANTLR4_RUNTIME_LIBRARY
                               ANTLR4_RUNTIME_INCLUDE_DIR)

set(ANTLR4_RUNTIME_LIBRARIES ${ANTLR4_RUNTIME_LIBRARY})
set(ANTLR4_RUNTIME_INCLUDE_DIRS ${ANTLR4_RUNTIME_INCLUDE_DIR})

if(NOT TARGET antlr4-runtime::antlr4-runtime)
  add_library(antlr4-runtime::antlr4-runtime UNKNOWN IMPORTED)
  set_target_properties(
    antlr4-runtime::antlr4-runtime PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                              "${ANTLR4_RUNTIME_INCLUDE_DIRS}")
  set_target_properties(
    antlr4-runtime::antlr4-runtime
    PROPERTIES IMPORTED_LINK_INTERFACE_LANGUAGES "C"
               IMPORTED_LOCATION "${ANTLR4_RUNTIME_LIBRARIES}")
endif()
