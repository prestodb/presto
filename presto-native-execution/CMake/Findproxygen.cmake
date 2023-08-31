include(FindPackageHandleStandardArgs)

find_library(PROXYGEN_LIBRARY proxygen)
find_library(PROXYGEN_HTTP_SERVER_LIBRARY proxygenhttpserver)

find_path(PROXYGEN_INCLUDE_DIR NAMES include/proxygen)

find_package_handle_standard_args(
  proxygen REQUIRED_VARS PROXYGEN_LIBRARY PROXYGEN_HTTP_SERVER_LIBRARY
                         PROXYGEN_INCLUDE_DIR)

set(PROXYGEN_LIBRARIES ${PROXYGEN_HTTP_SERVER_LIBRARY} ${PROXYGEN_LIBRARY})
set(PROXYGEN_INCLUDE_DIRS ${PROXYGEN_INCLUDE_DIR})

if(NOT TARGET proxygen::proxygen)
  add_library(proxygen::proxygen UNKNOWN IMPORTED)
  set_target_properties(
    proxygen::proxygen PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                  "${PROXYGEN_INCLUDE_DIRS}")
  set_target_properties(
    proxygen::proxygen PROPERTIES IMPORTED_LINK_INTERFACE_LANGUAGES "C"
                                  IMPORTED_LOCATION "${PROXYGEN_LIBRARY}")
endif()

if(NOT TARGET proxygen::proxygenhttpserver)
  add_library(proxygen::proxygenhttpserver UNKNOWN IMPORTED)
  set_target_properties(
    proxygen::proxygenhttpserver PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                            "${PROXYGEN_INCLUDE_DIRS}")
  set_target_properties(
    proxygen::proxygenhttpserver
    PROPERTIES IMPORTED_LINK_INTERFACE_LANGUAGES "C"
               IMPORTED_LOCATION "${PROXYGEN_HTTP_SERVER_LIBRARY}")
endif()
