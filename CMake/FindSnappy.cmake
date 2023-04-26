# Copyright (c) Facebook, Inc. and its affiliates.
# - Try to find snappy
# Once done, this will define
#
# SNAPPY_FOUND - system has Glog
# SNAPPY_INCLUDE_DIRS - deprecated
# SNAPPY_LIBRARIES -  deprecated
# Snappy::snappy will be defined based on CMAKE_FIND_LIBRARY_SUFFIXES priority

include(FindPackageHandleStandardArgs)
include(SelectLibraryConfigurations)

find_library(SNAPPY_LIBRARY_RELEASE snappy PATHS $SNAPPY_LIBRARYDIR})
find_library(SNAPPY_LIBRARY_DEBUG snappyd PATHS ${SNAPPY_LIBRARYDIR})

find_path(SNAPPY_INCLUDE_DIR snappy.h PATHS ${SNAPPY_INCLUDEDIR})

select_library_configurations(SNAPPY)

find_package_handle_standard_args(Snappy DEFAULT_MSG SNAPPY_LIBRARY
                                  SNAPPY_INCLUDE_DIR)

mark_as_advanced(SNAPPY_LIBRARY SNAPPY_INCLUDE_DIR)

get_filename_component(libsnappy_ext ${SNAPPY_LIBRARY} EXT)
if(libsnappy_ext STREQUAL ".a")
  set(libsnappy_type STATIC)
else()
  set(libsnappy_type SHARED)
endif()

if(NOT TARGET Snappy::snappy)
  add_library(Snappy::snappy ${libsnappy_type} IMPORTED)
  set_target_properties(Snappy::snappy PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                                  "${SNAPPY_INCLUDE_DIR}")
  set_target_properties(
    Snappy::snappy PROPERTIES IMPORTED_LINK_INTERFACE_LANGUAGES "C"
                              IMPORTED_LOCATION "${SNAPPY_LIBRARIES}")
endif()
