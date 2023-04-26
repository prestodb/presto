# Copyright (c) Facebook, Inc. and its affiliates.
# - Try to find lz4
# Once done, this will define
#
# LZ4_FOUND - system has Glog
# LZ4_INCLUDE_DIRS - deprecated
# LZ4_LIBRARIES -  deprecated
# lz4::lz4 will be defined based on CMAKE_FIND_LIBRARY_SUFFIXES priority

include(FindPackageHandleStandardArgs)
include(SelectLibraryConfigurations)

find_library(LZ4_LIBRARY_RELEASE lz4 PATHS $LZ4_LIBRARYDIR})
find_library(LZ4_LIBRARY_DEBUG lz4d PATHS ${LZ4_LIBRARYDIR})

find_path(LZ4_INCLUDE_DIR lz4.h PATHS ${LZ4_INCLUDEDIR})

select_library_configurations(LZ4)

find_package_handle_standard_args(lz4 DEFAULT_MSG LZ4_LIBRARY LZ4_INCLUDE_DIR)

mark_as_advanced(LZ4_LIBRARY LZ4_INCLUDE_DIR)

get_filename_component(liblz4_ext ${LZ4_LIBRARY} EXT)
if(liblz4_ext STREQUAL ".a")
  set(liblz4_type STATIC)
else()
  set(liblz4_type SHARED)
endif()

if(NOT TARGET lz4::lz4)
  add_library(lz4::lz4 ${liblz4_type} IMPORTED)
  set_target_properties(lz4::lz4 PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                            "${LZ4_INCLUDE_DIR}")
  set_target_properties(
    lz4::lz4 PROPERTIES IMPORTED_LINK_INTERFACE_LANGUAGES "C"
                        IMPORTED_LOCATION "${LZ4_LIBRARIES}")
endif()
