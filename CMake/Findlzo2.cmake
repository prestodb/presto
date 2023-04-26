# Copyright (c) Facebook, Inc. and its affiliates.
# - Try to find lzo2
# Once done, this will define
#
# LZO2_FOUND - system has Glog
# LZO2_INCLUDE_DIRS - deprecated
# LZO2_LIBRARIES -  deprecated
# lzo2::lzo2 will be defined based on CMAKE_FIND_LIBRARY_SUFFIXES priority

include(FindPackageHandleStandardArgs)
include(SelectLibraryConfigurations)

find_library(LZO2_LIBRARY_RELEASE lzo2 PATHS $LZO2_LIBRARYDIR})
find_library(LZO2_LIBRARY_DEBUG lzo2d PATHS ${LZO2_LIBRARYDIR})

find_path(LZO2_INCLUDE_DIR lzo/lzo1a.h PATHS ${LZO2_INCLUDEDIR})

select_library_configurations(LZO2)

find_package_handle_standard_args(lzo2 DEFAULT_MSG LZO2_LIBRARY
                                  LZO2_INCLUDE_DIR)

mark_as_advanced(LZO2_LIBRARY LZO2_INCLUDE_DIR)

get_filename_component(liblzo2_ext ${LZO2_LIBRARY} EXT)
if(liblzo2_ext STREQUAL ".a")
  set(liblzo2_type STATIC)
else()
  set(liblzo2_type SHARED)
endif()

if(NOT TARGET lzo2::lzo2)
  add_library(lzo2::lzo2 ${liblzo2_type} IMPORTED)
  set_target_properties(lzo2::lzo2 PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                              "${LZO2_INCLUDE_DIR}")
  set_target_properties(
    lzo2::lzo2 PROPERTIES IMPORTED_LINK_INTERFACE_LANGUAGES "C"
                          IMPORTED_LOCATION "${LZO2_LIBRARIES}")
endif()
