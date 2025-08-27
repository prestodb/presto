# Copyright (C) 2022 The Qt Company Ltd.
# SPDX-License-Identifier: BSD-3-Clause

# Fallback find module for double-conversion if double-conversion is built with
# CMake it'll install a config module, which we prefer if it's built with Scons
# (their default), we search ourselves

find_package(double-conversion CONFIG)
if(double-conversion_FOUND)
  if(TARGET double-conversion::double-conversion)
    return()
  endif()
endif()

find_path(DOUBLE_CONVERSION_INCLUDE_DIR NAMES double-conversion.h PATH_SUFFIXES double-conversion)
find_library(DOUBLE_CONVERSION_LIBRARY NAMES double-conversion)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  double-conversion
  DEFAULT_MSG
  DOUBLE_CONVERSION_LIBRARY
  DOUBLE_CONVERSION_INCLUDE_DIR
)

if(double-conversion_FOUND AND NOT TARGET double-conversion::double-conversion)
  add_library(double-conversion::double-conversion UNKNOWN IMPORTED)
  set_target_properties(
    double-conversion::double-conversion
    PROPERTIES
      IMPORTED_LOCATION "${DOUBLE_CONVERSION_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${DOUBLE_CONVERSION_INCLUDE_DIR}"
  )
endif()

mark_as_advanced(DOUBLE_CONVERSION_INCLUDE_DIR DOUBLE_CONVERSION_LIBRARY)
