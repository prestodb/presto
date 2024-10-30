set(MAGEN_PATH "/usr/local/lib")

add_library(magen_cpp SHARED IMPORTED GLOBAL)

if(${CMAKE_SYSTEM_NAME} MATCHES Darwin)
  set_property(TARGET magen_cpp PROPERTY IMPORTED_LOCATION
                                         ${MAGEN_PATH}/libmagen.dylib)
elseif(${CMAKE_SYSTEM_NAME} MATCHES Linux)
  set_property(TARGET magen_cpp PROPERTY IMPORTED_LOCATION
                                         ${MAGEN_PATH}/libmagen.so)
endif()
