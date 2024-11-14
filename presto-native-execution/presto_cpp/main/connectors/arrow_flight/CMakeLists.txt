# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
find_package(Arrow REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(ARROW_FLIGHT REQUIRED arrow-flight)

if(ARROW_FLIGHT_FOUND)
  set(ArrowFlight_FOUND TRUE)
  set(ArrowFlight_INCLUDE_DIRS ${ARROW_FLIGHT_INCLUDE_DIRS})
  set(ArrowFlight_LIBRARIES ${ARROW_FLIGHT_LIBRARIES})

  include_directories(${ArrowFlight_INCLUDE_DIRS})
else()
  set(ArrowFlight_FOUND FALSE)
  message(FATAL_ERROR "Arrow Flight package not found")
endif()

add_library(presto_flight_connector_utils INTERFACE Macros.h)
target_link_libraries(presto_flight_connector_utils INTERFACE velox_exception)

add_subdirectory(auth)

add_library(presto_flight_connector OBJECT ArrowFlightConnector.cpp
                                           FlightConfig.cpp)

target_compile_definitions(presto_flight_connector
                           PUBLIC PRESTO_ENABLE_ARROW_FLIGHT_CONNECTOR)

target_link_libraries(
  presto_flight_connector velox_connector arrow ${ArrowFlight_LIBRARIES}
  presto_flight_connector_utils presto_flight_connector_auth)

if(PRESTO_ENABLE_TESTING)
  add_subdirectory(tests)
endif()
