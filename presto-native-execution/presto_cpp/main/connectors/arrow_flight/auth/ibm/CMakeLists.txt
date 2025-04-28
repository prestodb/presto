add_library(
  presto_flight_connector_ibm_auth CpdAuthenticator.cpp SaasAuthenticator.cpp
                                   SaasAuthenticatorConfig.cpp)

target_link_libraries(presto_flight_connector_ibm_auth
                      presto_flight_connector_auth cpr::cpr)

if(PRESTO_ENABLE_TESTING)
  add_subdirectory(tests)
endif()
