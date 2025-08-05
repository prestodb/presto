if(PRESTO_ENABLE_DATASKETCHES)
  add_executable(presto_server_sketch_functions_test
                 ThetaSketchAggregationTest.cpp)

  add_test(
    NAME presto_server_sketch_functions_test
    COMMAND presto_server_sketch_functions_test
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  target_link_libraries(
    presto_server_sketch_functions_test
    presto_server_lib
    velox_exec
    velox_exec_test_lib
    velox_expression
    velox_functions_aggregates_test_lib
    velox_temp_path
    GTest::gmock
    GTest::gtest
    GTest::gtest_main)
endif()
