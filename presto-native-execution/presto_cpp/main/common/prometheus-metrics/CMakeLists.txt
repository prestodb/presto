find_package(prometheus-cpp CONFIG REQUIRED)
add_library(prometheus_reporter PrometheusReporter.cpp PrometheusReporter.h)
target_link_libraries(prometheus_reporter presto_common prometheus-cpp::core)
set_property(TARGET prometheus_reporter PROPERTY JOB_POOL_LINK
                                                 presto_link_job_pool)
add_executable(prometheus_reporter_test PrometheusReporterTest.cpp)
target_link_libraries(prometheus_reporter_test presto_server_lib
                      velox_exec_test_lib prometheus_reporter gtest gtest_main)
