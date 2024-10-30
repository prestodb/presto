add_library(presto_magen QaagFunctionsRegistration.cpp)

target_link_libraries(presto_magen magen_cpp xsimd ${GFLAGS_LIBRARIES})
