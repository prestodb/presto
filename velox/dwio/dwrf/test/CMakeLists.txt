# Copyright (c) Facebook, Inc. and its affiliates.
#
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

add_subdirectory(utils)

set(TEST_LINK_LIBS
    velox_dwio_common_test_utils
    velox_dwio_dwrf_proto
    velox_dwio_dwrf_reader
    velox_dwio_dwrf_writer
    gflags::gflags
    gtest
    gtest_main
    gmock
    glog::glog
    ${FILESYSTEM})

add_executable(velox_dwio_dwrf_buffered_output_stream_test
               TestBufferedOutputStream.cpp)
add_test(velox_dwio_dwrf_buffered_output_stream_test
         velox_dwio_dwrf_buffered_output_stream_test)

target_link_libraries(
  velox_dwio_dwrf_buffered_output_stream_test velox_link_libs Folly::folly
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_column_statistics_test
               TestDwrfColumnStatistics.cpp)
add_test(velox_dwio_dwrf_column_statistics_test
         velox_dwio_dwrf_column_statistics_test)

target_link_libraries(
  velox_dwio_dwrf_column_statistics_test velox_link_libs Folly::folly
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_orc_column_statistics_test
               TestOrcColumnStatistics.cpp)
add_test(velox_dwio_orc_column_statistics_test
         velox_dwio_orc_column_statistics_test)

target_link_libraries(
  velox_dwio_orc_column_statistics_test velox_link_libs Folly::folly
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_compression_test CompressionTest.cpp)
add_test(velox_dwio_dwrf_compression_test velox_dwio_dwrf_compression_test)

target_link_libraries(
  velox_dwio_dwrf_compression_test
  velox_link_libs
  Folly::folly
  glog::glog
  lz4::lz4
  lzo2::lzo2
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_decompression_test TestDecompression.cpp)
add_test(
  NAME velox_dwio_dwrf_decompression_test
  COMMAND velox_dwio_dwrf_decompression_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(
  velox_dwio_dwrf_decompression_test
  velox_link_libs
  Folly::folly
  lz4::lz4
  lzo2::lzo2
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_stripe_stream_test TestStripeStream.cpp)
add_test(velox_dwio_dwrf_stripe_stream_test velox_dwio_dwrf_stripe_stream_test)

target_link_libraries(
  velox_dwio_dwrf_stripe_stream_test
  velox_link_libs
  Folly::folly
  fmt::fmt
  lz4::lz4
  lzo2::lzo2
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_stream_labels_test StreamLabelsTests.cpp)
add_test(velox_dwio_dwrf_stream_labels_test velox_dwio_dwrf_stream_labels_test)

target_link_libraries(
  velox_dwio_dwrf_stream_labels_test
  velox_link_libs
  Folly::folly
  fmt::fmt
  lz4::lz4
  lzo2::lzo2
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_stripe_dictionary_cache_test
               TestStripeDictionaryCache.cpp)
add_test(velox_dwio_dwrf_stripe_dictionary_cache_test
         velox_dwio_dwrf_stripe_dictionary_cache_test)

target_link_libraries(
  velox_dwio_dwrf_stripe_dictionary_cache_test
  velox_link_libs
  Folly::folly
  fmt::fmt
  lz4::lz4
  lzo2::lzo2
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_dictionary_encoder_test
               TestIntegerDictionaryEncoder.cpp TestStringDictionaryEncoder.cpp)
add_test(velox_dwio_dwrf_dictionary_encoder_test
         velox_dwio_dwrf_dictionary_encoder_test)

target_link_libraries(
  velox_dwio_dwrf_dictionary_encoder_test velox_link_libs Folly::folly
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_encoding_selector_test TestEncodingSelector.cpp)
add_test(velox_dwio_dwrf_encoding_selector_test
         velox_dwio_dwrf_encoding_selector_test)

target_link_libraries(
  velox_dwio_dwrf_encoding_selector_test velox_link_libs Folly::folly
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_index_builder_test IndexBuilderTests.cpp)
add_test(velox_dwio_dwrf_index_builder_test velox_dwio_dwrf_index_builder_test)

target_link_libraries(
  velox_dwio_dwrf_index_builder_test velox_link_libs Folly::folly
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_dictionary_encoding_utils_test
               TestDictionaryEncodingUtils.cpp)
add_test(velox_dwio_dwrf_dictionary_encoding_utils_test
         velox_dwio_dwrf_dictionary_encoding_utils_test)

target_link_libraries(
  velox_dwio_dwrf_dictionary_encoding_utils_test velox_link_libs Folly::folly
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_checksum_test ChecksumTests.cpp)
add_test(velox_dwio_dwrf_checksum_test velox_dwio_dwrf_checksum_test)

target_link_libraries(
  velox_dwio_dwrf_checksum_test
  velox_link_libs
  Folly::folly
  Boost::headers
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_writer_test WriterTest.cpp)
add_test(velox_dwio_dwrf_writer_test velox_dwio_dwrf_writer_test)

target_link_libraries(
  velox_dwio_dwrf_writer_test
  velox_link_libs
  Folly::folly
  lz4::lz4
  lzo2::lzo2
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_writer_context_test WriterContextTest.cpp)
add_test(velox_dwio_dwrf_writer_context_test
         velox_dwio_dwrf_writer_context_test)

target_link_libraries(
  velox_dwio_dwrf_writer_context_test
  velox_link_libs
  Folly::folly
  lz4::lz4
  lzo2::lzo2
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_writer_encoding_manager_test
               EncodingManagerTests.cpp)
add_test(velox_dwio_dwrf_writer_encoding_manager_test
         velox_dwio_dwrf_writer_encoding_manager_test)

target_link_libraries(
  velox_dwio_dwrf_writer_encoding_manager_test
  velox_link_libs
  Folly::folly
  lz4::lz4
  lzo2::lzo2
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_writer_sink_test WriterSinkTest.cpp)
add_test(velox_dwio_dwrf_writer_sink_test velox_dwio_dwrf_writer_sink_test)

target_link_libraries(
  velox_dwio_dwrf_writer_sink_test velox_link_libs Folly::folly
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_data_buffer_holder_test
               DataBufferHolderTests.cpp)
add_test(velox_dwio_dwrf_data_buffer_holder_test
         velox_dwio_dwrf_data_buffer_holder_test)

target_link_libraries(
  velox_dwio_dwrf_data_buffer_holder_test velox_link_libs Folly::folly
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_layout_planner_test LayoutPlannerTests.cpp)
add_test(velox_dwio_dwrf_layout_planner_test
         velox_dwio_dwrf_layout_planner_test)

target_link_libraries(
  velox_dwio_dwrf_layout_planner_test
  velox_link_libs
  Folly::folly
  lz4::lz4
  lzo2::lzo2
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_decryption_test DecryptionTests.cpp)
add_test(velox_dwio_dwrf_decryption_test velox_dwio_dwrf_decryption_test)

target_link_libraries(
  velox_dwio_dwrf_decryption_test velox_link_libs Folly::folly
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_encryption_test EncryptionTests.cpp)
add_test(velox_dwio_dwrf_encryption_test velox_dwio_dwrf_encryption_test)

target_link_libraries(
  velox_dwio_dwrf_encryption_test velox_link_libs Folly::folly
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_stripe_reader_base_test
               StripeReaderBaseTests.cpp)
add_test(velox_dwio_dwrf_stripe_reader_base_test
         velox_dwio_dwrf_stripe_reader_base_test)

target_link_libraries(
  velox_dwio_dwrf_stripe_reader_base_test
  velox_link_libs
  Folly::folly
  lz4::lz4
  lzo2::lzo2
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_reader_base_test ReaderBaseTests.cpp)
add_test(velox_dwio_dwrf_reader_base_test velox_dwio_dwrf_reader_base_test)

target_link_libraries(
  velox_dwio_dwrf_reader_base_test
  velox_link_libs
  Folly::folly
  glog::glog
  lz4::lz4
  lzo2::lzo2
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_config_test ConfigTests.cpp)
add_test(velox_dwio_dwrf_config_test velox_dwio_dwrf_config_test)

target_link_libraries(
  velox_dwio_dwrf_config_test velox_link_libs Folly::folly ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_ratio_checker_test RatioTrackerTest.cpp)
add_test(velox_dwio_dwrf_ratio_checker_test velox_dwio_dwrf_ratio_checker_test)

target_link_libraries(
  velox_dwio_dwrf_ratio_checker_test velox_link_libs Folly::folly
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_flush_policy_test FlushPolicyTest.cpp)
add_test(velox_dwio_dwrf_flush_policy_test velox_dwio_dwrf_flush_policy_test)

target_link_libraries(
  velox_dwio_dwrf_flush_policy_test velox_link_libs Folly::folly
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_byte_rle_test TestByteRle.cpp)
add_test(velox_dwio_dwrf_byte_rle_test velox_dwio_dwrf_byte_rle_test)

target_link_libraries(
  velox_dwio_dwrf_byte_rle_test
  velox_link_libs
  fmt::fmt
  Folly::folly
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_byte_rle_encoder_test TestByteRLEEncoder.cpp)
add_test(velox_dwio_dwrf_byte_rle_encoder_test
         velox_dwio_dwrf_byte_rle_encoder_test)

target_link_libraries(
  velox_dwio_dwrf_byte_rle_encoder_test
  velox_link_libs
  fmt::fmt
  Folly::folly
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_int_encoder_test TestIntEncoder.cpp)
add_test(velox_dwio_dwrf_int_encoder_test velox_dwio_dwrf_int_encoder_test)

target_link_libraries(
  velox_dwio_dwrf_int_encoder_test velox_link_libs Folly::folly
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_rle_test TestRle.cpp)
add_test(velox_dwio_dwrf_rle_test velox_dwio_dwrf_rle_test)

target_link_libraries(
  velox_dwio_dwrf_rle_test velox_link_libs Folly::folly ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_int_direct_test TestIntDirect.cpp)
add_test(velox_dwio_dwrf_int_direct_test velox_dwio_dwrf_int_direct_test)

target_link_libraries(
  velox_dwio_dwrf_int_direct_test
  velox_link_libs
  fmt::fmt
  Folly::folly
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_rlev1_encoder_test TestRLEv1Encoder.cpp)
add_test(velox_dwio_dwrf_rlev1_encoder_test velox_dwio_dwrf_rlev1_encoder_test)

target_link_libraries(
  velox_dwio_dwrf_rlev1_encoder_test velox_link_libs Folly::folly
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_column_reader_test TestColumnReader.cpp)
add_test(velox_dwio_dwrf_column_reader_test velox_dwio_dwrf_column_reader_test)

target_link_libraries(
  velox_dwio_dwrf_column_reader_test
  velox_link_libs
  Folly::folly
  fmt::fmt
  ${TEST_LINK_LIBS})

add_executable(velox_dwio_dwrf_reader_test ReaderTest.cpp)
add_test(
  NAME velox_dwio_dwrf_reader_test
  COMMAND velox_dwio_dwrf_reader_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(
  velox_dwio_dwrf_reader_test
  velox_dwrf_test_utils
  velox_vector_test_lib
  velox_link_libs
  Folly::folly
  fmt::fmt
  lz4::lz4
  lzo2::lzo2
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS})

add_executable(velox_dwrf_e2e_filter_test E2EFilterTest.cpp)
add_test(velox_dwrf_e2e_filter_test velox_dwrf_e2e_filter_test)

target_link_libraries(
  velox_dwrf_e2e_filter_test
  velox_dwio_common_test_utils
  velox_link_libs
  velox_test_util
  Folly::folly
  fmt::fmt
  lz4::lz4
  lzo2::lzo2
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS})

add_executable(velox_dwrf_statistics_builder_utils_test
               TestStatisticsBuilderUtils.cpp)
add_test(velox_dwrf_statistics_builder_utils_test
         velox_dwrf_statistics_builder_utils_test)

target_link_libraries(
  velox_dwrf_statistics_builder_utils_test velox_link_libs Folly::folly
  ${TEST_LINK_LIBS})

add_executable(velox_dwrf_column_writer_test ColumnWriterTest.cpp)
add_test(velox_dwrf_column_writer_test velox_dwrf_column_writer_test)

target_link_libraries(
  velox_dwrf_column_writer_test
  velox_dwrf_test_utils
  velox_vector_test_lib
  velox_link_libs
  Folly::folly
  fmt::fmt
  lz4::lz4
  lzo2::lzo2
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS})

add_executable(velox_dwrf_column_writer_index_test ColumnWriterIndexTest.cpp)
add_test(velox_dwrf_column_writer_index_test
         velox_dwrf_column_writer_index_test)

target_link_libraries(
  velox_dwrf_column_writer_index_test
  velox_link_libs
  velox_dwrf_test_utils
  Folly::folly
  fmt::fmt
  lz4::lz4
  lzo2::lzo2
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS})

add_executable(velox_dwrf_writer_flush_test WriterFlushTest.cpp)
add_test(velox_dwrf_writer_flush_test velox_dwrf_writer_flush_test)

target_link_libraries(
  velox_dwrf_writer_flush_test
  velox_dwrf_test_utils
  velox_link_libs
  Folly::folly
  fmt::fmt
  lz4::lz4
  lzo2::lzo2
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS})

add_executable(velox_dwrf_e2e_writer_test E2EWriterTest.cpp)
add_test(velox_dwrf_e2e_writer_test velox_dwrf_e2e_writer_test)

target_link_libraries(
  velox_dwrf_e2e_writer_test
  velox_dwrf_test_utils
  velox_vector_fuzzer
  velox_vector_test_lib
  velox_link_libs
  Folly::folly
  fmt::fmt
  lz4::lz4
  lzo2::lzo2
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS})

add_executable(velox_dwrf_e2e_reader_test E2EReaderTest.cpp)
add_test(velox_dwrf_e2e_reader_test velox_dwrf_e2e_reader_test)

target_link_libraries(
  velox_dwrf_e2e_reader_test
  velox_dwrf_test_utils
  velox_vector_fuzzer
  velox_vector_test_lib
  velox_link_libs
  Folly::folly
  fmt::fmt
  lz4::lz4
  lzo2::lzo2
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS})

add_executable(velox_dwrf_writer_extended_test WriterExtendedTests.cpp)

target_link_libraries(
  velox_dwrf_writer_extended_test
  velox_dwrf_test_utils
  velox_link_libs
  Folly::folly
  fmt::fmt
  lz4::lz4
  lzo2::lzo2
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS})

add_executable(velox_dwrf_column_writer_stats_test ColumnWriterStatsTests.cpp)
add_test(velox_dwrf_column_writer_stats_test
         velox_dwrf_column_writer_stats_test)

target_link_libraries(
  velox_dwrf_column_writer_stats_test
  velox_dwrf_test_utils
  velox_link_libs
  Folly::folly
  fmt::fmt
  lz4::lz4
  lzo2::lzo2
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS})

add_executable(physical_size_aggregator_test PhysicalSizeAggregatorTest.cpp)
add_test(physical_size_aggregator_test physical_size_aggregator_test)

target_link_libraries(
  physical_size_aggregator_test
  velox_dwrf_test_utils
  velox_link_libs
  Folly::folly
  fmt::fmt
  lz4::lz4
  lzo2::lzo2
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS})

add_executable(velox_dwrf_int_encoder_benchmark IntEncoderBenchmark.cpp)
target_link_libraries(
  velox_dwrf_int_encoder_benchmark
  velox_dwio_dwrf_common
  velox_memory
  velox_dwio_common_exception
  Folly::folly
  ${FOLLY_BENCHMARK})

add_executable(velox_dwrf_float_column_writer_benchmark
               FloatColumnWriterBenchmark.cpp)
target_link_libraries(
  velox_dwrf_float_column_writer_benchmark
  velox_vector
  velox_dwio_common_exception
  velox_dwio_dwrf_writer
  Folly::folly
  ${FOLLY_BENCHMARK}
  fmt::fmt)

add_executable(velox_dwio_cache_test CacheInputTest.cpp
                                     DirectBufferedInputTest.cpp)

add_test(velox_dwio_cache_test velox_dwio_cache_test)

target_link_libraries(
  velox_dwio_cache_test
  velox_common_io
  velox_link_libs
  velox_temp_path
  Folly::folly
  fmt::fmt
  lz4::lz4
  lzo2::lzo2
  zstd::zstd
  ZLIB::ZLIB
  ${TEST_LINK_LIBS})
