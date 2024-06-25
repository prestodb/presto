/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sessionpropertyproviders;

import com.facebook.presto.Session;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.session.SystemSessionPropertyProvider;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.longProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;

public class BuiltInNativeSystemSessionPropertyProvider
        implements SystemSessionPropertyProvider
{
    public static final String DRIVER_CPU_TIME_SLICE_LIMIT = "driver_cpu_time_slice_limit_ms";
    public static final String LEGACY_TIMESTAMP = "legacy_timestamp";
    public static final String NATIVE_AGGREGATION_SPILL_MEMORY_THRESHOLD = "native_aggregation_spill_memory_threshold";
    public static final String NATIVE_DEBUG_VALIDATE_OUTPUT_FROM_OPERATORS = "native_debug_validate_output_from_operators";
    public static final String NATIVE_JOIN_SPILL_ENABLED = "native_join_spill_enabled";
    public static final String NATIVE_JOIN_SPILL_MEMORY_THRESHOLD = "native_join_spill_memory_threshold";
    public static final String NATIVE_JOIN_SPILLER_PARTITION_BITS = "native_join_spiller_partition_bits";
    public static final String NATIVE_MAX_SPILL_FILE_SIZE = "native_max_spill_file_size";
    public static final String NATIVE_MAX_SPILL_LEVEL = "native_max_spill_level";
    public static final String NATIVE_ORDER_BY_SPILL_MEMORY_THRESHOLD = "native_order_by_spill_memory_threshold";
    public static final String NATIVE_ROW_NUMBER_SPILL_ENABLED = "native_row_number_spill_enabled";
    public static final String NATIVE_SIMPLIFIED_EXPRESSION_EVALUATION_ENABLED = "native_simplified_expression_evaluation_enabled";
    public static final String NATIVE_SPILL_COMPRESSION_CODEC = "native_spill_compression_codec";
    public static final String NATIVE_SPILL_FILE_CREATE_CONFIG = "native_spill_file_create_config";
    public static final String NATIVE_SPILL_WRITE_BUFFER_SIZE = "native_spill_write_buffer_size";
    public static final String NATIVE_TOPN_ROW_NUMBER_SPILL_ENABLED = "native_topn_row_number_spill_enabled";
    public static final String NATIVE_WINDOW_SPILL_ENABLED = "native_window_spill_enabled";
    public static final String NATIVE_WRITER_SPILL_ENABLED = "native_writer_spill_enabled";
    private final List<PropertyMetadata<?>> sessionProperties;

    public BuiltInNativeSystemSessionPropertyProvider()
    {
        sessionProperties = ImmutableList.of(
                integerProperty(
                        DRIVER_CPU_TIME_SLICE_LIMIT,
                        "Native Execution only. The cpu time slice limit in ms that a driver thread.If not zero, can continuously run without yielding. If it is zero,then there is no limit.",
                        1000,
                        false),
                booleanProperty(
                        LEGACY_TIMESTAMP,
                        "Use legacy TIME & TIMESTAMP semantics (warning: this will be removed)",
                        true,
                        false),
                integerProperty(
                        NATIVE_AGGREGATION_SPILL_MEMORY_THRESHOLD,
                        "Native Execution only. The max memory that a final aggregation can use before spilling. If it is 0, then there is no limit",
                        0,
                        false),
                booleanProperty(
                        NATIVE_DEBUG_VALIDATE_OUTPUT_FROM_OPERATORS,
                        "If set to true, then during execution of tasks, the output vectors of " +
                                "every operator are validated for consistency. This is an expensive check " +
                                "so should only be used for debugging. It can help debug issues where " +
                                "malformed vector cause failures or crashes by helping identify which " +
                                "operator is generating them.",
                        false,
                        false),
                booleanProperty(
                        NATIVE_JOIN_SPILL_ENABLED,
                        "Native Execution only. Enable join spilling on native engine",
                        true,
                        false),
                integerProperty(
                        NATIVE_JOIN_SPILL_MEMORY_THRESHOLD,
                        "Native Execution only. The max memory that hash join can use before spilling. If it is 0, then there is no limit",
                        0,
                        false),
                integerProperty(
                        NATIVE_JOIN_SPILLER_PARTITION_BITS,
                        "Native Execution only. The number of bits (N) used to calculate the " +
                                "spilling partition number for hash join and RowNumber: 2 ^ N",
                        2,
                        false),
                integerProperty(
                        NATIVE_MAX_SPILL_FILE_SIZE,
                        "The max allowed spill file size. If it is zero, then there is no limit.",
                        0,
                        false),
                integerProperty(
                        NATIVE_MAX_SPILL_LEVEL,
                        "Native Execution only. The maximum allowed spilling level for hash join build.\n" +
                                "0 is the initial spilling level, -1 means unlimited.",
                        4,
                        false),
                integerProperty(
                        NATIVE_ORDER_BY_SPILL_MEMORY_THRESHOLD,
                        "Native Execution only. The max memory that order by can use before spilling. If it is 0, then there is no limit",
                        0,
                        false),
                booleanProperty(
                        NATIVE_ROW_NUMBER_SPILL_ENABLED,
                        "Native Execution only. Enable row number spilling on native engine",
                        true,
                        false),
                booleanProperty(
                        NATIVE_SIMPLIFIED_EXPRESSION_EVALUATION_ENABLED,
                        "Native Execution only. Enable simplified path in expression evaluation",
                        false,
                        false),
                stringProperty(
                        NATIVE_SPILL_COMPRESSION_CODEC,
                        "Native Execution only. The compression algorithm type to compress the spilled data.\n " +
                                "Supported compression codecs are: ZLIB, SNAPPY, LZO, ZSTD, LZ4 and GZIP. NONE means no compression.",
                        "none",
                        false),
                stringProperty(
                        NATIVE_SPILL_FILE_CREATE_CONFIG,
                        "Native Execution only. Config used to create spill files. This config is \n" +
                                "provided to underlying file system and the config is free form. The form should be\n" +
                                "defined by the underlying file system.",
                        "",
                        false),
                longProperty(
                        NATIVE_SPILL_WRITE_BUFFER_SIZE,
                        "Native Execution only. The maximum size in bytes to buffer the serialized spill data before writing to disk for IO efficiency.\n" +
                                "If set to zero, buffering is disabled.",
                        1048576L,
                        false),
                booleanProperty(
                        NATIVE_TOPN_ROW_NUMBER_SPILL_ENABLED,
                        "Native Execution only. Enable topN row number spilling on native engine",
                        true,
                        false),
                booleanProperty(
                        NATIVE_WINDOW_SPILL_ENABLED,
                        "Native Execution only. Enable window spilling on native engine",
                        true,
                        false),
                booleanProperty(
                        NATIVE_WRITER_SPILL_ENABLED,
                        "Native Execution only. Enable writer spilling on native engine",
                        true,
                        false));
    }

    @Deprecated
    public static boolean isLegacyTimestamp(Session session)
    {
        return session.getSystemProperty(LEGACY_TIMESTAMP, Boolean.class);
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
