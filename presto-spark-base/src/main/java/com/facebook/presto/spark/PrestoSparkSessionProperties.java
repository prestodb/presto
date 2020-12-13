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
package com.facebook.presto.spark;

import com.facebook.presto.Session;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.dataSizeProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;

public class PrestoSparkSessionProperties
{
    public static final String SPARK_PARTITION_COUNT_AUTO_TUNE_ENABLED = "spark_partition_count_auto_tune_enabled";
    public static final String MIN_SPARK_INPUT_PARTITION_COUNT_FOR_AUTO_TUNE = "min_spark_input_partition_count_for_auto_tune";
    public static final String MAX_SPARK_INPUT_PARTITION_COUNT_FOR_AUTO_TUNE = "max_spark_input_partition_count_for_auto_tune";
    public static final String SPARK_INITIAL_PARTITION_COUNT = "spark_initial_partition_count";
    public static final String MAX_SPLITS_DATA_SIZE_PER_SPARK_PARTITION = "max_splits_data_size_per_spark_partition";
    public static final String SHUFFLE_OUTPUT_TARGET_AVERAGE_ROW_SIZE = "shuffle_output_target_average_row_size";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public PrestoSparkSessionProperties(PrestoSparkConfig prestoSparkConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        SPARK_PARTITION_COUNT_AUTO_TUNE_ENABLED,
                        "Automatic tuning of spark initial partition count based on splits size per partition",
                        prestoSparkConfig.isSparkPartitionCountAutoTuneEnabled(),
                        false),
                integerProperty(
                        MIN_SPARK_INPUT_PARTITION_COUNT_FOR_AUTO_TUNE,
                        "Minimal Spark input partition count when Spark partition auto tune is enabled",
                        prestoSparkConfig.getMinSparkInputPartitionCountForAutoTune(),
                        false),
                integerProperty(
                        MAX_SPARK_INPUT_PARTITION_COUNT_FOR_AUTO_TUNE,
                        "Max Spark input partition count when Spark partition auto tune is enabled",
                        prestoSparkConfig.getMaxSparkInputPartitionCountForAutoTune(),
                        false),
                integerProperty(
                        SPARK_INITIAL_PARTITION_COUNT,
                        "Initial partition count for Spark RDD when reading table",
                        prestoSparkConfig.getInitialSparkPartitionCount(),
                        false),
                dataSizeProperty(
                        MAX_SPLITS_DATA_SIZE_PER_SPARK_PARTITION,
                        "Maximal size in bytes for splits assigned to one partition",
                        prestoSparkConfig.getMaxSplitsDataSizePerSparkPartition(),
                        false),
                dataSizeProperty(
                        SHUFFLE_OUTPUT_TARGET_AVERAGE_ROW_SIZE,
                        "Target average size for row entries produced by Presto on Spark for shuffle",
                        prestoSparkConfig.getShuffleOutputTargetAverageRowSize(),
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isSparkPartitionCountAutoTuneEnabled(Session session)
    {
        return session.getSystemProperty(SPARK_PARTITION_COUNT_AUTO_TUNE_ENABLED, Boolean.class);
    }

    public static int getMinSparkInputPartitionCountForAutoTune(Session session)
    {
        return session.getSystemProperty(MIN_SPARK_INPUT_PARTITION_COUNT_FOR_AUTO_TUNE, Integer.class);
    }

    public static int getMaxSparkInputPartitionCountForAutoTune(Session session)
    {
        return session.getSystemProperty(MAX_SPARK_INPUT_PARTITION_COUNT_FOR_AUTO_TUNE, Integer.class);
    }

    public static int getSparkInitialPartitionCount(Session session)
    {
        return session.getSystemProperty(SPARK_INITIAL_PARTITION_COUNT, Integer.class);
    }

    public static DataSize getMaxSplitsDataSizePerSparkPartition(Session session)
    {
        return session.getSystemProperty(MAX_SPLITS_DATA_SIZE_PER_SPARK_PARTITION, DataSize.class);
    }

    public static DataSize getShuffleOutputTargetAverageRowSize(Session session)
    {
        return session.getSystemProperty(SHUFFLE_OUTPUT_TARGET_AVERAGE_ROW_SIZE, DataSize.class);
    }
}
