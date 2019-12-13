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

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;

public class PrestoSparkSessionProperties
{
    public static final String SPARK_INITIAL_PARTITION_COUNT = "spark_initial_partition_count";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public PrestoSparkSessionProperties(PrestoSparkConfig prestoSparkConfig)
    {
        sessionProperties = ImmutableList.of(
                integerProperty(
                        SPARK_INITIAL_PARTITION_COUNT,
                        "Initial partition count for Spark RDD when reading table",
                        prestoSparkConfig.getInitialSparkPartitionCount(),
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static int getSparkInitialPartitionCount(Session session)
    {
        return session.getSystemProperty(SPARK_INITIAL_PARTITION_COUNT, Integer.class);
    }
}
