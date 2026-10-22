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

import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.spi.session.PropertyMetadata;
import org.testng.annotations.Test;

import static com.facebook.airlift.units.DataSize.Unit.GIGABYTE;
import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_TASK_INFO_AGGREGATION_DRAIN_INTERVAL;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_TASK_INFO_AGGREGATION_MAX_BACKLOG_SIZE;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_TASK_INFO_AGGREGATION_SEAL_TIMEOUT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public class TestPrestoSparkSessionProperties
{
    @Test
    public void testTaskInfoAggregationLimitsAreCappedAtConfig()
    {
        PrestoSparkSessionProperties sessionProperties = new PrestoSparkSessionProperties(new PrestoSparkConfig()
                .setTaskInfoAggregationDrainInterval(new Duration(1, SECONDS))
                .setTaskInfoAggregationSealTimeout(new Duration(2, MINUTES))
                .setTaskInfoAggregationMaxBacklogSize(new DataSize(1, GIGABYTE)));

        PropertyMetadata<?> drainInterval = getProperty(sessionProperties, SPARK_TASK_INFO_AGGREGATION_DRAIN_INTERVAL);
        assertEquals(drainInterval.getDefaultValue(), new Duration(1, SECONDS));
        assertEquals(drainInterval.decode("1h"), new Duration(1, SECONDS));
        assertEquals(drainInterval.decode("1ms"), new Duration(1, MILLISECONDS));

        PropertyMetadata<?> sealTimeout = getProperty(sessionProperties, SPARK_TASK_INFO_AGGREGATION_SEAL_TIMEOUT);
        assertEquals(sealTimeout.getDefaultValue(), new Duration(2, MINUTES));
        assertEquals(sealTimeout.decode("1d"), new Duration(2, MINUTES));
        assertEquals(sealTimeout.decode("0ms"), new Duration(0, MILLISECONDS));

        PropertyMetadata<?> maxBacklogSize = getProperty(sessionProperties, SPARK_TASK_INFO_AGGREGATION_MAX_BACKLOG_SIZE);
        assertEquals(maxBacklogSize.getDefaultValue(), new DataSize(1, GIGABYTE));
        assertEquals(maxBacklogSize.decode("10GB"), new DataSize(1, GIGABYTE));
        assertEquals(maxBacklogSize.decode("10MB"), new DataSize(10, MEGABYTE));
    }

    private static PropertyMetadata<?> getProperty(PrestoSparkSessionProperties sessionProperties, String name)
    {
        return sessionProperties.getSessionProperties().stream()
                .filter(property -> property.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("missing session property " + name));
    }
}
