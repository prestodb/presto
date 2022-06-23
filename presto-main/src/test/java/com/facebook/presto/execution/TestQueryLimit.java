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
package com.facebook.presto.execution;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import static com.facebook.presto.execution.QueryLimit.Source.QUERY;
import static com.facebook.presto.execution.QueryLimit.Source.RESOURCE_GROUP;
import static com.facebook.presto.execution.QueryLimit.Source.SYSTEM;
import static com.facebook.presto.execution.QueryLimit.createDataSizeLimit;
import static com.facebook.presto.execution.QueryLimit.createDurationLimit;
import static com.facebook.presto.execution.QueryLimit.getMinimum;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertThrows;

public class TestQueryLimit
{
    private static final QueryLimit<Duration> QUERY_LIMIT_DURATION = createDurationLimit(new Duration(1, HOURS), SYSTEM);
    private static final QueryLimit<DataSize> QUERY_LIMIT_DATA_SIZE = createDataSizeLimit(new DataSize(1, MEGABYTE), RESOURCE_GROUP);

    @Test
    public void testGetLimit()
    {
        assertEquals(QUERY_LIMIT_DURATION.getLimit(), new Duration(1, HOURS));
        assertEquals(QUERY_LIMIT_DATA_SIZE.getLimit(), new DataSize(1, MEGABYTE));
    }

    @Test
    public void testGetSource()
    {
        assertEquals(QUERY_LIMIT_DATA_SIZE.getLimitSource(), RESOURCE_GROUP);
        assertEquals(QUERY_LIMIT_DURATION.getLimitSource(), SYSTEM);
        assertEquals(createDurationLimit(new Duration(1, HOURS), QUERY).getLimitSource(), QUERY);
    }

    @Test
    public void testGetMinimum()
    {
        QueryLimit<Duration> longLimit = createDurationLimit(new Duration(2, HOURS), SYSTEM);
        QueryLimit<Duration> shortLimit = createDurationLimit(new Duration(1, MINUTES), RESOURCE_GROUP);
        assertEquals(getMinimum(QUERY_LIMIT_DURATION, longLimit, shortLimit), shortLimit);

        QueryLimit<DataSize> largeLimit = createDataSizeLimit(new DataSize(2, MEGABYTE), QUERY);
        QueryLimit<DataSize> smallLimit = createDataSizeLimit(new DataSize(1, BYTE), RESOURCE_GROUP);
        assertEquals(getMinimum(smallLimit, QUERY_LIMIT_DATA_SIZE, largeLimit), smallLimit);
        assertEquals(getMinimum(null, QUERY_LIMIT_DATA_SIZE, largeLimit), QUERY_LIMIT_DATA_SIZE);
        assertEquals(getMinimum(null, largeLimit, null, null, QUERY_LIMIT_DATA_SIZE), QUERY_LIMIT_DATA_SIZE);
        assertEquals(getMinimum(smallLimit, null, null, null), smallLimit);
        assertThrows(IllegalArgumentException.class, () -> getMinimum(null));
        assertThrows(IllegalArgumentException.class, () -> getMinimum(null, null, null));
    }
}
