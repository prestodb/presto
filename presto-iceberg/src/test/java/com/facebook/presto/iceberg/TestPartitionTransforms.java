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
package com.facebook.presto.iceberg;

import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types.DateType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.TimestampType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestPartitionTransforms
{
    @Test
    public void testToStringMatchesSpecification()
    {
        assertEquals(Transforms.identity(StringType.get()).toString(), "identity");
        assertEquals(Transforms.bucket(StringType.get(), 13).toString(), "bucket[13]");
        assertEquals(Transforms.truncate(StringType.get(), 19).toString(), "truncate[19]");
        assertEquals(Transforms.year(DateType.get()).toString(), "year");
        assertEquals(Transforms.month(DateType.get()).toString(), "month");
        assertEquals(Transforms.day(DateType.get()).toString(), "day");
        assertEquals(Transforms.hour(TimestampType.withoutZone()).toString(), "hour");
    }
}
