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
package com.facebook.presto.hive;

import org.testng.annotations.Test;

import java.util.OptionalInt;

import static com.facebook.presto.hive.HiveWriterFactory.computeBucketedFileName;
import static com.facebook.presto.hive.HiveWriterFactory.getBucketNumber;
import static org.apache.hadoop.hive.ql.exec.Utilities.getBucketIdFromFile;
import static org.testng.Assert.assertEquals;

public class TestHiveWriterFactory
{
    @Test
    public void testComputeBucketedFileName()
    {
        String name = computeBucketedFileName("20180102_030405_00641_x1y2z", 1234);
        assertEquals(name, "001234_0_20180102_030405_00641_x1y2z");
        assertEquals(getBucketIdFromFile(name), 1234);
    }

    @Test
    public void testGetBucketNumber()
    {
        assertEquals(getBucketNumber("0234_0"), OptionalInt.of(234));
        assertEquals(getBucketNumber("000234_0"), OptionalInt.of(234));
        assertEquals(getBucketNumber("0234_99"), OptionalInt.of(234));
        assertEquals(getBucketNumber("0234_0.txt"), OptionalInt.of(234));
        assertEquals(getBucketNumber("0234_0_copy_1"), OptionalInt.of(234));
        assertEquals(getBucketNumber("20190526_072952_00009_fn7s5_bucket-00234"), OptionalInt.of(234));
        assertEquals(getBucketNumber("20190526_072952_00009_fn7s5_bucket-00234.txt"), OptionalInt.of(234));
        assertEquals(getBucketNumber("20190526_235847_87654_fn7s5_bucket-56789"), OptionalInt.of(56789));

        assertEquals(getBucketNumber("234_99"), OptionalInt.empty());
        assertEquals(getBucketNumber("0234.txt"), OptionalInt.empty());
        assertEquals(getBucketNumber("0234.txt"), OptionalInt.empty());
    }
}
