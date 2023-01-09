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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class TestHiveCommitHandle
{
    private Map<SchemaTableName, List<Long>> testData;

    @BeforeTest
    public void setTestData()
    {
        ImmutableMap.Builder<SchemaTableName, List<Long>> builder = ImmutableMap.builder();
        builder.put(new SchemaTableName("s1", "t1"), ImmutableList.of(1L, 2L));
        builder.put(new SchemaTableName("s2", "t2"), ImmutableList.of(3L, 4L));
        testData = builder.build();
    }

    @Test
    public void testGetSerializedCommitOutput()
    {
        HiveCommitHandle commitHandle = new HiveCommitHandle(testData, ImmutableMap.of());
        assertEquals(commitHandle.getSerializedCommitOutputForRead(new SchemaTableName("s1", "t1")), "1,2");
        assertEquals(commitHandle.getSerializedCommitOutputForRead(new SchemaTableName("s2", "t2")), "3,4");
        assertEquals(commitHandle.getSerializedCommitOutputForRead(new SchemaTableName("s3", "t3")), "");
    }
}
