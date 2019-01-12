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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.prestosql.plugin.hive.PartitionUpdate.UpdateMode;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestPartitionUpdate
{
    private static final JsonCodec<PartitionUpdate> CODEC = jsonCodec(PartitionUpdate.class);

    @Test
    public void testRoundTrip()
    {
        PartitionUpdate expected = new PartitionUpdate(
                "test",
                UpdateMode.APPEND,
                "/writePath",
                "/targetPath",
                ImmutableList.of("file1", "file3"),
                123,
                456,
                789);

        PartitionUpdate actual = CODEC.fromJson(CODEC.toJson(expected));

        assertEquals(actual.getName(), "test");
        assertEquals(actual.getUpdateMode(), UpdateMode.APPEND);
        assertEquals(actual.getWritePath(), new Path("/writePath"));
        assertEquals(actual.getTargetPath(), new Path("/targetPath"));
        assertEquals(actual.getFileNames(), ImmutableList.of("file1", "file3"));
        assertEquals(actual.getRowCount(), 123);
        assertEquals(actual.getInMemoryDataSizeInBytes(), 456);
        assertEquals(actual.getOnDiskDataSizeInBytes(), 789);
    }
}
