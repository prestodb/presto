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
package com.facebook.presto.spark.execution.task;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spark.execution.task.PrestoSparkNativeTaskExecutorFactory.readBroadcastDescriptor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * Channel 3 of the stats page carries the descriptor. Its absence, nullness and
 * emptiness all mean "open by path" rather than fail the read.
 */
public class TestReadBroadcastDescriptor
{
    private static Block varchar(String value)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, 1);
        if (value == null) {
            builder.appendNull();
        }
        else {
            VARCHAR.writeString(builder, value);
        }
        return builder.build();
    }

    private static Block bigint(long value)
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(null, 1);
        BIGINT.writeLong(builder, value);
        return builder.build();
    }

    private static Page statsPage(String descriptor)
    {
        return new Page(varchar("/tmp/file.bin"), bigint(1024), bigint(7), varchar(descriptor));
    }

    @Test
    public void testReadsDescriptorFromFourChannelPage()
    {
        assertEquals(readBroadcastDescriptor(statsPage("c2VjcmV0LXRva2Vu"), 0), "c2VjcmV0LXRva2Vu");
    }

    /**
     * A file system that cannot produce a handle emits an empty string, not a null block.
     */
    @Test
    public void testEmptyDescriptorReadsAsNull()
    {
        assertNull(readBroadcastDescriptor(statsPage(""), 0));
    }

    @Test
    public void testNullDescriptorBlockReadsAsNull()
    {
        assertNull(readBroadcastDescriptor(statsPage(null), 0));
    }

    /**
     * Backward compatibility: a native worker that predates this change emits only three
     * channels, so indexing channel 3 must not throw.
     */
    @Test
    public void testThreeChannelPageReadsAsNull()
    {
        Page legacyPage = new Page(varchar("/tmp/file.bin"), bigint(1024), bigint(7));

        assertNull(readBroadcastDescriptor(legacyPage, 0));
    }
}
