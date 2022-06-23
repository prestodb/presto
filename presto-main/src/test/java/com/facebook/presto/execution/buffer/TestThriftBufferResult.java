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
package com.facebook.presto.execution.buffer;

import com.facebook.presto.spi.page.PageCodecMarker;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.execution.buffer.ThriftBufferResult.fromBufferResult;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static org.testng.Assert.assertEquals;

public class TestThriftBufferResult
{
    @Test
    public void testThriftSerializedPage()
    {
        SerializedPage serializedPage = new SerializedPage(EMPTY_SLICE, PageCodecMarker.none(), 0, 0, 0);
        ThriftSerializedPage thriftSerializedPage = new ThriftSerializedPage((serializedPage));
        SerializedPage newSerializedPage = thriftSerializedPage.toSerializedPage();
        assertEquals(serializedPage, newSerializedPage);
    }

    @Test
    public void testThriftBufferResult()
    {
        BufferResult bufferResult = new BufferResult("task-instance-id", 0, 0, false, ImmutableList.of());
        ThriftBufferResult thriftBufferResult = fromBufferResult(bufferResult);
        BufferResult newBufferResult = thriftBufferResult.toBufferResult();
        assertEquals(bufferResult, newBufferResult);
    }
}
