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

import com.facebook.airlift.http.client.thrift.ThriftProtocolUtils;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Optional;

import static com.facebook.drift.transport.netty.codec.Protocol.FB_COMPACT;
import static com.facebook.presto.hive.HiveFileInfo.createHiveFileInfo;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;

public class TestHiveFileInfo
{
    @Test
    public void testThriftRoundTrip()
            throws IOException
    {
        ThriftCodec<HiveFileInfo> hiveFileInfoThriftCodec = new ThriftCodecManager().getCodec(HiveFileInfo.class);
        HiveFileInfo hiveFileInfo = createHiveFileInfo(new LocatedFileStatus(
                        100,
                        false,
                        0,
                        0L,
                        0L,
                        0L,
                        null,
                        null,
                        null,
                        null,
                        new Path("test"),
                        new org.apache.hadoop.fs.BlockLocation[] {new BlockLocation(new String[1], new String[] {"localhost"}, 0, 100)}),
                Optional.empty());

        int thriftBufferSize = toIntExact(new DataSize(1000, BYTE).toBytes());
        SliceOutput dynamicSliceOutput = new DynamicSliceOutput(thriftBufferSize);
        ThriftProtocolUtils.write(hiveFileInfo, hiveFileInfoThriftCodec, FB_COMPACT, dynamicSliceOutput);
        byte[] serialized = dynamicSliceOutput.slice().getBytes();

        HiveFileInfo copy = ThriftProtocolUtils.read(hiveFileInfoThriftCodec, FB_COMPACT, Slices.wrappedBuffer(serialized).getInput());
        assertEquals(copy, hiveFileInfo);
    }
}
