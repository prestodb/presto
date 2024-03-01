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
package com.facebook.presto.statistic;

import com.facebook.airlift.http.client.thrift.ThriftProtocolException;
import com.facebook.airlift.http.client.thrift.ThriftProtocolUtils;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.transport.netty.codec.Protocol;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Redis codec implementation for string keys and HistoricalPlanStatistics values.
 */
public class HistoricalStatisticsSerde
        implements RedisCodec<String, HistoricalPlanStatistics>
{
    private static final int ESTIMATED_BUFFER_SIZE_BYTES = 100 * 1024;
    private final ThriftCodecManager thriftCodecManager = new ThriftCodecManager();

    @Override
    public String decodeKey(ByteBuffer bytes)
    {
        if (bytes.hasArray()) {
            return StandardCharsets.UTF_8.decode(bytes).toString();
        }
        else {
            throw new RuntimeException("HistoricalStatisticsSerde: Error decoding key planHash which was of type String");
        }
    }

    @Override
    public ByteBuffer encodeKey(String key)
    {
        return ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public ByteBuffer encodeValue(HistoricalPlanStatistics historicalPlanStatistics)
    {
        ThriftCodec<HistoricalPlanStatistics> writeCodec = thriftCodecManager.getCodec(HistoricalPlanStatistics.class);
        SliceOutput dynamicSliceOutput = new DynamicSliceOutput(ESTIMATED_BUFFER_SIZE_BYTES);
        try {
            ThriftProtocolUtils.write(historicalPlanStatistics, writeCodec, Protocol.BINARY, dynamicSliceOutput);
            return ByteBuffer.wrap(dynamicSliceOutput.slice().getBytes());
        }
        catch (ThriftProtocolException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public HistoricalPlanStatistics decodeValue(ByteBuffer byteBuffer)
    {
        ThriftCodec<HistoricalPlanStatistics> readCodec = thriftCodecManager.getCodec(HistoricalPlanStatistics.class);
        try {
            return ThriftProtocolUtils.read(readCodec, Protocol.BINARY, Slices.wrappedBuffer(byteBuffer).getInput());
        }
        catch (ThriftProtocolException e) {
            throw new RuntimeException(e);
        }
    }
}
