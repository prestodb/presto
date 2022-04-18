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
package com.facebook.presto.operator;

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.codec.internal.compiler.CompilerThriftCodecFactory;
import com.facebook.drift.codec.internal.reflection.ReflectionThriftCodecFactory;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.utils.DurationToMillisThriftCodec;
import com.facebook.drift.codec.utils.JodaDateTimeToEpochMillisThriftCodec;
import com.facebook.drift.protocol.TBinaryProtocol;
import com.facebook.drift.protocol.TCompactProtocol;
import com.facebook.drift.protocol.TFacebookCompactProtocol;
import com.facebook.drift.protocol.TMemoryBuffer;
import com.facebook.drift.protocol.TProtocol;
import com.facebook.drift.protocol.TTransport;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertNotNull;

@Test(singleThreaded = true)
public class TestOperatorInfoUnionSerde
{
    private static final ThriftCatalog COMMON_CATALOG = new ThriftCatalog();
    private static final DurationToMillisThriftCodec DURATION_CODEC = new DurationToMillisThriftCodec(COMMON_CATALOG);
    private static final JodaDateTimeToEpochMillisThriftCodec DATE_TIME_CODEC = new JodaDateTimeToEpochMillisThriftCodec(COMMON_CATALOG);
    private static final ThriftCodecManager COMPILER_READ_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false), COMMON_CATALOG, ImmutableSet.of(DURATION_CODEC, DATE_TIME_CODEC));
    private static final ThriftCodecManager COMPILER_WRITE_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false), COMMON_CATALOG, ImmutableSet.of(DURATION_CODEC, DATE_TIME_CODEC));
    private static final ThriftCodec<OperatorInfoUnion> COMPILER_READ_CODEC = COMPILER_READ_CODEC_MANAGER.getCodec(OperatorInfoUnion.class);
    private static final ThriftCodec<OperatorInfoUnion> COMPILER_WRITE_CODEC = COMPILER_WRITE_CODEC_MANAGER.getCodec(OperatorInfoUnion.class);
    private static final ThriftCodecManager REFLECTION_READ_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory(), COMMON_CATALOG, ImmutableSet.of(DURATION_CODEC, DATE_TIME_CODEC));
    private static final ThriftCodecManager REFLECTION_WRITE_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory(), COMMON_CATALOG, ImmutableSet.of(DURATION_CODEC, DATE_TIME_CODEC));
    private static final ThriftCodec<OperatorInfoUnion> REFLECTION_READ_CODEC = REFLECTION_READ_CODEC_MANAGER.getCodec(OperatorInfoUnion.class);
    private static final ThriftCodec<OperatorInfoUnion> REFLECTION_WRITE_CODEC = REFLECTION_WRITE_CODEC_MANAGER.getCodec(OperatorInfoUnion.class);
    private static final TMemoryBuffer transport = new TMemoryBuffer(100 * 1024);
    private OperatorInfoUnion operatorInfoUnion;

    @BeforeMethod
    public void setUp()
    {
        operatorInfoUnion = new OperatorInfoUnion(getExchangeClientStatus());
    }

    @DataProvider
    public Object[][] codecCombinations()
    {
        return new Object[][] {
                {COMPILER_READ_CODEC, COMPILER_WRITE_CODEC},
                {COMPILER_READ_CODEC, REFLECTION_WRITE_CODEC},
                {REFLECTION_READ_CODEC, COMPILER_WRITE_CODEC},
                {REFLECTION_READ_CODEC, REFLECTION_WRITE_CODEC}
        };
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeBinaryProtocol(ThriftCodec<OperatorInfoUnion> readCodec, ThriftCodec<OperatorInfoUnion> writeCodec)
            throws Exception
    {
        OperatorInfoUnion operatorInfoUnion = getRoundTripSerialize(readCodec, writeCodec, TBinaryProtocol::new);
        assertSerde(operatorInfoUnion);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTCompactProtocol(ThriftCodec<OperatorInfoUnion> readCodec, ThriftCodec<OperatorInfoUnion> writeCodec)
            throws Exception
    {
        OperatorInfoUnion operatorInfoUnion = getRoundTripSerialize(readCodec, writeCodec, TCompactProtocol::new);
        assertSerde(operatorInfoUnion);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTFacebookCompactProtocol(ThriftCodec<OperatorInfoUnion> readCodec, ThriftCodec<OperatorInfoUnion> writeCodec)
            throws Exception
    {
        OperatorInfoUnion operatorInfoUnion = getRoundTripSerialize(readCodec, writeCodec, TFacebookCompactProtocol::new);
        assertSerde(operatorInfoUnion);
    }

    private OperatorInfoUnion getRoundTripSerialize(ThriftCodec<OperatorInfoUnion> readCodec, ThriftCodec<OperatorInfoUnion> writeCodec, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        TProtocol protocol = protocolFactory.apply(transport);
        writeCodec.write(operatorInfoUnion, protocol);
        return readCodec.read(protocol);
    }

    private void assertSerde(OperatorInfoUnion operatorInfoUnion)
    {
        ExchangeClientStatus exchangeClientStatus = operatorInfoUnion.getExchangeClientStatus();
        assertNotNull(exchangeClientStatus);
        assertThat(exchangeClientStatus.getBufferedBytes()).isEqualTo(246L);
        assertThat(exchangeClientStatus.getMaxBufferedBytes()).isEqualTo(762L);
        assertThat(exchangeClientStatus.getAverageBytesPerRequest()).isEqualTo(4155);
        assertThat(exchangeClientStatus.getSuccessfulRequestsCount()).isEqualTo(5708);
        assertThat(exchangeClientStatus.getBufferedPages()).isEqualTo(316);
        assertThat(exchangeClientStatus.isNoMoreLocations()).isTrue();

        List<PageBufferClientStatus> pageBufferClientStatuses = exchangeClientStatus.getPageBufferClientStatuses();
        assertNotNull(pageBufferClientStatuses);
        assertThat(pageBufferClientStatuses).hasSize(1);

        PageBufferClientStatus pageBufferClientStatus = pageBufferClientStatuses.get(0);
        assertThat(pageBufferClientStatus.getUri()).isEqualTo(URI.create("http://fake"));
        assertThat(pageBufferClientStatus.getState()).isEqualTo("running");
        assertThat(pageBufferClientStatus.getLastUpdate()).isEqualTo(new DateTime(2022, 10, 28, 16, 7, 15, 0));
        assertThat(pageBufferClientStatus.getRowsReceived()).isEqualTo(7174L);
        assertThat(pageBufferClientStatus.getPagesReceived()).isEqualTo(612);
        assertThat(pageBufferClientStatus.getRowsRejected()).isEqualTo(OptionalLong.of(93L));
        assertThat(pageBufferClientStatus.getPagesRejected()).isEqualTo(OptionalInt.of(12));
        assertThat(pageBufferClientStatus.getRequestsScheduled()).isEqualTo(2);
        assertThat(pageBufferClientStatus.getRequestsCompleted()).isEqualTo(71);
        assertThat(pageBufferClientStatus.getRequestsFailed()).isEqualTo(3);
        assertThat(pageBufferClientStatus.getHttpRequestState()).isEqualTo("OK");
    }

    private ExchangeClientStatus getExchangeClientStatus()
    {
        return new ExchangeClientStatus(
                246L,
                762L,
                4155,
                5708,
                316,
                true,
                ImmutableList.of(new PageBufferClientStatus(
                        URI.create("http://fake"),
                        "running",
                        new DateTime(2022, 10, 28, 16, 7, 15, 0),
                        7174L,
                        612,
                        OptionalLong.of(93L),
                        OptionalInt.of(12),
                        2,
                        71,
                        3,
                        "OK")));
    }
}
