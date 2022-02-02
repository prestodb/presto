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
package com.facebook.presto.server;

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.codec.internal.compiler.CompilerThriftCodecFactory;
import com.facebook.drift.codec.internal.reflection.ReflectionThriftCodecFactory;
import com.facebook.drift.protocol.TBinaryProtocol;
import com.facebook.drift.protocol.TCompactProtocol;
import com.facebook.drift.protocol.TFacebookCompactProtocol;
import com.facebook.drift.protocol.TMemoryBuffer;
import com.facebook.drift.protocol.TProtocol;
import com.facebook.drift.protocol.TTransport;
import com.facebook.presto.client.ClientTypeSignature;
import com.facebook.presto.client.ClientTypeSignatureParameter;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.ErrorLocation;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.common.RuntimeMetric;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.type.ParameterKind;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.WarningCode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestThriftSerdeQueryResults
{
    private static final ThriftCodecManager COMPILER_READ_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
    private static final ThriftCodecManager COMPILER_WRITE_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
    private static final ThriftCodec<QueryResults> COMPILER_READ_CODEC = COMPILER_READ_CODEC_MANAGER.getCodec(QueryResults.class);
    private static final ThriftCodec<QueryResults> COMPILER_WRITE_CODEC = COMPILER_WRITE_CODEC_MANAGER.getCodec(QueryResults.class);
    private static final ThriftCodecManager REFLECTION_READ_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory());
    private static final ThriftCodecManager REFLECTION_WRITE_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory());
    private static final ThriftCodec<QueryResults> REFLECTION_READ_CODEC = REFLECTION_READ_CODEC_MANAGER.getCodec(QueryResults.class);
    private static final ThriftCodec<QueryResults> REFLECTION_WRITE_CODEC = REFLECTION_WRITE_CODEC_MANAGER.getCodec(QueryResults.class);
    private static final TMemoryBuffer transport = new TMemoryBuffer(100 * 1024);

    public static final String QUERY_ID = "20160128_214710_00012_rk68b";
    public static final long TOKEN = 123L;
    public static final String DATA_TYPE = "bigint";
    public static final Long DATA_VALUE = 123L;
    public static final URI INFO_URI = URI.create("http://localhost:54855/query.html?" + QUERY_ID);
    public static final URI PARTIAL_CANCEL_URI = URI.create("http://localhost:54855//v1/statement/queued/" + QUERY_ID + "/" + TOKEN);
    public static final URI NEXT_URI = URI.create("http://localhost:54855//v1/statement/queued/" + QUERY_ID + "/" + TOKEN + "?slug=xabcdef123456789");

    public static final String FINISHED = "FINISHED";

    private static final String COLUMN_NAME = "_col0";
    private static final String TEST_METRIC_NAME = "test";
    private static final String ERROR_MESSAGE = "error";
    private static final String FAILURE_TYPE = "failure";
    private static final int LOCATION_ERROR = 1;
    private static final int ERROR_CODE = 1;
    private static final int WARNING_CODE = 1;
    private static final String WARNING_CODE_NAME = "warning code";
    private static final String WARNING_MESSAGE = "warning";
    private static final String UPDATE_TYPE = "update";
    private static final Long UPDATE_COUNT = 1L;

    private QueryResults QueryResults;

    @BeforeMethod
    public void setUp()
    {
        QueryResults = getQueryResults();
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
    public void testRoundTripSerializeBinaryProtocol(ThriftCodec<QueryResults> readCodec, ThriftCodec<QueryResults> writeCodec)
            throws Exception
    {
        QueryResults QueryResults = getRoundTripSerialize(readCodec, writeCodec, TBinaryProtocol::new);
        assertSerde(QueryResults);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTCompactProtocol(ThriftCodec<QueryResults> readCodec, ThriftCodec<QueryResults> writeCodec)
            throws Exception
    {
        QueryResults QueryResults = getRoundTripSerialize(readCodec, writeCodec, TCompactProtocol::new);
        assertSerde(QueryResults);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTFacebookCompactProtocol(ThriftCodec<QueryResults> readCodec, ThriftCodec<QueryResults> writeCodec)
            throws Exception
    {
        QueryResults QueryResults = getRoundTripSerialize(readCodec, writeCodec, TFacebookCompactProtocol::new);
        assertSerde(QueryResults);
    }

    private void assertSerde(QueryResults queryResults)
    {
        // new QueryResults(QUERY_ID, INFO_URI, PARTIAL_CANCEL_URI, NEXT_URI, ImmutableList.of(column), data, stats, error, ImmutableList.of(warning), UPDATE_TYPE, UPDATE_COUNT);
        assertEquals(queryResults.getId(), QUERY_ID);
        assertEquals(queryResults.getInfoUri(), INFO_URI);
        assertEquals(queryResults.getPartialCancelUri(), PARTIAL_CANCEL_URI);
        assertEquals(queryResults.getNextUri(), NEXT_URI);

        assertEquals(queryResults.getColumns().size(), 1);
        Column column = queryResults.getColumns().get(0);
        ClientTypeSignature typeSignature = column.getTypeSignature();
        ClientTypeSignatureParameter signatureParameter = typeSignature.getArguments().get(0);
        assertEquals(column.getName(), COLUMN_NAME);
        assertEquals(column.getType(), DATA_TYPE);
        assertEquals(signatureParameter.getKind(), ParameterKind.LONG);
        assertEquals(typeSignature.getRawType(), DATA_TYPE);

        assertEquals(queryResults.getData().iterator().next().get(0), DATA_VALUE);

        assertEquals(queryResults.getStats().getState(), FINISHED);
        RuntimeStats runtimeStats = queryResults.getStats().getRuntimeStats();
        RuntimeMetric metric = runtimeStats.getMetrics().get(TEST_METRIC_NAME);
        assertEquals(metric.getName(), TEST_METRIC_NAME);
        assertEquals(metric.getCount(), 1);
        assertEquals(metric.getMax(), 1);
        assertEquals(metric.getMin(), 1);
        assertEquals(metric.getSum(), 1);

        QueryError error = queryResults.getError();
        ErrorLocation errorLocation = error.getErrorLocation();
        FailureInfo failureInfo = error.getFailureInfo();
        assertEquals(error.getErrorName(), ERROR_MESSAGE);
        assertEquals(error.getErrorCode(), ERROR_CODE);
        assertEquals(errorLocation, failureInfo.getErrorLocation());
        assertEquals(errorLocation.getColumnNumber(), LOCATION_ERROR);
        assertEquals(errorLocation.getLineNumber(), LOCATION_ERROR);
        assertEquals(failureInfo.getType(), FAILURE_TYPE);
        assertEquals(failureInfo.getMessage(), ERROR_MESSAGE);

        PrestoWarning warning = queryResults.getWarnings().get(0);
        WarningCode warningCode = warning.getWarningCode();
        assertEquals(warning.getMessage(), WARNING_MESSAGE);
        assertEquals(warningCode.getCode(), WARNING_CODE);
        assertEquals(warningCode.getName(), WARNING_CODE_NAME);

        assertEquals(queryResults.getUpdateCount(), UPDATE_COUNT);
        assertEquals(queryResults.getUpdateType(), UPDATE_TYPE);
    }

    private QueryResults getRoundTripSerialize(ThriftCodec<QueryResults> readCodec, ThriftCodec<QueryResults> writeCodec, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        TProtocol protocol = protocolFactory.apply(transport);
        writeCodec.write(QueryResults, protocol);
        return readCodec.read(protocol);
    }

    private QueryResults getQueryResults()
    {
        ClientTypeSignatureParameter signatureParameter = new ClientTypeSignatureParameter(ParameterKind.LONG, DATA_VALUE);
        ClientTypeSignature typeSignature = new ClientTypeSignature(DATA_TYPE, Collections.emptyList(), Collections.emptyList(), ImmutableList.of(signatureParameter));
        Column column = new Column(COLUMN_NAME, DATA_TYPE, typeSignature);

        RuntimeStats runtimeStats = new RuntimeStats();
        runtimeStats.addMetricValue(TEST_METRIC_NAME, 1);
        StatementStats stats = StatementStats.builder().setState(FINISHED).setRuntimeStats(runtimeStats).build();

        ErrorLocation errorLocation = new ErrorLocation(LOCATION_ERROR, LOCATION_ERROR);
        FailureInfo failureInfo = new FailureInfo(FAILURE_TYPE, ERROR_MESSAGE, null, Collections.emptyList(), Collections.emptyList(), errorLocation);
        QueryError error = new QueryError(ERROR_MESSAGE, null, ERROR_CODE, null, null, false, errorLocation, failureInfo);

        List<List<Object>> data = ImmutableList.of(ImmutableList.of(DATA_VALUE));
        PrestoWarning warning = new PrestoWarning(new WarningCode(WARNING_CODE, WARNING_CODE_NAME), WARNING_MESSAGE);
        return new QueryResults(QUERY_ID, INFO_URI, PARTIAL_CANCEL_URI, NEXT_URI, ImmutableList.of(column), data, stats, error, ImmutableList.of(warning), UPDATE_TYPE, UPDATE_COUNT);
    }
}
