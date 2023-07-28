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
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.utils.DataSizeToBytesThriftCodec;
import com.facebook.drift.codec.utils.DurationToMillisThriftCodec;
import com.facebook.drift.codec.utils.LocaleToLanguageTagCodec;
import com.facebook.drift.codec.utils.UuidToLeachSalzBinaryEncodingThriftCodec;
import com.facebook.drift.protocol.*;
import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.function.*;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.testng.Assert.*;

@Test(singleThreaded = true)
public class TestThriftTaskUpdateRequest
{
    private static final ThriftCatalog THRIFT_CATALOG = new ThriftCatalog();
    private static final Set<ThriftCodec<?>> THRIFT_CODECS_SET = ImmutableSet.of(
            new DurationToMillisThriftCodec(THRIFT_CATALOG),
            new DataSizeToBytesThriftCodec(THRIFT_CATALOG),
            new LocaleToLanguageTagCodec(THRIFT_CATALOG),
            new UuidToLeachSalzBinaryEncodingThriftCodec(THRIFT_CATALOG));
    private static final ThriftCodecManager COMPILER_READ_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false), THRIFT_CATALOG, THRIFT_CODECS_SET);
    private static final ThriftCodecManager COMPILER_WRITE_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false), THRIFT_CATALOG, THRIFT_CODECS_SET);
    private static final ThriftCodec<TaskUpdateRequest> COMPILER_READ_CODEC = COMPILER_READ_CODEC_MANAGER.getCodec(TaskUpdateRequest.class);
    private static final ThriftCodec<TaskUpdateRequest> COMPILER_WRITE_CODEC = COMPILER_WRITE_CODEC_MANAGER.getCodec(TaskUpdateRequest.class);
    private static final ThriftCodecManager REFLECTION_READ_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory(), THRIFT_CATALOG, THRIFT_CODECS_SET);
    private static final ThriftCodecManager REFLECTION_WRITE_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory(), THRIFT_CATALOG, THRIFT_CODECS_SET);
    private static final ThriftCodec<TaskUpdateRequest> REFLECTION_READ_CODEC = REFLECTION_READ_CODEC_MANAGER.getCodec(TaskUpdateRequest.class);
    private static final ThriftCodec<TaskUpdateRequest> REFLECTION_WRITE_CODEC = REFLECTION_WRITE_CODEC_MANAGER.getCodec(TaskUpdateRequest.class);
    private static final TMemoryBuffer transport = new TMemoryBuffer(100 * 1024);

    //Dummy values for fake TaskUpdateRequest
    //values for SessionRepresentation
    private static final String FAKE_QUERY_ID = "FAKE_QUERY_ID";
    private static final Optional<TransactionId> FAKE_TRANSACTION_ID = Optional.of(TransactionId.create());
    private static final boolean FAKE_CLIENT_TRANSACTION_SUPPORT = false;
    private static final String FAKE_USER = "FAKE_USER";
    private static final Optional<String> FAKE_PRINCIPAL = Optional.of("FAKE_PRINCIPAL");
    private static final Optional<String> FAKE_SOURCE = Optional.of("FAKE_SOURCE");
    private static final Optional<String> FAKE_CATALOG = Optional.of("FAKE_CATALOG");
    private static final Optional<String> FAKE_SCHEMA = Optional.of("FAKE_SCHEMA");
    private static final Optional<String> FAKE_TRACE_TOKEN = Optional.of("FAKE_TRACE_TOKEN");
    private static final TimeZoneKey FAKE_TIME_ZONE_KEY = TimeZoneKey.UTC_KEY;
    private static final Locale FAKE_LOCALE = Locale.ENGLISH;
    private static final Optional<String> FAKE_REMOTE_USER_ADDRESS = Optional.of("FAKE_REMOTE_USER_ADDRESS");
    private static final Optional<String> FAKE_USER_AGENT = Optional.of("FAKE_USER_AGENT");
    private static final Optional<String> FAKE_CLIENT_INFO = Optional.of("FAKE_CLIENT_INFO");
    private static final Set<String> FAKE_CLIENT_TAGS = ImmutableSet.of("FAKE_CLIENT_TAG");

    private static final long FAKE_START_TIME = 124354L;
    private static final Optional<Duration> FAKE_EXECUTION_TIME = Optional.of(new Duration(12434L, TimeUnit.MICROSECONDS));
    private static final Optional<Duration> FAKE_CPU_TIME = Optional.of(new Duration(44279L, TimeUnit.NANOSECONDS));
    private static final Optional<DataSize> FAKE_PEAK_MEMORY = Optional.of(new DataSize(135513L, DataSize.Unit.BYTE));
    private static final Optional<DataSize> FAKE_PEAK_TASK_MEMORY = Optional.of(new DataSize(1313L, DataSize.Unit.MEGABYTE));
    private static final Map<String, String> FAKE_SYSTEM_PROPERTIES = ImmutableMap.of("FAKE_KEY","FAKE_VALUE");
    private static final ConnectorId FAKE_CATALOG_KEY = new ConnectorId("FAKE_CATALOG_NAME");
    private static final Map<String, String> FAKE_CATALOG_VALUE = ImmutableMap.of("FAKE_KEY","FAKE_VALUE");
    private static final Map<ConnectorId, Map<String, String>> FAKE_CATALOG_PROPERTIES = ImmutableMap.of(FAKE_CATALOG_KEY, FAKE_CATALOG_VALUE);
    private static final Map<String, Map<String, String>> FAKE_UNPROCESSED_CATALOG_PROPERTIES = ImmutableMap.of("FAKE_NAME", FAKE_CATALOG_VALUE);
    private static final SelectedRole FAKE_SELECTED_ROLE = new SelectedRole(SelectedRole.Type.ROLE, Optional.of("FAKE_ROLE"));
    private static final Map<String, SelectedRole> FAKE_ROLES = ImmutableMap.of("FAKE_KEY", FAKE_SELECTED_ROLE);
    private static final Map<String, String> FAKE_PREPARED_STATEMENTS = ImmutableMap.of("FAKE_KEY","FAKE_VALUE");
    private static final QualifiedObjectName FAKE_QUALIFIED_OBJECT_NAME = new QualifiedObjectName("catalog", "schema", "object");
    private static final TypeVariableConstraint FAKE_TYPE_CONSTRAINT = new TypeVariableConstraint("FAKE_NAME", true, true, "FAKE_BOUND", true);
    private static final List<TypeVariableConstraint> FAKE_TYPE_CONSTRAINT_LIST = ImmutableList.of(FAKE_TYPE_CONSTRAINT);
    private static final LongVariableConstraint FAKE_LONG_CONSTRAINT = new LongVariableConstraint("FAKE_NAME", "FAKE_EXP");
    private static final List<LongVariableConstraint> FAKE_LONG_CONSTRAINT_LIST = ImmutableList.of(FAKE_LONG_CONSTRAINT);
    private static final List<TypeSignature> FAKE_TYPE_LIST = ImmutableList.of(new TypeSignature("FAKE_BASE"));
    private static final SqlFunctionId FAKE_SQL_FUNCTION_ID = new SqlFunctionId(FAKE_QUALIFIED_OBJECT_NAME, FAKE_TYPE_LIST);
    private static final String FAKE_NAME = "FAKE_NAME";
    private static final TypeSignature FAKE_TYPE_SIGNATURE = TypeSignature.parseTypeSignature("FAKE_TYPE");
    private static final List<TypeSignature> FAKE_ARGUMENT_TYPES = ImmutableList.of(FAKE_TYPE_SIGNATURE);
    private static final boolean FAKE_VARIABLE_ARITY = true;
    private static final String FAKE_DESCRIPTION = "FAKE_DESCRIPTION";
    private static final String FAKE_BODY = "FAKE_BODY";
    //values for extraCredentials and fragment
    private static final Map<String, String> FAKE_EXTRA_CREDENTIALS = ImmutableMap.of("FAKE_KEY", "FAKE_VALUE");
    private static final Optional<byte[]> FAKE_FRAGMENT = Optional.of(new byte[] {1, 2, 3, 4});
    private TaskUpdateRequest taskUpdateRequest;

    @BeforeMethod
    public void setUp()
    {
        taskUpdateRequest = getTaskUpdateRequest();
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
    public void testRoundTripSerializeBinaryProtocol(ThriftCodec<TaskUpdateRequest> readCodec, ThriftCodec<TaskUpdateRequest> writeCodec)
            throws Exception
    {
        TaskUpdateRequest taskUpdateRequest = getRoundTripSerialize(readCodec, writeCodec, TBinaryProtocol::new);
        assertSerde(taskUpdateRequest);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTCompactProtocol(ThriftCodec<TaskUpdateRequest> readCodec, ThriftCodec<TaskUpdateRequest> writeCodec)
            throws Exception
    {
        TaskUpdateRequest taskUpdateRequest = getRoundTripSerialize(readCodec, writeCodec, TCompactProtocol::new);
        assertSerde(taskUpdateRequest);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTFacebookCompactProtocol(ThriftCodec<TaskUpdateRequest> readCodec, ThriftCodec<TaskUpdateRequest> writeCodec)
            throws Exception
    {
        TaskUpdateRequest taskUpdateRequest = getRoundTripSerialize(readCodec, writeCodec, TFacebookCompactProtocol::new);
        assertSerde(taskUpdateRequest);
    }

    private void assertSerde(TaskUpdateRequest taskUpdateRequest)
    {
        //Assertions for session
        SessionRepresentation actualSession = taskUpdateRequest.getSession();
        assertEquals(actualSession.getQueryId(), FAKE_QUERY_ID);
        assertEquals(actualSession.getTransactionId(), FAKE_TRANSACTION_ID);
        assertEquals(actualSession.isClientTransactionSupport(), FAKE_CLIENT_TRANSACTION_SUPPORT);
        assertEquals(actualSession.getUser(), FAKE_USER);
        assertEquals(actualSession.getPrincipal(), FAKE_PRINCIPAL);
        assertEquals(actualSession.getSource(), FAKE_SOURCE);
        assertEquals(actualSession.getCatalog(), FAKE_CATALOG);
        assertEquals(actualSession.getSchema(), FAKE_SCHEMA);
        assertEquals(actualSession.getTraceToken(), FAKE_TRACE_TOKEN);
        assertEquals(actualSession.getTimeZoneKey(), FAKE_TIME_ZONE_KEY);
        assertEquals(actualSession.getLocale(), FAKE_LOCALE);
        assertEquals(actualSession.getRemoteUserAddress(), FAKE_REMOTE_USER_ADDRESS);
        assertEquals(actualSession.getUserAgent(), FAKE_USER_AGENT);
        assertEquals(actualSession.getClientInfo(), FAKE_CLIENT_INFO);
        assertEquals(actualSession.getClientTags(), FAKE_CLIENT_TAGS);
        //resourceEstimates
        ResourceEstimates actualResourceEstimates = actualSession.getResourceEstimates();
        assertEquals(actualResourceEstimates.getExecutionTime(), FAKE_EXECUTION_TIME);
        assertEquals(actualResourceEstimates.getCpuTime(), FAKE_CPU_TIME);
        assertEquals(actualResourceEstimates.getPeakMemory(), FAKE_PEAK_MEMORY);
        assertEquals(actualResourceEstimates.getPeakTaskMemory(), FAKE_PEAK_TASK_MEMORY);
        assertEquals(actualSession.getStartTime(), FAKE_START_TIME);
        assertEquals(actualSession.getSystemProperties(), FAKE_SYSTEM_PROPERTIES);
        assertEquals(actualSession.getCatalogProperties(), FAKE_CATALOG_PROPERTIES);
        assertEquals(actualSession.getUnprocessedCatalogProperties(), FAKE_UNPROCESSED_CATALOG_PROPERTIES);
        assertEquals(actualSession.getRoles(), FAKE_ROLES);
        assertEquals(actualSession.getPreparedStatements(), FAKE_PREPARED_STATEMENTS);
        assertEquals(actualSession.getSessionFunctions(), getSessionFunctions());

        //Assertions for extraCredentials
        assertEquals(taskUpdateRequest.getExtraCredentials(), FAKE_EXTRA_CREDENTIALS);

        //Assertions for fragment
        assertNotNull(taskUpdateRequest.getFragment().orElse(null));
        assertTrue(Arrays.equals(taskUpdateRequest.getFragment().orElse(null), FAKE_FRAGMENT.orElse(null)));

        //Assertions for sources
        //Assertions for outputIDs
        //Assertions for tableWriteInfo
    }

    private TaskUpdateRequest getRoundTripSerialize(ThriftCodec<TaskUpdateRequest> readCodec, ThriftCodec<TaskUpdateRequest> writeCodec, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        TProtocol protocol = protocolFactory.apply(transport);
        writeCodec.write(taskUpdateRequest, protocol);
        return readCodec.read(protocol);
    }

    private TaskUpdateRequest getTaskUpdateRequest()
    {
        return new TaskUpdateRequest(
                getSession(),
                FAKE_EXTRA_CREDENTIALS,
                FAKE_FRAGMENT
        );
    }

    private SessionRepresentation getSession() {
        ResourceEstimates resourceEstimates = new ResourceEstimates(
                FAKE_EXECUTION_TIME,
                FAKE_CPU_TIME,
                FAKE_PEAK_MEMORY,
                FAKE_PEAK_TASK_MEMORY);
        Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions = getSessionFunctions();
        return new SessionRepresentation(
                FAKE_QUERY_ID,
                FAKE_TRANSACTION_ID,
                FAKE_CLIENT_TRANSACTION_SUPPORT,
                FAKE_USER,
                FAKE_PRINCIPAL,
                FAKE_SOURCE,
                FAKE_CATALOG,
                FAKE_SCHEMA,
                FAKE_TRACE_TOKEN,
                FAKE_TIME_ZONE_KEY,
                FAKE_LOCALE,
                FAKE_REMOTE_USER_ADDRESS,
                FAKE_USER_AGENT,
                FAKE_CLIENT_INFO,
                FAKE_CLIENT_TAGS,
                resourceEstimates,
                FAKE_START_TIME,
                FAKE_SYSTEM_PROPERTIES,
                FAKE_CATALOG_PROPERTIES,
                FAKE_UNPROCESSED_CATALOG_PROPERTIES,
                FAKE_ROLES,
                FAKE_PREPARED_STATEMENTS,
                sessionFunctions
        );
    }

    private Map<SqlFunctionId, SqlInvokedFunction> getSessionFunctions() {
        List<Parameter> parameters = ImmutableList.of(new Parameter(FAKE_NAME, FAKE_TYPE_SIGNATURE));
        RoutineCharacteristics routineCharacteristics = new RoutineCharacteristics(
                Optional.of(RoutineCharacteristics.Language.SQL),
                Optional.of(RoutineCharacteristics.Determinism.DETERMINISTIC),
                Optional.of(RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT)
        );
        SqlInvokedFunction sqlInvokedFunction = getSqlInvokedFunction(parameters, routineCharacteristics);
        return ImmutableMap.of(FAKE_SQL_FUNCTION_ID, sqlInvokedFunction);
    }

    private SqlInvokedFunction getSqlInvokedFunction(List<Parameter> parameters, RoutineCharacteristics routineCharacteristics) {
        Signature signature = new Signature(
                FAKE_QUALIFIED_OBJECT_NAME,
                FunctionKind.AGGREGATE,
                FAKE_TYPE_CONSTRAINT_LIST,
                FAKE_LONG_CONSTRAINT_LIST,
                FAKE_TYPE_SIGNATURE,
                FAKE_ARGUMENT_TYPES,
                FAKE_VARIABLE_ARITY
        );
        return new SqlInvokedFunction(
                parameters,
                FAKE_DESCRIPTION,
                routineCharacteristics,
                FAKE_BODY,
                signature,
                FAKE_SQL_FUNCTION_ID
        );
    }
}
