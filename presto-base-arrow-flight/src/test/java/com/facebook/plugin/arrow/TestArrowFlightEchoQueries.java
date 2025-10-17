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
package com.facebook.plugin.arrow;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.plugin.arrow.testingServer.TestingArrowFlightRequest;
import com.facebook.plugin.arrow.testingServer.TestingArrowFlightResponse;
import com.facebook.presto.Session;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.arrow.vector.util.DictionaryUtility;
import org.apache.arrow.vector.util.Text;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.SystemSessionProperties.LEGACY_TIMESTAMP;
import static com.facebook.presto.common.block.MethodHandleUtil.compose;
import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingEnvironment.getOperatorMethodHandle;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampWithoutTimeZone;
import static java.lang.String.format;
import static java.nio.channels.Channels.newChannel;

public class TestArrowFlightEchoQueries
        extends AbstractTestQueryFramework
{
    private static final Logger logger = Logger.get(TestArrowFlightEchoQueries.class);
    private static final CallOption CALL_OPTIONS = CallOptions.timeout(300, TimeUnit.SECONDS);
    private int serverPort;
    private RootAllocator allocator;
    private FlightServer server;
    private DistributedQueryRunner arrowFlightQueryRunner;
    private JsonCodec<TestingArrowFlightRequest> requestCodec;
    private JsonCodec<TestingArrowFlightResponse> responseCodec;

    @BeforeClass
    public void setup()
            throws Exception
    {
        arrowFlightQueryRunner = getDistributedQueryRunner();
        File certChainFile = new File("src/test/resources/certs/server.crt");
        File privateKeyFile = new File("src/test/resources/certs/server.key");

        allocator = new RootAllocator(Long.MAX_VALUE);

        requestCodec = jsonCodec(TestingArrowFlightRequest.class);
        responseCodec = jsonCodec(TestingArrowFlightResponse.class);

        Location location = Location.forGrpcTls("localhost", serverPort);
        server = FlightServer.builder(allocator, location, new TestingEchoFlightProducer(allocator, requestCodec, responseCodec))
                .useTls(certChainFile, privateKeyFile)
                .build();

        server.start();
        logger.info("Server listening on port %s", server.getPort());
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws InterruptedException
    {
        arrowFlightQueryRunner.close();
        server.close();
        allocator.close();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        serverPort = ArrowFlightQueryRunner.findUnusedPort();
        return ArrowFlightQueryRunner.createQueryRunner(serverPort);
    }

    @Test
    public void testDateTimeVectors() throws Exception
    {
        // Disable legacy timestamp behavior
        Session sessionStandardTimestamps = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();

        try (BufferAllocator bufferAllocator = allocator.newChildAllocator("echo-test-client", 0, Long.MAX_VALUE);
                IntVector intVector = new IntVector("id", bufferAllocator);
                DateDayVector dateVector = new DateDayVector("date", bufferAllocator);
                TimeMilliVector timeVector = new TimeMilliVector("time", bufferAllocator);
                TimeStampMilliVector timestampVector = new TimeStampMilliVector("timestamp", bufferAllocator);
                VectorSchemaRoot root = new VectorSchemaRoot(Arrays.asList(intVector, dateVector, timeVector, timestampVector));
                FlightClient client = createFlightClient(bufferAllocator, serverPort)) {
            MaterializedResult.Builder expectedBuilder = resultBuilder(getSession(), INTEGER, DATE, TIME, TIMESTAMP);

            intVector.setSafe(0, 0);
            dateVector.setSafe(0, 0);
            timeVector.setSafe(0, 0);
            timestampVector.setSafe(0, 0);
            expectedBuilder.row(0, LocalDate.parse("1970-01-01"), LocalTime.of(0, 0), LocalDateTime.parse("1970-01-01T00:00:00"));

            intVector.setSafe(1, 1);
            LocalDate date = LocalDate.of(2024, 1, 1);
            dateVector.setSafe(1, (int) date.toEpochDay());
            LocalTime time = LocalTime.of(1, 1, 1);
            timeVector.setSafe(1, (int) TimeUnit.NANOSECONDS.toMillis(time.toNanoOfDay()));
            timestampVector.setSafe(1, parseTimestampWithoutTimeZone("2024-01-01 01:01:01"));
            expectedBuilder.row(1, date, time, LocalDateTime.parse("2024-01-01T01:01:01"));

            intVector.setSafe(2, 2);
            date = LocalDate.of(2024, 1, 2);
            dateVector.setSafe(2, (int) date.toEpochDay());
            time = LocalTime.of(2, 12);
            timeVector.setSafe(2, (int) TimeUnit.NANOSECONDS.toMillis(time.toNanoOfDay()));
            timestampVector.setSafe(2, parseTimestampWithoutTimeZone("2024-01-02 12:00:00"));
            expectedBuilder.row(2, date, time, LocalDateTime.parse("2024-01-02T12:00:00"));

            intVector.setSafe(3, 3);
            date = LocalDate.of(2112, 12, 31);
            dateVector.setSafe(3, (int) date.toEpochDay());
            time = LocalTime.of(23, 58);
            timeVector.setSafe(3, (int) TimeUnit.NANOSECONDS.toMillis(time.toNanoOfDay()));
            timestampVector.setSafe(3, parseTimestampWithoutTimeZone("2112-12-31 23:58:00"));
            expectedBuilder.row(3, date, time, LocalDateTime.parse("2112-12-31T23:58:00"));

            intVector.setSafe(4, 4);
            date = LocalDate.of(1968, 7, 5);
            dateVector.setSafe(4, (int) date.toEpochDay());
            time = LocalTime.of(8, 15, 12, (int) TimeUnit.MILLISECONDS.toNanos(345));
            timeVector.setSafe(4, (int) TimeUnit.NANOSECONDS.toMillis(time.toNanoOfDay()));
            timestampVector.setSafe(4, parseTimestampWithoutTimeZone("1968-07-05 08:15:12.345"));
            expectedBuilder.row(4, date, time, LocalDateTime.parse("1968-07-05T08:15:12.345"));

            root.setRowCount(5);

            String tableName = "datetime";
            addTableToServer(client, root, tableName);

            MaterializedResult actual = computeActual(sessionStandardTimestamps, format("SELECT * FROM %s", tableName));
            MaterializedResult expected = expectedBuilder.build();

            assertEquals(actual.getRowCount(), root.getRowCount());
            assertEquals(actual, expected);

            removeTableFromServer(client, tableName);
        }
    }

    @Test
    public void testVarCharVector() throws Exception
    {
        try (BufferAllocator bufferAllocator = allocator.newChildAllocator("echo-test-client", 0, Long.MAX_VALUE);
                IntVector intVector = new IntVector("id", bufferAllocator);
                VarCharVector stringVector = new VarCharVector("c", bufferAllocator);
                VectorSchemaRoot root = new VectorSchemaRoot(Arrays.asList(intVector, stringVector));
                FlightClient client = createFlightClient(bufferAllocator, serverPort)) {
            MaterializedResult.Builder expectedBuilder = resultBuilder(getSession(), INTEGER, VARCHAR);

            final int numValues = 10;
            final String stringData = "abcdefghijklmnopqrstuvwxyz";
            for (int i = 0; i < numValues; i++) {
                intVector.setSafe(i, i);
                String value = stringData.substring(0, i % stringData.length());
                stringVector.setSafe(i, new Text(value));
                expectedBuilder.row(i, value);
            }
            root.setRowCount(numValues);

            String tableName = "varchar";
            addTableToServer(client, root, tableName);

            MaterializedResult actual = computeActual(format("SELECT * FROM %s", tableName));

            assertEquals(actual.getRowCount(), numValues);
            assertEquals(actual, expectedBuilder.build());

            removeTableFromServer(client, tableName);
        }
    }

    @Test
    public void testListVector() throws Exception
    {
        try (BufferAllocator bufferAllocator = allocator.newChildAllocator("echo-test-client", 0, Long.MAX_VALUE);
                IntVector intVector = new IntVector("id", bufferAllocator);
                ListVector listVectorInt = ListVector.empty("array-int", bufferAllocator);
                ListVector listVectorVarchar = ListVector.empty("array-varchar", bufferAllocator)) {
            // Add the element vectors
            listVectorInt.addOrGetVector(FieldType.nullable(Types.MinorType.INT.getType()));
            listVectorVarchar.addOrGetVector(FieldType.nullable(Types.MinorType.VARCHAR.getType()));
            listVectorInt.allocateNew();
            listVectorVarchar.allocateNew();

            try (VectorSchemaRoot expectedRoot = new VectorSchemaRoot(Arrays.asList(intVector, listVectorInt, listVectorVarchar));
                    FlightClient client = createFlightClient(bufferAllocator, serverPort)) {
                MaterializedResult.Builder expectedBuilder = resultBuilder(getSession(), INTEGER, new ArrayType(INTEGER), new ArrayType(VARCHAR));

                final int numValues = 10;
                final String stringData = "abcdefghijklmnopqrstuvwxyz";
                final UnionListWriter writerInt = listVectorInt.getWriter();
                final UnionListWriter writerVarchar = listVectorVarchar.getWriter();
                for (int i = 0; i < numValues; i++) {
                    intVector.setSafe(i, i);

                    List<Integer> intArray = new ArrayList<>();
                    List<String> stringArray = new ArrayList<>();
                    writerInt.setPosition(i);
                    writerInt.startList();
                    writerVarchar.startList();
                    for (int j = 0; j < i % 4; j++) {
                        writerInt.integer().writeInt(i * j);
                        String stringValue = stringData.substring(0, i % stringData.length());
                        writerVarchar.writeVarChar(new Text(stringValue));
                        intArray.add(i * j);
                        stringArray.add(stringValue);
                    }
                    writerInt.endList();
                    writerVarchar.endList();

                    expectedBuilder.row(i, intArray, stringArray);
                }
                expectedRoot.setRowCount(numValues);

                String tableName = "arrays";
                addTableToServer(client, expectedRoot, tableName);

                MaterializedResult actual = computeActual(format("SELECT * FROM %s", tableName));

                assertEquals(actual.getRowCount(), numValues);
                assertEquals(actual, expectedBuilder.build());

                removeTableFromServer(client, tableName);
            }
        }
    }

    @Test
    public void testMapVector() throws Exception
    {
        try (BufferAllocator bufferAllocator = allocator.newChildAllocator("echo-test-client", 0, Long.MAX_VALUE);
                IntVector intVector = new IntVector("id", bufferAllocator);
                MapVector mapVector = MapVector.empty("map-int-long", bufferAllocator, false)) {
            UnionMapWriter mapWriter = mapVector.getWriter();
            mapWriter.allocate();

            MaterializedResult.Builder expectedBuilder = resultBuilder(getSession(), INTEGER, createMapType(INTEGER, BIGINT));

            final int numValues = 10;
            for (int i = 0; i < numValues; i++) {
                intVector.setSafe(i, i);
                mapWriter.setPosition(i);
                mapWriter.startMap();

                Map<Integer, Long> expectedMap = new HashMap<>();
                for (int j = 0; j < i; j++) {
                    mapWriter.startEntry();
                    mapWriter.key().integer().writeInt(j);
                    mapWriter.value().bigInt().writeBigInt(i * j);
                    mapWriter.endEntry();
                    expectedMap.put(j, (long) i * j);
                }
                mapWriter.endMap();
                expectedBuilder.row(i, expectedMap);
            }
            mapWriter.setValueCount(numValues);

            try (VectorSchemaRoot expectedRoot = new VectorSchemaRoot(Arrays.asList(intVector, mapVector));
                    FlightClient client = createFlightClient(bufferAllocator, serverPort)) {
                expectedRoot.setRowCount(numValues);

                String tableName = "map";
                addTableToServer(client, expectedRoot, tableName);

                MaterializedResult actual = computeActual(format("SELECT * FROM %s", tableName));
                assertEquals(actual.getRowCount(), numValues);
                assertEquals(actual, expectedBuilder.build());

                removeTableFromServer(client, tableName);
            }
        }
    }

    @Test
    public void testStructVector() throws Exception
    {
        try (BufferAllocator bufferAllocator = allocator.newChildAllocator("echo-test-client", 0, Long.MAX_VALUE);
                IntVector intVector = new IntVector("id", bufferAllocator);
                StructVector structVector = StructVector.empty("struct", bufferAllocator)) {
            MaterializedResult.Builder expectedBuilder = resultBuilder(getSession(), INTEGER,
                    RowType.from(ImmutableList.of(
                            new RowType.Field(Optional.of("int"), INTEGER),
                            new RowType.Field(Optional.of("long"), BIGINT))));

            final IntVector childIntVector
                    = structVector.addOrGet("int", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
            final BigIntVector childLongVector
                    = structVector.addOrGet("long", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);
            childIntVector.allocateNew();
            childLongVector.allocateNew();

            final int numValues = 10;
            for (int i = 0; i < numValues; i++) {
                intVector.setSafe(i, i);
                childIntVector.setSafe(i, i + i);
                childLongVector.setSafe(i, i * i);
                structVector.setIndexDefined(i);
                expectedBuilder.row(i, ImmutableList.of(i + i, (long) i * i));
            }

            try (VectorSchemaRoot expectedRoot = new VectorSchemaRoot(Arrays.asList(intVector, structVector));
                    FlightClient client = createFlightClient(bufferAllocator, serverPort)) {
                expectedRoot.setRowCount(numValues);

                String tableName = "structs";
                addTableToServer(client, expectedRoot, tableName);

                MaterializedResult actual = computeActual(format("SELECT * FROM %s", tableName));

                assertEquals(actual.getRowCount(), numValues);
                assertEquals(actual, expectedBuilder.build());

                removeTableFromServer(client, tableName);
            }
        }
    }

    @Test
    public void testDictionaryVector() throws Exception
    {
        try (BufferAllocator bufferAllocator = allocator.newChildAllocator("echo-test-client", 0, Long.MAX_VALUE);
                IntVector intVector = new IntVector("id", bufferAllocator);
                VarCharVector rawVector = new VarCharVector("varchar", bufferAllocator);
                VarCharVector dictionaryVector = new VarCharVector("dictionary", bufferAllocator)) {
            intVector.allocateNew();
            rawVector.allocateNew();
            dictionaryVector.allocateNew(3); // allocating 3 elements in dictionary

            // Fill dictionaryVector with some values
            dictionaryVector.set(0, "apple".getBytes());
            dictionaryVector.set(1, "banana".getBytes());
            dictionaryVector.set(2, "cherry".getBytes());
            dictionaryVector.setValueCount(3);

            MaterializedResult.Builder expectedBuilder = resultBuilder(getSession(), INTEGER, VARCHAR);

            final int numValues = 10;
            for (int i = 0; i < numValues; i++) {
                intVector.setSafe(i, i);
                Text rawValue = dictionaryVector.getObject((numValues - i) % dictionaryVector.getValueCount());
                rawVector.setSafe(i, rawValue);
                expectedBuilder.row(i, rawValue.toString());
            }
            rawVector.setValueCount(numValues);

            Dictionary dictionary = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));

            try (FieldVector encodedVector = (FieldVector) DictionaryEncoder.encode(rawVector, dictionary);
                    VectorSchemaRoot root = new VectorSchemaRoot(Arrays.asList(intVector, encodedVector));
                    DictionaryProvider.MapDictionaryProvider dictionaryProvider = new DictionaryProvider.MapDictionaryProvider(dictionary);
                    FlightClient client = createFlightClient(bufferAllocator, serverPort)) {
                root.setRowCount(numValues);

                String tableName = "dictionary";
                addTableToServer(client, root, tableName, dictionaryProvider);

                MaterializedResult actual = computeActual(format("SELECT * FROM %s", tableName));

                assertEquals(actual.getRowCount(), numValues);
                assertEquals(actual, expectedBuilder.build());

                removeTableFromServer(client, tableName);
            }
        }
    }

    private static MapType createMapType(Type keyType, Type valueType)
    {
        MethodHandle keyNativeEquals = getOperatorMethodHandle(OperatorType.EQUAL, keyType, keyType);
        MethodHandle keyBlockEquals = compose(keyNativeEquals, nativeValueGetter(keyType), nativeValueGetter(keyType));
        MethodHandle keyNativeHashCode = getOperatorMethodHandle(OperatorType.HASH_CODE, keyType);
        MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(keyType));

        return new MapType(
                keyType,
                valueType,
                keyBlockEquals,
                keyBlockHashCode);
    }

    private static FlightClient createFlightClient(BufferAllocator allocator, int serverPort) throws IOException
    {
        InputStream trustedCertificate = new ByteArrayInputStream(Files.readAllBytes(Paths.get("src/test/resources/certs/server.crt")));
        Location location = Location.forGrpcTls("localhost", serverPort);
        return FlightClient.builder(allocator, location).useTls().trustedCertificates(trustedCertificate).build();
    }

    private void addTableToServer(FlightClient client, VectorSchemaRoot root, String tableName)
    {
        addTableToServer(client, root, tableName, null);
    }

    private void addTableToServer(FlightClient client, VectorSchemaRoot root, String tableName, DictionaryProvider dictionaryProvider)
    {
        TestingArrowFlightRequest putRequest = new TestingArrowFlightRequest(Optional.empty(), Optional.of(tableName), Optional.empty());
        final FlightClient.ClientStreamListener stream;

        if (dictionaryProvider == null) {
            stream = client.startPut(FlightDescriptor.command(requestCodec.toJsonBytes(putRequest)),
                    root, new AsyncPutListener(), CALL_OPTIONS);
        }
        else {
            stream = client.startPut(FlightDescriptor.command(requestCodec.toJsonBytes(putRequest)),
                    root, dictionaryProvider, new AsyncPutListener(), CALL_OPTIONS);
        }
        stream.putNext();
        stream.completed();
        stream.getResult();
    }

    private void removeTableFromServer(FlightClient client, String tableName)
    {
        TestingArrowFlightRequest dropRequest = new TestingArrowFlightRequest(Optional.empty(), Optional.of(tableName), Optional.empty());
        Iterator<Result> iterator = client.doAction(new Action("drop", requestCodec.toJsonBytes(dropRequest)), CALL_OPTIONS);
        iterator.hasNext();
    }

    private static class TestingEchoFlightProducer
            extends NoOpFlightProducer
    {
        private final BufferAllocator allocator;
        private final Map<String, byte[]> tableMap = new ConcurrentHashMap<>();
        private final JsonCodec<TestingArrowFlightRequest> requestCodec;
        private final JsonCodec<TestingArrowFlightResponse> responseCodec;

        public TestingEchoFlightProducer(BufferAllocator allocator, JsonCodec<TestingArrowFlightRequest> requestCodec, JsonCodec<TestingArrowFlightResponse> responseCodec)
        {
            this.allocator = allocator;
            this.requestCodec = requestCodec;
            this.responseCodec = responseCodec;
        }

        public Runnable acceptPut(FlightProducer.CallContext context, FlightStream flightStream, FlightProducer.StreamListener<PutResult> ackStream)
        {
            return () -> {
                TestingArrowFlightRequest request = requestCodec.fromJson(flightStream.getDescriptor().getCommand());
                if (!request.getTable().isPresent()) {
                    throw new IllegalArgumentException("Table name must be specified");
                }

                final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                try (ArrowStreamWriter writer = new ArrowStreamWriter(flightStream.getRoot(), flightStream.getDictionaryProvider(), newChannel(outputStream))) {
                    while (flightStream.next()) {
                        writer.writeBatch();
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException("Error receiving table batches", e);
                }

                tableMap.put(request.getTable().get(), outputStream.toByteArray());
            };
        }

        @Override
        public void doAction(CallContext context, Action action, StreamListener<Result> listener)
        {
            try {
                TestingArrowFlightRequest request = requestCodec.fromJson(action.getBody());

                if ("discovery".equals(action.getType())) {
                    TestingArrowFlightResponse response;
                    if (!request.getSchema().isPresent()) {
                        // Return the list of schemas
                        response = new TestingArrowFlightResponse(ImmutableList.of("tpch"), ImmutableList.of());
                    }
                    else {
                        // Return the list of tables
                        response = new TestingArrowFlightResponse(ImmutableList.of(), new ArrayList<>(tableMap.keySet()));
                    }

                    listener.onNext(new Result(responseCodec.toJsonBytes(response)));
                    listener.onCompleted();
                }
                else if ("drop".equals(action.getType())) {
                    if (!request.getTable().isPresent() || null == tableMap.remove(request.getTable().get())) {
                        listener.onError(CallStatus.INVALID_ARGUMENT.withDescription("Table not found: " + request.getTable()).toRuntimeException());
                    }
                    listener.onCompleted();
                }
                else {
                    listener.onError(CallStatus.INVALID_ARGUMENT.withDescription("Invalid action: " + action.getType() + ", request: " + request.toString()).toRuntimeException());
                }
            }
            catch (Exception e) {
                listener.onError(e);
            }
        }

        @Override
        public FlightInfo getFlightInfo(CallContext callContext, FlightDescriptor flightDescriptor)
        {
            TestingArrowFlightRequest request = requestCodec.fromJson(flightDescriptor.getCommand());

            if (!request.getTable().isPresent()) {
                throw new IllegalArgumentException("Table name must be specified");
            }

            if (!tableMap.containsKey(request.getTable().get())) {
                throw new IllegalArgumentException("Unknown table requested");
            }

            byte[] arrowFileBytes = tableMap.get(request.getTable().get());

            Schema schema;
            try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayReadableSeekableByteChannel(arrowFileBytes), allocator)) {
                schema = generateSchema(reader.getVectorSchemaRoot().getSchema(), reader, new TreeSet<>());
            }
            catch (IOException e) {
                throw new RuntimeException("Error deserializing Arrow file", e);
            }

            FlightEndpoint endpoint = new FlightEndpoint(new Ticket(request.getTable().get().getBytes(StandardCharsets.UTF_8)));
            return new FlightInfo(schema, flightDescriptor, Collections.singletonList(endpoint), -1, -1);
        }

        @Override
        public void getStream(CallContext callContext, Ticket ticket, ServerStreamListener serverStreamListener)
        {
            String tableName = new String(ticket.getBytes(), StandardCharsets.UTF_8);

            if (!tableMap.containsKey(tableName)) {
                throw new IllegalArgumentException("Unknown table requested");
            }

            byte[] arrowFileBytes = tableMap.get(tableName);

            try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayReadableSeekableByteChannel(arrowFileBytes), allocator)) {
                boolean started = false;
                // NOTE: need to read first batch to initialize dictionaries
                while (reader.loadNextBatch()) {
                    if (!started) {
                        serverStreamListener.start(reader.getVectorSchemaRoot(), reader);
                        started = true;
                    }
                    serverStreamListener.putNext();
                }
                serverStreamListener.completed();
            }
            catch (IOException e) {
                throw new RuntimeException("Error deserializing Arrow file", e);
            }
        }

        /**
         * From org.apache.arrow.flight.DictionaryUtils which is package private
         */
        static Schema generateSchema(
                final Schema originalSchema, final DictionaryProvider provider, Set<Long> dictionaryIds)
        {
            // first determine if a new schema needs to be created.
            boolean createSchema = false;
            for (Field field : originalSchema.getFields()) {
                if (DictionaryUtility.needConvertToMessageFormat(field)) {
                    createSchema = true;
                    break;
                }
            }

            if (!createSchema) {
                return originalSchema;
            }
            else {
                final List<Field> fields = new ArrayList<>(originalSchema.getFields().size());
                for (final Field field : originalSchema.getFields()) {
                    fields.add(DictionaryUtility.toMessageFormat(field, provider, dictionaryIds));
                }
                return new Schema(fields, originalSchema.getCustomMetadata());
            }
        }
    }
}
