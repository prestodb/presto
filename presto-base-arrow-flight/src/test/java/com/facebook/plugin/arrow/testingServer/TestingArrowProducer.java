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

package com.facebook.plugin.arrow.testingServer;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.common.Utils.checkArgument;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowUtils.jdbcToArrowSchema;

public class TestingArrowProducer
        implements FlightProducer
{
    private final BufferAllocator allocator;
    private final Connection connection;
    private static final Logger logger = Logger.get(TestingArrowProducer.class);
    private final JsonCodec<TestingArrowFlightRequest> requestCodec;
    private final JsonCodec<TestingArrowFlightResponse> responseCodec;

    public TestingArrowProducer(BufferAllocator allocator) throws Exception
    {
        this.allocator = allocator;
        String h2JdbcUrl = "jdbc:h2:mem:testdb" + System.nanoTime() + "_" + ThreadLocalRandom.current().nextInt() + ";DB_CLOSE_DELAY=-1";
        TestingH2DatabaseSetup.setup(h2JdbcUrl);
        this.connection = DriverManager.getConnection(h2JdbcUrl, "sa", "");
        this.requestCodec = jsonCodec(TestingArrowFlightRequest.class);
        this.responseCodec = jsonCodec(TestingArrowFlightResponse.class);
    }

    @Override
    public void getStream(CallContext callContext, Ticket ticket, ServerStreamListener serverStreamListener)
    {
        try (Statement stmt = connection.createStatement()) {
            TestingArrowFlightRequest request = requestCodec.fromJson(ticket.getBytes());
            checkArgument(request != null, "Request is null");
            checkArgument(request.getQuery().isPresent(), "Query is missing");

            // Extract and validate the SQL query
            String query = request.getQuery().get();
            if (query.trim().isEmpty()) {
                throw new IllegalArgumentException("Query cannot be empty.");
            }

            logger.debug("Executing query: %s", query);

            try (ResultSet resultSet = stmt.executeQuery(query.toUpperCase())) {
                JdbcToArrowConfig config = new JdbcToArrowConfigBuilder().setAllocator(allocator).setTargetBatchSize(2048)
                        .setCalendar(Calendar.getInstance(TimeZone.getDefault())).build();
                Schema schema = jdbcToArrowSchema(resultSet.getMetaData(), config);
                try (VectorSchemaRoot streamRoot = VectorSchemaRoot.create(schema, allocator)) {
                    VectorLoader loader = new VectorLoader(streamRoot);
                    serverStreamListener.start(streamRoot);
                    ArrowVectorIterator iterator = JdbcToArrow.sqlToArrowVectorIterator(resultSet, config);

                    while (iterator.hasNext()) {
                        try (VectorSchemaRoot iteratorRoot = iterator.next()) {
                            VectorUnloader vectorUnloader = new VectorUnloader(iteratorRoot);
                            try (ArrowRecordBatch batch = vectorUnloader.getRecordBatch()) {
                                loader.load(batch);
                                serverStreamListener.putNext();
                            }
                            streamRoot.clear();
                        }
                    }
                }
                serverStreamListener.completed();
            }
        }
        // Handle Arrow processing errors
        catch (IOException e) {
            logger.error("Arrow data processing failed", e);
            serverStreamListener.error(e);
            throw new RuntimeException("Failed to process Arrow data", e);
        }
        // Handle all other exceptions, including parsing errors
        catch (Exception e) {
            logger.error("Ticket processing failed", e);
            serverStreamListener.error(e);
            throw new RuntimeException("Failed to process the ticket", e);
        }
    }

    @Override
    public void listFlights(CallContext callContext, Criteria criteria, StreamListener<FlightInfo> streamListener)
    {
        throw new UnsupportedOperationException("This operation is not supported");
    }

    @Override
    public FlightInfo getFlightInfo(CallContext callContext, FlightDescriptor flightDescriptor)
    {
        try {
            TestingArrowFlightRequest request = requestCodec.fromJson(flightDescriptor.getCommand());
            checkArgument(request != null, "Request is null");

            checkArgument(request.getSchema().isPresent(), "Schema is missing");
            String schemaName = request.getSchema().get();
            Optional<String> tableName = request.getTable();
            String selectStatement = request.getQuery().orElse(null);

            List<Field> fields = new ArrayList<>();
            if (tableName.isPresent()) {
                String query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS " +
                        "WHERE TABLE_SCHEMA='" + schemaName.toUpperCase() + "' " +
                        "AND TABLE_NAME='" + tableName.get().toUpperCase() + "'";

                try (ResultSet rs = connection.createStatement().executeQuery(query)) {
                    while (rs.next()) {
                        String columnName = rs.getString("COLUMN_NAME");
                        String dataType = rs.getString("TYPE_NAME");
                        String charMaxLength = rs.getString("CHARACTER_MAXIMUM_LENGTH");
                        int precision = rs.getInt("NUMERIC_PRECISION");
                        int scale = rs.getInt("NUMERIC_SCALE");

                        ArrowType arrowType = convertSqlTypeToArrowType(dataType, precision, scale);
                        Map<String, String> metaDataMap = new HashMap<>();
                        metaDataMap.put("columnNativeType", dataType);
                        if (charMaxLength != null) {
                            metaDataMap.put("columnLength", charMaxLength);
                        }
                        FieldType fieldType = new FieldType(true, arrowType, null, metaDataMap);
                        Field field = new Field(columnName, fieldType, null);
                        fields.add(field);
                    }
                }
            }
            else if (selectStatement != null) {
                selectStatement = selectStatement.toUpperCase();
                logger.debug("Executing SELECT query: %s", selectStatement);
                try (ResultSet rs = connection.createStatement().executeQuery(selectStatement)) {
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();

                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        String columnType = metaData.getColumnTypeName(i);
                        int precision = metaData.getPrecision(i);
                        int scale = metaData.getScale(i);

                        ArrowType arrowType = convertSqlTypeToArrowType(columnType, precision, scale);
                        Field field = new Field(columnName, FieldType.nullable(arrowType), null);
                        fields.add(field);
                    }
                }
            }
            else {
                throw new IllegalArgumentException("Either schema_name/table_name or select_statement must be provided.");
            }

            Schema schema = new Schema(fields);
            FlightEndpoint endpoint = new FlightEndpoint(new Ticket(flightDescriptor.getCommand()));
            return new FlightInfo(schema, flightDescriptor, Collections.singletonList(endpoint), -1, -1);
        }
        catch (Exception e) {
            logger.error(e);
            throw new RuntimeException("Failed to retrieve FlightInfo", e);
        }
    }

    @Override
    public Runnable acceptPut(CallContext callContext, FlightStream flightStream, StreamListener<PutResult> streamListener)
    {
        throw new UnsupportedOperationException("This operation is not supported");
    }

    @Override
    public void doAction(CallContext callContext, Action action, StreamListener<Result> streamListener)
    {
        try {
            TestingArrowFlightRequest request = requestCodec.fromJson(action.getBody());
            Optional<String> schemaName = request.getSchema();

            String query;
            if (!schemaName.isPresent()) {
                query = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA";
            }
            else {
                query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='" + schemaName.get().toUpperCase() + "'";
            }
            ResultSet rs = connection.createStatement().executeQuery(query);
            List<String> names = new ArrayList<>();
            while (rs.next()) {
                names.add(rs.getString(1));
            }

            TestingArrowFlightResponse response;
            if (!schemaName.isPresent()) {
                response = new TestingArrowFlightResponse(names, ImmutableList.of());
            }
            else {
                response = new TestingArrowFlightResponse(ImmutableList.of(), names);
            }

            streamListener.onNext(new Result(responseCodec.toJsonBytes(response)));
            streamListener.onCompleted();
        }
        catch (Exception e) {
            streamListener.onError(e);
        }
    }

    @Override
    public void listActions(CallContext callContext, StreamListener<ActionType> streamListener)
    {
        throw new UnsupportedOperationException("This operation is not supported");
    }

    private ArrowType convertSqlTypeToArrowType(String sqlType, int precision, int scale)
    {
        switch (sqlType.toUpperCase()) {
            case "VARCHAR":
            case "CHAR":
            case "CHARACTER VARYING":
            case "CHARACTER":
            case "CLOB":
                return new ArrowType.Utf8();
            case "INTEGER":
            case "INT":
                return new ArrowType.Int(32, true);
            case "BIGINT":
                return new ArrowType.Int(64, true);
            case "SMALLINT":
                return new ArrowType.Int(16, true);
            case "TINYINT":
                return new ArrowType.Int(8, true);
            case "DOUBLE":
            case "DOUBLE PRECISION":
            case "FLOAT":
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case "REAL":
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case "BOOLEAN":
                return new ArrowType.Bool();
            case "DATE":
                return new ArrowType.Date(DateUnit.DAY);
            case "TIMESTAMP":
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            case "TIME":
                return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
            case "DECIMAL":
            case "NUMERIC":
                return new ArrowType.Decimal(precision, scale);
            case "BINARY":
            case "VARBINARY":
                return new ArrowType.Binary();
            case "NULL":
                return new ArrowType.Null();
            default:
                throw new IllegalArgumentException("Unsupported SQL type: " + sqlType);
        }
    }
}
