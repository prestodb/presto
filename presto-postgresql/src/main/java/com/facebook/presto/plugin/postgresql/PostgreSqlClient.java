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
package com.facebook.presto.plugin.postgresql;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.type.*;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.BooleanReadFunction;
import com.facebook.presto.plugin.jdbc.ColumnMapping;
import com.facebook.presto.plugin.jdbc.DoubleReadFunction;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.LongReadFunction;
import com.facebook.presto.plugin.jdbc.ObjectReadFunction;
import com.facebook.presto.plugin.jdbc.ObjectWriteFunction;
import com.facebook.presto.plugin.jdbc.QueryBuilder;
import com.facebook.presto.plugin.jdbc.ReadFunction;
import com.facebook.presto.plugin.jdbc.SliceReadFunction;
import com.facebook.presto.plugin.jdbc.SliceWriteFunction;
import com.facebook.presto.plugin.jdbc.WriteMapping;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.DynamicSliceOutput;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.postgresql.Driver;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.util.PGobject;

import javax.inject.Inject;

import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.geospatial.GeometryUtils.wktFromJtsGeometry;
import static com.facebook.presto.geospatial.serde.EsriGeometrySerde.serialize;
import static com.facebook.presto.geospatial.serde.JtsGeometrySerde.deserialize;
import static com.facebook.presto.plugin.base.util.JsonTypeUtil.jsonParse;
import static com.facebook.presto.plugin.base.util.JsonTypeUtil.toJsonValue;
import static com.facebook.presto.plugin.jdbc.ColumnMapping.objectMapping;
import static com.facebook.presto.plugin.jdbc.ColumnMapping.sliceMapping;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static com.facebook.presto.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static com.facebook.presto.plugin.jdbc.WriteMapping.objectMapping;
import static com.facebook.presto.plugin.jdbc.WriteMapping.sliceMapping;
import static com.facebook.presto.plugin.jdbc.util.TypeUtils.arrayDepth;
import static com.facebook.presto.plugin.jdbc.util.TypeUtils.getArrayElementPgTypeName;
import static com.facebook.presto.plugin.jdbc.util.TypeUtils.getJdbcObjectArray;
import static com.facebook.presto.plugin.postgresql.PostgreSqlConfig.ArrayMapping.AS_ARRAY;
import static com.facebook.presto.plugin.postgresql.PostgreSqlConfig.ArrayMapping.AS_JSON;
import static com.facebook.presto.plugin.postgresql.PostgreSqlConfig.ArrayMapping.DISABLED;
import static com.facebook.presto.plugin.postgresql.PostgreSqlSessionProperties.getArrayMapping;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.slice.Slices.wrappedLongArray;
import static java.lang.String.format;
import static java.sql.DatabaseMetaData.columnNoNulls;
import static java.util.Objects.requireNonNull;

public class PostgreSqlClient
        extends BaseJdbcClient
{
    /**
     * @see Array#getResultSet()
     */
    private static final int ARRAY_RESULT_SET_VALUE_COLUMN = 2;
    protected final Type jsonType;
    private static final String DUPLICATE_TABLE_SQLSTATE = "42P07";
    private final Type uuidType;

    @Inject
    public PostgreSqlClient(JdbcConnectorId connectorId, BaseJdbcConfig config, TypeManager typeManager)
    {
        super(connectorId, config, "\"", new DriverConnectionFactory(new Driver(), config));
        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));
        this.uuidType = typeManager.getType(new TypeSignature(StandardTypes.UUID));
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session))) {
            Map<String, Integer> arrayColumnDims = ImmutableMap.of();
            if (getArrayMapping(session) == AS_ARRAY) {
                arrayColumnDims = getArrayColumnDimensions(connection, tableHandle);
            }
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                List<JdbcColumnHandle> handles = new ArrayList<>();
                while (resultSet.next()) {
                    String columnName = resultSet.getString("COLUMN_NAME");
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(resultSet.getInt("DATA_TYPE"),
                            resultSet.getString("TYPE_NAME"),
                            resultSet.getInt("COLUMN_SIZE"),
                            resultSet.getInt("DECIMAL_DIGITS"),
                            Optional.ofNullable(arrayColumnDims.get(columnName)));
                    Optional<ColumnMapping> columnMapping = toPrestoType(session, typeHandle);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        boolean nullable = (resultSet.getInt("NULLABLE") != columnNoNulls);
                        handles.add(new JdbcColumnHandle(connectorId, columnName, typeHandle, columnMapping.get().getType(), nullable, Optional.empty()));
                    }
                }
                if (handles.isEmpty()) {
                    // In rare cases a table might have no columns.
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(handles);
            }
        }
        catch (SQLException se) {
            throw new PrestoException(JDBC_ERROR, se);
        }
    }

    private Map<String, Integer> getArrayColumnDimensions(Connection connection, JdbcTableHandle tableHandle)
            throws SQLException
    {
        String sql = "" +
                "SELECT att.attname, greatest(att.attndims, 1) as attndims " +
                "FROM pg_attribute att " +
                " JOIN pg_type attyp on att.atttypid = attyp.oid" +
                "  JOIN pg_class tbl ON tbl.oid = att.attrelid " +
                "  JOIN pg_namespace ns ON tbl.relnamespace = ns.oid " +
                "WHERE ns.nspname = ? " +
                "AND tbl.relname = ? " +
                "AND attyp.typcategory = 'A' ";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tableHandle.getSchemaName());
            statement.setString(2, tableHandle.getTableName());

            Map<String, Integer> arrayColumnDimensions = new HashMap<>();
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    arrayColumnDimensions.put(resultSet.getString("attname"), resultSet.getInt("attndims"));
                }
            }
            return arrayColumnDimensions;
        }
    }

    @Override
    public PreparedStatement getPreparedStatement(ConnectorSession session, Connection connection, String sql)
            throws SQLException
    {
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(1000);
        return statement;
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape).orElse(null),
                escapeNamePattern(tableName, escape).orElse(null),
                new String[] {"TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE", "PARTITIONED TABLE"});
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (VARBINARY.equals(type)) {
            return sliceMapping("bytea", varbinaryWriteFunction());
        }
        if (type.getTypeSignature().getBase().equals(StandardTypes.JSON)) {
            return sliceMapping("jsonb", jsonWriteFunction());
        }

        if (type instanceof ArrayType && getArrayMapping(session) == AS_ARRAY) {
            Type elementType = ((ArrayType) type).getElementType();
            String elementDataType = toWriteMapping(session, elementType).getDataType();
            return objectMapping(elementDataType + "[]", arrayWriteFunction(session, elementType, getArrayElementPgTypeName(session, this, elementType)));
        }

        return super.toWriteMapping(session, type);
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        String typeName = typeHandle.getJdbcTypeName();
        int columnSize = typeHandle.getColumnSize();

        switch (typeName) {
            case "uuid":
                if (columnSize > VarcharType.MAX_LENGTH) {
                    return Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
                }
                return Optional.of(varcharColumnMapping(createVarcharType(columnSize)));
            case "geometry":
                return Optional.of(geometryColumnMapping());
            case "jsonb":
            case "json":
                return Optional.of(jsonColumnMapping());
        }
        if (typeHandle.getJdbcType() == Types.ARRAY) {
            PostgreSqlConfig.ArrayMapping arrayMapping = getArrayMapping(session);
            if (arrayMapping == DISABLED) {
                return Optional.empty();
            }

            //resolve and map base array element type
            JdbcTypeHandle baseElementTypeHandle = getArrayElementTypeHandle(session, typeHandle);
            String baseElementTypeName = baseElementTypeHandle.getJdbcTypeName();
            if (baseElementTypeHandle.getJdbcType() == Types.VARBINARY) {
                // PostgreSQL jdbc driver doesn't currently support array of varbinary (bytea[])
                // https://github.com/pgjdbc/pgjdbc/pull/1184
                return Optional.empty();
            }
            Optional<ColumnMapping> baseElementMapping = toPrestoType(session, baseElementTypeHandle);
            if (arrayMapping == AS_ARRAY) {
                if (!typeHandle.getArrayDimensions().isPresent()) {
                    return Optional.empty();
                }
                return baseElementMapping.map(elementMapping -> {
                    ArrayType prestoArrayType = new ArrayType(elementMapping.getType());
                    ColumnMapping arrayColumnMapping = arrayColumnMapping(session, prestoArrayType, elementMapping, baseElementTypeName);
                    int arrayDimensions = typeHandle.getArrayDimensions().get();
                    for (int i = 1; i < arrayDimensions; i++) {
                        prestoArrayType = new ArrayType(prestoArrayType);
                        arrayColumnMapping = arrayColumnMapping(session, prestoArrayType, arrayColumnMapping, baseElementTypeName);
                    }
                    return arrayColumnMapping;
                });
            }
            if (arrayMapping == AS_JSON) {
                return baseElementMapping.map(elementMapping -> arrayAsJsonColumnMapping(session, elementMapping));
            }
            throw new IllegalArgumentException(format("Unsupported array mapping type provided: %s", arrayMapping));
        }
        return super.toPrestoType(session, typeHandle);
    }

    private static ColumnMapping arrayColumnMapping(ConnectorSession session, ArrayType arrayType, ColumnMapping elementMapping, String baseElementJdbcTypeName)
    {
        return objectMapping(arrayType,
                arrayReadFunction(arrayType.getElementType(), elementMapping.getReadFunction()),
                arrayWriteFunction(session, arrayType.getElementType(), baseElementJdbcTypeName));
    }

    private static ObjectWriteFunction arrayWriteFunction(ConnectorSession session, Type elementType, String baseElementJdbcTypeName)
    {
        return ObjectWriteFunction.of(Block.class, (statement, index, value) -> {
            Array jdbcArray = statement.getConnection().createArrayOf(baseElementJdbcTypeName, getJdbcObjectArray(session, elementType, value));
            statement.setArray(index, jdbcArray);
        });
    }

    private static ObjectReadFunction arrayReadFunction(Type elementType, ReadFunction elementReadFunction)
    {
        return ObjectReadFunction.of(Block.class, (resultSet, columnIndex) -> {
            Array array = resultSet.getArray(columnIndex);
            BlockBuilder builder = elementType.createBlockBuilder(null, 10);
            try (ResultSet arrayAsResultSet = array.getResultSet()) {
                while (arrayAsResultSet.next()) {
                    if (elementReadFunction.isNull(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN)) {
                        builder.appendNull();
                    }
                    else if (elementType.getJavaType() == boolean.class) {
                        elementType.writeBoolean(builder, ((BooleanReadFunction) elementReadFunction).readBoolean(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else if (elementType.getJavaType() == long.class) {
                        elementType.writeLong(builder, ((LongReadFunction) elementReadFunction).readLong(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else if (elementType.getJavaType() == double.class) {
                        elementType.writeDouble(builder, ((DoubleReadFunction) elementReadFunction).readDouble(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else if (elementType.getJavaType() == Slice.class) {
                        elementType.writeSlice(builder, ((SliceReadFunction) elementReadFunction).readSlice(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else {
                        elementType.writeObject(builder, ((ObjectReadFunction) elementReadFunction).readObject(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                }
            }
            return builder.build();
        });
    }

    private ColumnMapping arrayAsJsonColumnMapping(ConnectorSession session, ColumnMapping baseElementMapping)
    {
        return sliceMapping(jsonType,
                arrayAsJsonReadFunction(session, baseElementMapping),
                (statement, index, value) -> { throw new UnsupportedOperationException(); });
    }

    private SliceReadFunction arrayAsJsonReadFunction(ConnectorSession session, ColumnMapping baseElementMapping)
    {
        return (resultSet, columnIndex) -> {
            // resolve array type
            Object jdbcArray = resultSet.getArray(columnIndex).getArray();
            int arrayDimensions = arrayDepth(jdbcArray);

            ReadFunction readFunction = baseElementMapping.getReadFunction();
            Type type = baseElementMapping.getType();
            for (int i = 0; i < arrayDimensions; i++) {
                readFunction = arrayReadFunction(type, readFunction);
                type = new ArrayType(type);
            }

            //read array into a block
            Block block = (Block) ((ObjectReadFunction) readFunction).readObject(resultSet, columnIndex);

            //cast block to json slice
            BlockBuilder builder = type.createBlockBuilder(null, 1);
            type.writeObject(builder, block);
            Object value = type.getObjectValue(session.getSqlFunctionProperties(), builder.build(), 0);

            try {
                return toJsonValue(value);
            }
            catch (IOException ioe) {
                throw new PrestoException(JDBC_ERROR, "Conversion to json failed for " + type.getDisplayName(), ioe);
            }
        };
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles) throws SQLException
    {
        ImmutableMap.Builder<String, String> columnExpressions = ImmutableMap.builder();
        for (JdbcColumnHandle column : columnHandles) {
            JdbcTypeHandle jdbcTypeHandle = column.getJdbcTypeHandle();
            if (jdbcTypeHandle.getJdbcTypeName().equalsIgnoreCase("geometry") ||
                    jdbcTypeHandle.getJdbcTypeName().equalsIgnoreCase("geography")) {
                String columnName = column.getColumnName();
                columnExpressions.put(columnName, "ST_AsBinary(\"" + columnName + "\")");
            }
        }
        return new QueryBuilder(identifierQuote).buildSql(
                this,
                session,
                connection,
                split.getCatalogName(),
                split.getSchemaName(),
                split.getTableName(),
                columnHandles,
                columnExpressions.build(),
                split.getTupleDomain(),
                split.getAdditionalPredicate());
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            createTable(tableMetadata, session, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            if (DUPLICATE_TABLE_SQLSTATE.equals(e.getSQLState())) {
                throw new PrestoException(ALREADY_EXISTS, e);
            }
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, SchemaTableName oldTable, SchemaTableName newTable)
    {
        // PostgreSQL does not allow qualifying the target of a rename
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String sql = format(
                    "ALTER TABLE %s RENAME TO %s",
                    quoted(catalogName, oldTable.getSchemaName(), oldTable.getTableName()),
                    quoted(newTable.getTableName()));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    private ColumnMapping jsonColumnMapping()
    {
        return sliceMapping(
                jsonType,
                (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))),
                jsonWriteFunction());
    }

    private SliceWriteFunction jsonWriteFunction()
    {
        return ((statement, index, value) -> {
            PGobject pgObject = new PGobject();
            pgObject.setType("json");
            pgObject.setValue(value.toStringUtf8());
            statement.setObject(index, pgObject);
        });
    }

    private JdbcTypeHandle getArrayElementTypeHandle(ConnectorSession session, JdbcTypeHandle arrayTypeHandle)
    {
        // PostgreSql array type names are the base element type prepended with "_"
        checkArgument(arrayTypeHandle.getJdbcTypeName().startsWith("_"), "array type must start with '_'");
        String elementPgTypeName = arrayTypeHandle.getJdbcTypeName().substring(1);
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session))) {
            int elementJdbcType = connection.unwrap(PgConnection.class).getTypeInfo().getSQLType(elementPgTypeName);
            return new JdbcTypeHandle(
                    elementJdbcType,
                    elementPgTypeName,
                    arrayTypeHandle.getColumnSize(),
                    arrayTypeHandle.getDecimalDigits(),
                    arrayTypeHandle.getArrayDimensions());
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    private ColumnMapping geometryColumnMapping()
    {
        return sliceMapping(
                VARCHAR,
                (resultSet, columnIndex) -> getAsText(getGeomFromBinary(wrappedBuffer(resultSet.getBytes(columnIndex)))),
                geometryWriteFunction());
    }

    private SliceWriteFunction geometryWriteFunction()
    {
        //TODO: see if this can be implemented in some way.
        return ((statement, index, value) -> {
            throw new UnsupportedOperationException();
        });
    }

    private static Slice getAsText(Slice input)
    {
        return utf8Slice(wktFromJtsGeometry(deserialize(input)));
    }

    private static Slice getGeomFromBinary(Slice input)
    {
        requireNonNull(input, "input is null");
        OGCGeometry geometry;
        try {
            geometry = fromBinary(input.toByteBuffer().slice());
        }
        catch (IllegalArgumentException | IndexOutOfBoundsException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid WKB", e);
        }
        geometry.setSpatialReference(null);
        return serialize(geometry);
    }
}
