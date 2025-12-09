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
package com.facebook.presto.plugin.oracle;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.mapping.ReadMapping;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.DoubleRange;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.collect.Maps;
import jakarta.inject.Inject;
import oracle.jdbc.OracleTypes;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.jdbc.mapping.StandardColumnMappings.bigintReadMapping;
import static com.facebook.presto.plugin.jdbc.mapping.StandardColumnMappings.decimalReadMapping;
import static com.facebook.presto.plugin.jdbc.mapping.StandardColumnMappings.doubleReadMapping;
import static com.facebook.presto.plugin.jdbc.mapping.StandardColumnMappings.realReadMapping;
import static com.facebook.presto.plugin.jdbc.mapping.StandardColumnMappings.smallintReadMapping;
import static com.facebook.presto.plugin.jdbc.mapping.StandardColumnMappings.timestampReadMapping;
import static com.facebook.presto.plugin.jdbc.mapping.StandardColumnMappings.varbinaryReadMapping;
import static com.facebook.presto.plugin.jdbc.mapping.StandardColumnMappings.varcharReadMapping;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Double.NaN;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class OracleClient
        extends BaseJdbcClient
{
    private static final Logger LOG = Logger.get(OracleClient.class);
    private static final int FETCH_SIZE = 1000;

    private final boolean synonymsEnabled;
    private final int numberDefaultScale;

    @Inject
    public OracleClient(
            JdbcConnectorId connectorId,
            BaseJdbcConfig config,
            OracleConfig oracleConfig,
            ConnectionFactory connectionFactory)
    {
        super(connectorId, config, "\"", connectionFactory);

        requireNonNull(oracleConfig, "oracle config is null");
        this.synonymsEnabled = oracleConfig.isSynonymsEnabled();
        this.numberDefaultScale = oracleConfig.getNumberDefaultScale();
    }

    private String[] getTableTypes()
    {
        if (synonymsEnabled) {
            return new String[] {"TABLE", "VIEW", "SYNONYM"};
        }
        return new String[] {"TABLE", "VIEW"};
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, Optional.of(escape)).orElse(null),
                escapeNamePattern(tableName, Optional.of(escape)).orElse(null),
                getTableTypes());
    }

    @Override
    public PreparedStatement getPreparedStatement(ConnectorSession session, Connection connection, String sql)
            throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(FETCH_SIZE);
        return statement;
    }

    @Override
    protected String generateTemporaryTableName()
    {
        return "presto_tmp_" + System.nanoTime();
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, SchemaTableName oldTable, SchemaTableName newTable)
    {
        if (!oldTable.getSchemaName().equalsIgnoreCase(newTable.getSchemaName())) {
            throw new PrestoException(NOT_SUPPORTED, "Table rename across schemas is not supported in Oracle");
        }

        try (Connection connection = connectionFactory.openConnection(identity)) {
            DatabaseMetaData metadata = connection.getMetaData();
            String schemaName = oldTable.getSchemaName();
            String newTableName = newTable.getTableName();
            String oldTableName = oldTable.getTableName();
            if (metadata.storesUpperCaseIdentifiers() && !caseSensitiveNameMatchingEnabled) {
                schemaName = schemaName != null ? schemaName.toUpperCase(ENGLISH) : null;
                newTableName = newTableName.toUpperCase(ENGLISH);
                oldTableName = oldTableName.toUpperCase(ENGLISH);
            }

            String sql = format(
                    "ALTER TABLE %s RENAME TO %s",
                    quoted(catalogName, schemaName, oldTableName),
                    quoted(newTableName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, List<JdbcColumnHandle> columnHandles, TupleDomain<ColumnHandle> tupleDomain)
    {
        try {
            requireNonNull(handle.getSchemaName(), "schema name is null");
            requireNonNull(handle.getTableName(), "table name is null");
            String sql = format(
                    "SELECT NUM_ROWS, AVG_ROW_LEN, LAST_ANALYZED\n" +
                            "FROM   ALL_TAB_STATISTICS\n" +
                            "WHERE  OWNER='%s'\n" +
                            "AND    TABLE_NAME='%s'",
                    handle.getSchemaName().toUpperCase(), handle.getTableName().toUpperCase());
            try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session));
                    PreparedStatement preparedStatement = getPreparedStatement(session, connection, sql);
                    PreparedStatement preparedStatementCol = getPreparedStatement(session, connection, getColumnStaticsSql(handle));
                    ResultSet resultSet = preparedStatement.executeQuery();
                    ResultSet resultSetColumnStats = preparedStatementCol.executeQuery()) {
                if (!resultSet.next()) {
                    LOG.debug("Stats not found for table : %s.%s", handle.getSchemaName(), handle.getTableName());
                    return TableStatistics.empty();
                }
                double numRows = resultSet.getDouble("NUM_ROWS");
                // double avgRowLen = resultSet.getDouble("AVG_ROW_LEN");
                Date lastAnalyzed = resultSet.getDate("LAST_ANALYZED");

                Map<ColumnHandle, ColumnStatistics> columnStatisticsMap = new HashMap<>();
                Map<String, JdbcColumnHandle> columnHandleMap = Maps.uniqueIndex(columnHandles, JdbcColumnHandle::getColumnName);
                while (resultSetColumnStats.next() && numRows > 0) {
                    String columnName = resultSetColumnStats.getString("COLUMN_NAME");
                    double nullsCount = resultSetColumnStats.getDouble("NUM_NULLS");
                    double ndv = resultSetColumnStats.getDouble("NUM_DISTINCT");
                    // Oracle stores low and high values as RAW(1000) i.e. a byte array. No way to unwrap it, without a clue about the underlying type
                    // So we use column type as a clue and parse to double by converting as string first.
                    double lowValue = toDouble(resultSetColumnStats.getString("LOW_VALUE"));
                    double highValue = toDouble(resultSetColumnStats.getString("HIGH_VALUE"));
                    ColumnStatistics.Builder columnStatisticsBuilder = ColumnStatistics.builder()
                            .setNullsFraction(Estimate.estimateFromDouble(nullsCount / numRows))
                            .setDistinctValuesCount(Estimate.estimateFromDouble(ndv));
                    if (resultSetColumnStats.getString("DATA_TYPE").startsWith("VARCHAR") ||
                            resultSetColumnStats.getString("DATA_TYPE").startsWith("CHAR")) {
                        columnStatisticsBuilder.setDataSize(Estimate.estimateFromDouble(resultSetColumnStats.getDouble("DATA_LENGTH")));
                    }
                    ColumnStatistics columnStatistics = columnStatisticsBuilder.build();
                    if (Double.isFinite(lowValue) && Double.isFinite(highValue)) {
                        columnStatistics = columnStatisticsBuilder.setRange(new DoubleRange(lowValue, highValue)).build();
                    }
                    columnStatisticsMap.put(columnHandleMap.get(columnName), columnStatistics);
                }
                LOG.info("getTableStatics for table: %s.%s.%s with last analyzed: %s",
                        handle.getCatalogName(), handle.getSchemaName(), handle.getTableName(), lastAnalyzed);
                return TableStatistics.builder()
                        .setColumnStatistics(columnStatisticsMap)
                        .setRowCount(Estimate.estimateFromDouble(numRows)).build();
            }
        }
        catch (SQLException | RuntimeException e) {
            throw new PrestoException(JDBC_ERROR, "Failed fetching statistics for table: " + handle, e);
        }
    }

    private String getColumnStaticsSql(JdbcTableHandle handle)
    {
        // UTL_RAW.CAST_TO_BINARY_X does not render correctly so those types are not supported.
        return format(
                "SELECT COLUMN_NAME,\n" +
                        "DATA_TYPE,\n" +
                        "DATA_LENGTH,\n" +
                        "NUM_NULLS,\n" +
                        "NUM_DISTINCT,\n" +
                        "DENSITY,\n" +
                        "CASE DATA_TYPE\n" +
                        "   WHEN 'NUMBER'   THEN TO_CHAR(UTL_RAW.CAST_TO_NUMBER(LOW_VALUE))\n" +
                        "   ELSE NULL\n" +
                        "END AS LOW_VALUE,\n" +
                        "CASE DATA_TYPE\n" +
                        "   WHEN 'NUMBER'   THEN TO_CHAR(UTL_RAW.CAST_TO_NUMBER(HIGH_VALUE))\n" +
                        "   ELSE NULL\n" +
                        "END AS HIGH_VALUE\n" +
                        "FROM ALL_TAB_COLUMNS\n" +
                        "WHERE OWNER = '%s'\n" +
                        "  AND TABLE_NAME = '%s'", handle.getSchemaName().toUpperCase(), handle.getTableName().toUpperCase());
    }

    private double toDouble(String number)
    {
        try {
            return Double.parseDouble(number);
        }
        catch (Exception e) {
            // a string represented by number, may not even be a parseable number this is expected. e.g. if column type is
            // varchar.
            LOG.debug(e, "error while decoding : %s", number);
        }
        return NaN;
    }

    @Override
    public Optional<ReadMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        int columnSize = typeHandle.getColumnSize();

        switch (typeHandle.getJdbcType()) {
            case Types.CLOB:
            case Types.NCLOB:
                return Optional.of(varcharReadMapping(createUnboundedVarcharType()));
            case Types.BLOB:
                return Optional.of(varbinaryReadMapping());
            case Types.SMALLINT:
                return Optional.of(smallintReadMapping());
            case Types.FLOAT:
            case Types.DOUBLE:
            case OracleTypes.BINARY_DOUBLE:
                return Optional.of(doubleReadMapping());
            case Types.REAL:
                return Optional.of(realReadMapping());
            case Types.NUMERIC:
                int precision = columnSize == 0 ? Decimals.MAX_PRECISION : columnSize;
                int scale = typeHandle.getDecimalDigits();

                if (scale == 0) {
                    return Optional.of(bigintReadMapping());
                }
                if (scale < 0 || scale > precision) {
                    return Optional.of(decimalReadMapping(createDecimalType(precision, numberDefaultScale)));
                }

                return Optional.of(decimalReadMapping(createDecimalType(precision, scale)));
            case Types.LONGVARCHAR:
                if (columnSize > VarcharType.MAX_LENGTH || columnSize == 0) {
                    return Optional.of(varcharReadMapping(createUnboundedVarcharType()));
                }
                return Optional.of(varcharReadMapping(createVarcharType(columnSize)));
            case Types.VARCHAR:
                return Optional.of(varcharReadMapping(createVarcharType(columnSize)));

             /* Note: In Oracle, DATE and TIMESTAMP values are internally stored in the format YYYYMMDD HH24MISS.
             * When reading from Oracle to Presto, TIMESTAMP mappings must be used to interpret the values correctly.
             * Official documentation: https://docs.oracle.com/en/database/oracle/oracle-database/26/nlspg/datetime-data-types-and-time-zone-support.html#GUID-4D95F6B2-8F28-458A-820D-6C05F848CA23 */
            case Types.DATE:
            case Types.TIMESTAMP:
                return Optional.of(timestampReadMapping());
        }
        return super.toPrestoType(session, typeHandle);
    }

    @Override
    protected String toSqlType(Type type)
    {
        if (isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.isUnbounded() || varcharType.getLengthSafe() > 1000) {
                return "NCLOB";
            }
            return format("VARCHAR2(%s CHAR)", varcharType.getLengthSafe());
        }
        if (type instanceof CharType) {
            CharType charType = (CharType) type;
            if (charType.getLength() > 500) {
                return "CLOB";
            }
            return format("CHAR(%s)", ((CharType) type).getLength());
        }
        if (type instanceof DecimalType) {
            return format("NUMBER(%s, %s)",
                    ((DecimalType) type).getPrecision(),
                    ((DecimalType) type).getScale());
        }

        if (BIGINT.equals(type)) {
            return "NUMBER(19)";
        }
        if (INTEGER.equals(type)) {
            return "NUMBER(10)";
        }
        if (DOUBLE.equals(type)) {
            return "BINARY_DOUBLE";
        }
        if (REAL.equals(type)) {
            return "BINARY_FLOAT";
        }
        if (BOOLEAN.equals(type)) {
            return "NUMBER(1)";
        }
        if (DATE.equals(type)) {
            return "DATE";
        }
        if (TIMESTAMP.equals(type)) {
            return "TIMESTAMP";
        }
        return super.toSqlType(type);
    }

    @Override
    public String normalizeIdentifier(ConnectorSession session, String identifier)
    {
        return caseSensitiveNameMatchingEnabled ? identifier : identifier.toLowerCase(ENGLISH);
    }
}
