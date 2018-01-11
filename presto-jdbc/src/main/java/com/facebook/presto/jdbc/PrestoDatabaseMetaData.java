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
package com.facebook.presto.jdbc;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;

public class PrestoDatabaseMetaData
        implements DatabaseMetaData
{
    private static final String SEARCH_STRING_ESCAPE = "\\";

    private final PrestoConnection connection;

    PrestoDatabaseMetaData(PrestoConnection connection)
    {
        this.connection = requireNonNull(connection, "connection is null");
    }

    @Override
    public boolean allProceduresAreCallable()
    {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable()
    {
        return false;
    }

    @Override
    public String getURL()
    {
        return connection.getURI().toString();
    }

    @Override
    public String getUserName()
    {
        return connection.getUser();
    }

    @Override
    public boolean isReadOnly()
    {
        return connection.isReadOnly();
    }

    @Override
    public boolean nullsAreSortedHigh()
    {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow()
    {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart()
    {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd()
    {
        return true;
    }

    @Override
    public String getDatabaseProductName()
    {
        return "Presto";
    }

    @Override
    public String getDatabaseProductVersion()
            throws SQLException
    {
        return connection.getServerInfo().getNodeVersion().getVersion();
    }

    @Override
    public String getDriverName()
    {
        return PrestoDriver.DRIVER_NAME;
    }

    @Override
    public String getDriverVersion()
    {
        return PrestoDriver.DRIVER_VERSION;
    }

    @Override
    public int getDriverMajorVersion()
    {
        return PrestoDriver.DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getDriverMinorVersion()
    {
        return PrestoDriver.DRIVER_VERSION_MINOR;
    }

    @Override
    public boolean usesLocalFiles()
    {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable()
    {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers()
    {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers()
    {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers()
    {
        return true;
    }

    @Override
    public boolean storesMixedCaseIdentifiers()
    {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers()
    {
        // TODO: support quoted identifiers properly
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers()
    {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers()
    {
        // TODO: support quoted identifiers properly
        return true;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers()
    {
        // TODO: support quoted identifiers properly
        return false;
    }

    @Override
    public String getIdentifierQuoteString()
    {
        return "\"";
    }

    @Override
    public String getSQLKeywords()
    {
        return "LIMIT";
    }

    @Override
    public String getNumericFunctions()
    {
        return "";
    }

    @Override
    public String getStringFunctions()
    {
        return "";
    }

    @Override
    public String getSystemFunctions()
    {
        return "";
    }

    @Override
    public String getTimeDateFunctions()
    {
        return "";
    }

    @Override
    public String getSearchStringEscape()
    {
        return SEARCH_STRING_ESCAPE;
    }

    @Override
    public String getExtraNameCharacters()
    {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn()
    {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn()
    {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing()
    {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull()
    {
        return true;
    }

    @Override
    public boolean supportsConvert()
    {
        // TODO: support convert
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType)
    {
        // TODO: support convert
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames()
    {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames()
    {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy()
    {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated()
    {
        return true;
    }

    @Override
    public boolean supportsGroupBy()
    {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated()
    {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect()
    {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause()
    {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets()
    {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions()
    {
        return true;
    }

    @Override
    public boolean supportsNonNullableColumns()
    {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar()
    {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar()
    {
        // TODO: support this
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar()
    {
        // TODO: support this
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL()
    {
        // TODO: verify this
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL()
    {
        // TODO: support this
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL()
    {
        // TODO: support this
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility()
    {
        return false;
    }

    @Override
    public boolean supportsOuterJoins()
    {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins()
    {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins()
    {
        return true;
    }

    @Override
    public String getSchemaTerm()
    {
        return "schema";
    }

    @Override
    public String getProcedureTerm()
    {
        return "procedure";
    }

    @Override
    public String getCatalogTerm()
    {
        return "catalog";
    }

    @Override
    public boolean isCatalogAtStart()
    {
        return true;
    }

    @Override
    public String getCatalogSeparator()
    {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation()
    {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls()
    {
        return true;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions()
    {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions()
    {
        return true;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions()
    {
        return true;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation()
    {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls()
    {
        return true;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions()
    {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions()
    {
        return true;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions()
    {
        return true;
    }

    @Override
    public boolean supportsPositionedDelete()
    {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate()
    {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate()
    {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures()
    {
        // TODO: support stored procedure escape syntax
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons()
    {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists()
    {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInIns()
    {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds()
    {
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries()
    {
        return true;
    }

    @Override
    public boolean supportsUnion()
    {
        return true;
    }

    @Override
    public boolean supportsUnionAll()
    {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit()
    {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback()
    {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit()
    {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback()
    {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength()
    {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength()
    {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength()
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy()
    {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex()
    {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy()
    {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect()
    {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable()
    {
        return 0;
    }

    @Override
    public int getMaxConnections()
    {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength()
    {
        return 0;
    }

    @Override
    public int getMaxIndexLength()
    {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength()
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength()
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength()
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getMaxRowSize()
    {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs()
    {
        return true;
    }

    @Override
    public int getMaxStatementLength()
    {
        return 0;
    }

    @Override
    public int getMaxStatements()
    {
        return 0;
    }

    @Override
    public int getMaxTableNameLength()
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getMaxTablesInSelect()
    {
        return 0;
    }

    @Override
    public int getMaxUserNameLength()
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation()
    {
        return Connection.TRANSACTION_READ_UNCOMMITTED;
    }

    @Override
    public boolean supportsTransactions()
    {
        return true;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level)
    {
        return true;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions()
    {
        return true;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly()
    {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit()
    {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions()
    {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException
    {
        return selectEmpty("" +
                "SELECT PROCEDURE_CAT, PROCEDURE_SCHEM, PROCEDURE_NAME,\n " +
                "  null, null, null, REMARKS, PROCEDURE_TYPE, SPECIFIC_NAME\n" +
                "FROM system.jdbc.procedures\n" +
                "ORDER BY PROCEDURE_CAT, PROCEDURE_SCHEM, PROCEDURE_NAME, SPECIFIC_NAME");
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)
            throws SQLException
    {
        return selectEmpty("" +
                "SELECT PROCEDURE_CAT, PROCEDURE_SCHEM, PROCEDURE_NAME, " +
                "  COLUMN_NAME, COLUMN_TYPE, DATA_TYPE, TYPE_NAME,\n" +
                "  PRECISION, LENGTH, SCALE, RADIX,\n" +
                "  NULLABLE, REMARKS, COLUMN_DEF, SQL_DATA_TYPE, SQL_DATETIME_SUB,\n" +
                "  CHAR_OCTET_LENGTH, ORDINAL_POSITION, IS_NULLABLE, SPECIFIC_NAME\n" +
                "FROM system.jdbc.procedure_columns\n" +
                "ORDER BY PROCEDURE_CAT, PROCEDURE_SCHEM, PROCEDURE_NAME, SPECIFIC_NAME, COLUMN_NAME");
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException
    {
        StringBuilder query = new StringBuilder("" +
                "SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS,\n" +
                "  TYPE_CAT, TYPE_SCHEM, TYPE_NAME, " +
                "  SELF_REFERENCING_COL_NAME, REF_GENERATION\n" +
                "FROM system.jdbc.tables");

        List<String> filters = new ArrayList<>();
        emptyStringEqualsFilter(filters, "TABLE_CAT", catalog);
        emptyStringLikeFilter(filters, "TABLE_SCHEM", schemaPattern);
        optionalStringLikeFilter(filters, "TABLE_NAME", tableNamePattern);
        optionalStringInFilter(filters, "TABLE_TYPE", types);
        buildFilters(query, filters);

        query.append("\nORDER BY TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME");

        return select(query.toString());
    }

    @Override
    public ResultSet getSchemas()
            throws SQLException
    {
        return select("" +
                "SELECT TABLE_SCHEM, TABLE_CATALOG\n" +
                "FROM system.jdbc.schemas\n" +
                "ORDER BY TABLE_CATALOG, TABLE_SCHEM");
    }

    @Override
    public ResultSet getCatalogs()
            throws SQLException
    {
        return select("" +
                "SELECT TABLE_CAT\n" +
                "FROM system.jdbc.catalogs\n" +
                "ORDER BY TABLE_CAT");
    }

    @Override
    public ResultSet getTableTypes()
            throws SQLException
    {
        return select("" +
                "SELECT TABLE_TYPE\n" +
                "FROM system.jdbc.table_types\n" +
                "ORDER BY TABLE_TYPE");
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException
    {
        StringBuilder query = new StringBuilder("" +
                "SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE,\n" +
                "  TYPE_NAME, COLUMN_SIZE, BUFFER_LENGTH, DECIMAL_DIGITS, NUM_PREC_RADIX,\n" +
                "  NULLABLE, REMARKS, COLUMN_DEF, SQL_DATA_TYPE, SQL_DATETIME_SUB,\n" +
                "  CHAR_OCTET_LENGTH, ORDINAL_POSITION, IS_NULLABLE,\n" +
                "  SCOPE_CATALOG, SCOPE_SCHEMA, SCOPE_TABLE,\n" +
                "  SOURCE_DATA_TYPE, IS_AUTOINCREMENT, IS_GENERATEDCOLUMN\n" +
                "FROM system.jdbc.columns");

        List<String> filters = new ArrayList<>();
        emptyStringEqualsFilter(filters, "TABLE_CAT", catalog);
        emptyStringLikeFilter(filters, "TABLE_SCHEM", schemaPattern);
        optionalStringLikeFilter(filters, "TABLE_NAME", tableNamePattern);
        optionalStringLikeFilter(filters, "COLUMN_NAME", columnNamePattern);
        buildFilters(query, filters);

        query.append("\nORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION");

        return select(query.toString());
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("privileges not supported");
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("privileges not supported");
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("row identifiers not supported");
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("version columns not supported");
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("primary keys not supported");
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("imported keys not supported");
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("exported keys not supported");
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("cross reference not supported");
    }

    @Override
    public ResultSet getTypeInfo()
            throws SQLException
    {
        return select("" +
                "SELECT TYPE_NAME, DATA_TYPE, PRECISION, LITERAL_PREFIX, LITERAL_SUFFIX,\n" +
                "CREATE_PARAMS, NULLABLE, CASE_SENSITIVE, SEARCHABLE, UNSIGNED_ATTRIBUTE,\n" +
                "FIXED_PREC_SCALE, AUTO_INCREMENT, LOCAL_TYPE_NAME, MINIMUM_SCALE, MAXIMUM_SCALE,\n" +
                "SQL_DATA_TYPE, SQL_DATETIME_SUB, NUM_PREC_RADIX\n" +
                "FROM system.jdbc.types\n" +
                "ORDER BY DATA_TYPE");
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("indexes not supported");
    }

    @Override
    public boolean supportsResultSetType(int type)
    {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency)
    {
        return (type == ResultSet.TYPE_FORWARD_ONLY) &&
                (concurrency == ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public boolean ownUpdatesAreVisible(int type)
    {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type)
    {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type)
    {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type)
    {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type)
    {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type)
    {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type)
    {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type)
    {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type)
    {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates()
    {
        // TODO: support batch updates
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException
    {
        return selectEmpty("" +
                "SELECT TYPE_CAT, TYPE_SCHEM, TYPE_NAME,\n" +
                "  CLASS_NAME, DATA_TYPE, REMARKS, BASE_TYPE\n" +
                "FROM system.jdbc.udts\n" +
                "ORDER BY DATA_TYPE, TYPE_CAT, TYPE_SCHEM, TYPE_NAME");
    }

    @Override
    public Connection getConnection()
    {
        return connection;
    }

    @Override
    public boolean supportsSavepoints()
    {
        return false;
    }

    @Override
    public boolean supportsNamedParameters()
    {
        return true;
    }

    @Override
    public boolean supportsMultipleOpenResults()
    {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys()
    {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
            throws SQLException
    {
        return selectEmpty("" +
                "SELECT TYPE_CAT, TYPE_SCHEM, TYPE_NAME,\n" +
                "  SUPERTYPE_CAT, SUPERTYPE_SCHEM, SUPERTYPE_NAME\n" +
                "FROM system.jdbc.super_types\n" +
                "ORDER BY TYPE_CAT, TYPE_SCHEM, TYPE_NAME");
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException
    {
        return selectEmpty("" +
                "SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, SUPERTABLE_NAME\n" +
                "FROM system.jdbc.super_tables\n" +
                "ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME");
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
            throws SQLException
    {
        return selectEmpty("" +
                "SELECT TYPE_CAT, TYPE_SCHEM, TYPE_NAME, ATTR_NAME, DATA_TYPE,\n" +
                "  ATTR_TYPE_NAME, ATTR_SIZE, DECIMAL_DIGITS, NUM_PREC_RADIX, NULLABLE,\n" +
                "  REMARKS, ATTR_DEF, SQL_DATA_TYPE, SQL_DATETIME_SUB, CHAR_OCTET_LENGTH,\n" +
                "  ORDINAL_POSITION, IS_NULLABLE, SCOPE_CATALOG, SCOPE_SCHEMA, SCOPE_TABLE,\n" +
                "SOURCE_DATA_TYPE\n" +
                "FROM system.jdbc.attributes\n" +
                "ORDER BY TYPE_CAT, TYPE_SCHEM, TYPE_NAME, ORDINAL_POSITION");
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability)
    {
        return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getResultSetHoldability()
    {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion()
            throws SQLException
    {
        return getDatabaseVersionPart(0);
    }

    @Override
    public int getDatabaseMinorVersion()
            throws SQLException
    {
        return getDatabaseVersionPart(1);
    }

    private int getDatabaseVersionPart(int part)
            throws SQLException
    {
        String version = getDatabaseProductVersion();
        List<String> parts = Splitter.on('.').limit(3).splitToList(version);
        try {
            return parseInt(parts.get(part));
        }
        catch (IndexOutOfBoundsException | NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public int getJDBCMajorVersion()
    {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion()
    {
        return 2;
    }

    @Override
    public int getSQLStateType()
    {
        return DatabaseMetaData.sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy()
    {
        return true;
    }

    @Override
    public boolean supportsStatementPooling()
    {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime()
    {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern)
            throws SQLException
    {
        StringBuilder query = new StringBuilder("" +
                "SELECT TABLE_SCHEM, TABLE_CATALOG\n" +
                "FROM system.jdbc.schemas");

        List<String> filters = new ArrayList<>();
        emptyStringEqualsFilter(filters, "TABLE_CATALOG", catalog);
        optionalStringLikeFilter(filters, "TABLE_SCHEM", schemaPattern);
        buildFilters(query, filters);

        query.append("\nORDER BY TABLE_CATALOG, TABLE_SCHEM");

        return select(query.toString());
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax()
    {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets()
    {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties()
            throws SQLException
    {
        // TODO: implement this
        throw new NotImplementedException("DatabaseMetaData", "getClientInfoProperties");
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException
    {
        // TODO: implement this
        throw new NotImplementedException("DatabaseMetaData", "getFunctions");
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)
            throws SQLException
    {
        // TODO: implement this
        throw new NotImplementedException("DatabaseMetaData", "getFunctionColumns");
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException
    {
        return selectEmpty("" +
                "SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE,\n" +
                "  COLUMN_SIZE, DECIMAL_DIGITS, NUM_PREC_RADIX, COLUMN_USAGE, REMARKS,\n" +
                "  CHAR_OCTET_LENGTH, IS_NULLABLE\n" +
                "FROM system.jdbc.pseudo_columns\n" +
                "ORDER BY TABLE_CAT, table_SCHEM, TABLE_NAME, COLUMN_NAME");
    }

    @Override
    public boolean generatedKeyAlwaysReturned()
    {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface)
            throws SQLException
    {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw new SQLException("No wrapper for " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface)
    {
        return iface.isInstance(this);
    }

    private ResultSet selectEmpty(String sql)
            throws SQLException
    {
        return select(sql + " LIMIT 0");
    }

    private ResultSet select(String sql)
            throws SQLException
    {
        return getConnection().createStatement().executeQuery(sql);
    }

    private static void buildFilters(StringBuilder out, List<String> filters)
    {
        if (!filters.isEmpty()) {
            out.append("\nWHERE ");
            Joiner.on(" AND ").appendTo(out, filters);
        }
    }

    private static void optionalStringInFilter(List<String> filters, String columnName, String[] values)
    {
        if (values == null) {
            return;
        }

        if (values.length == 0) {
            filters.add("false");
            return;
        }

        StringBuilder filter = new StringBuilder();
        filter.append(columnName).append(" IN (");

        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                filter.append(", ");
            }
            quoteStringLiteral(filter, values[i]);
        }

        filter.append(")");
        filters.add(filter.toString());
    }

    private static void optionalStringLikeFilter(List<String> filters, String columnName, String value)
    {
        if (value != null) {
            filters.add(stringColumnLike(columnName, value));
        }
    }

    private static void emptyStringEqualsFilter(List<String> filters, String columnName, String value)
    {
        if (value != null) {
            if (value.isEmpty()) {
                filters.add(columnName + " IS NULL");
            }
            else {
                filters.add(stringColumnEquals(columnName, value));
            }
        }
    }

    private static void emptyStringLikeFilter(List<String> filters, String columnName, String value)
    {
        if (value != null) {
            if (value.isEmpty()) {
                filters.add(columnName + " IS NULL");
            }
            else {
                filters.add(stringColumnLike(columnName, value));
            }
        }
    }

    private static String stringColumnEquals(String columnName, String value)
    {
        StringBuilder filter = new StringBuilder();
        filter.append(columnName).append(" = ");
        quoteStringLiteral(filter, value);
        return filter.toString();
    }

    private static String stringColumnLike(String columnName, String pattern)
    {
        StringBuilder filter = new StringBuilder();
        filter.append(columnName).append(" LIKE ");
        quoteStringLiteral(filter, pattern);
        filter.append(" ESCAPE ");
        quoteStringLiteral(filter, SEARCH_STRING_ESCAPE);
        return filter.toString();
    }

    private static void quoteStringLiteral(StringBuilder out, String value)
    {
        out.append('\'');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            out.append(c);
            if (c == '\'') {
                out.append('\'');
            }
        }
        out.append('\'');
    }
}
