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
import java.sql.Statement;
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
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable()
            throws SQLException
    {
        return false;
    }

    @Override
    public String getURL()
            throws SQLException
    {
        return connection.getURI().toString();
    }

    @Override
    public String getUserName()
            throws SQLException
    {
        return connection.getUser();
    }

    @Override
    public boolean isReadOnly()
            throws SQLException
    {
        return connection.isReadOnly();
    }

    @Override
    public boolean nullsAreSortedHigh()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd()
            throws SQLException
    {
        return true;
    }

    @Override
    public String getDatabaseProductName()
            throws SQLException
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
            throws SQLException
    {
        return PrestoDriver.DRIVER_NAME;
    }

    @Override
    public String getDriverVersion()
            throws SQLException
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
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean storesMixedCaseIdentifiers()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers()
            throws SQLException
    {
        // TODO: support quoted identifiers properly
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers()
            throws SQLException
    {
        // TODO: support quoted identifiers properly
        return true;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers()
            throws SQLException
    {
        // TODO: support quoted identifiers properly
        return false;
    }

    @Override
    public String getIdentifierQuoteString()
            throws SQLException
    {
        return "\"";
    }

    @Override
    public String getSQLKeywords()
            throws SQLException
    {
        return "LIMIT";
    }

    @Override
    public String getNumericFunctions()
            throws SQLException
    {
        return "";
    }

    @Override
    public String getStringFunctions()
            throws SQLException
    {
        return "";
    }

    @Override
    public String getSystemFunctions()
            throws SQLException
    {
        return "";
    }

    @Override
    public String getTimeDateFunctions()
            throws SQLException
    {
        return "";
    }

    @Override
    public String getSearchStringEscape()
            throws SQLException
    {
        return SEARCH_STRING_ESCAPE;
    }

    @Override
    public String getExtraNameCharacters()
            throws SQLException
    {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsConvert()
            throws SQLException
    {
        // TODO: support convert
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType)
            throws SQLException
    {
        // TODO: support convert
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsGroupBy()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsNonNullableColumns()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar()
            throws SQLException
    {
        // TODO: support this
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar()
            throws SQLException
    {
        // TODO: support this
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL()
            throws SQLException
    {
        // TODO: verify this
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL()
            throws SQLException
    {
        // TODO: support this
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL()
            throws SQLException
    {
        // TODO: support this
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsOuterJoins()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins()
            throws SQLException
    {
        return true;
    }

    @Override
    public String getSchemaTerm()
            throws SQLException
    {
        return "schema";
    }

    @Override
    public String getProcedureTerm()
            throws SQLException
    {
        return "procedure";
    }

    @Override
    public String getCatalogTerm()
            throws SQLException
    {
        return "catalog";
    }

    @Override
    public boolean isCatalogAtStart()
            throws SQLException
    {
        return true;
    }

    @Override
    public String getCatalogSeparator()
            throws SQLException
    {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsPositionedDelete()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures()
            throws SQLException
    {
        // TODO: support stored procedure escape syntax
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInIns()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsUnion()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsUnionAll()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback()
            throws SQLException
    {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength()
            throws SQLException
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxConnections()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxIndexLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength()
            throws SQLException
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength()
            throws SQLException
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength()
            throws SQLException
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getMaxRowSize()
            throws SQLException
    {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs()
            throws SQLException
    {
        return true;
    }

    @Override
    public int getMaxStatementLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxStatements()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxTableNameLength()
            throws SQLException
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getMaxTablesInSelect()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxUserNameLength()
            throws SQLException
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation()
            throws SQLException
    {
        return Connection.TRANSACTION_READ_UNCOMMITTED;
    }

    @Override
    public boolean supportsTransactions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level)
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions()
            throws SQLException
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
                "FROM system.jdbc.columns\n");

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
            throws SQLException
    {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency)
            throws SQLException
    {
        return (type == ResultSet.TYPE_FORWARD_ONLY) &&
                (concurrency == ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public boolean ownUpdatesAreVisible(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates()
            throws SQLException
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
            throws SQLException
    {
        return connection;
    }

    @Override
    public boolean supportsSavepoints()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsNamedParameters()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsMultipleOpenResults()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys()
            throws SQLException
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
            throws SQLException
    {
        return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getResultSetHoldability()
            throws SQLException
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
            throws SQLException
    {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion()
            throws SQLException
    {
        return 2;
    }

    @Override
    public int getSQLStateType()
            throws SQLException
    {
        return DatabaseMetaData.sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsStatementPooling()
            throws SQLException
    {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime()
            throws SQLException
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
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets()
            throws SQLException
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
            throws SQLException
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
            throws SQLException
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
        try (Statement statement = getConnection().createStatement()) {
            return statement.executeQuery(sql);
        }
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
