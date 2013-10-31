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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import static com.google.common.base.Preconditions.checkNotNull;

public class PrestoDatabaseMetaData
        implements DatabaseMetaData
{
    private final PrestoConnection connection;

    public PrestoDatabaseMetaData(PrestoConnection connection)
    {
        this.connection = checkNotNull(connection, "connection is null");
    }

    @Override
    public boolean allProceduresAreCallable()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean allTablesAreSelectable()
            throws SQLException
    {
        return true;
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
        // TODO: determine null sort order
        throw new UnsupportedOperationException("nullsAreSortedHigh");
    }

    @Override
    public boolean nullsAreSortedLow()
            throws SQLException
    {
        // TODO: determine null sort order
        throw new UnsupportedOperationException("nullsAreSortedLow");
    }

    @Override
    public boolean nullsAreSortedAtStart()
            throws SQLException
    {
        // TODO: determine null sort order
        throw new UnsupportedOperationException("nullsAreSortedAtStart");
    }

    @Override
    public boolean nullsAreSortedAtEnd()
            throws SQLException
    {
        // TODO: determine null sort order
        throw new UnsupportedOperationException("nullsAreSortedAtEnd");
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
        // TODO: get version from server
        return "UNKNOWN";
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
        return PrestoDriver.VERSION_MAJOR;
    }

    @Override
    public int getDriverMinorVersion()
    {
        return PrestoDriver.VERSION_MINOR;
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
        return true;
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
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers()
            throws SQLException
    {
        return true;
    }

    @Override
    public String getIdentifierQuoteString()
            throws SQLException
    {
        // TODO: support quoted identifiers
        return " ";
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
        return "\\";
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
        // TODO: verify this
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
        // TODO: verify this
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
        // TODO: support full outer joins
        return false;
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
        // TODO: support stored procedures
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons()
            throws SQLException
    {
        // TODO: support subqueries in comparisons
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists()
            throws SQLException
    {
        // TODO: support EXISTS
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns()
            throws SQLException
    {
        // TODO: support subqueries in IN clauses
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds()
            throws SQLException
    {
        // TODO: support subqueries in ANY/SOME/ALL predicates
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries()
            throws SQLException
    {
        // TODO: support correlated subqueries
        return false;
    }

    @Override
    public boolean supportsUnion()
            throws SQLException
    {
        // TODO: support UNION
        return false;
    }

    @Override
    public boolean supportsUnionAll()
            throws SQLException
    {
        // TODO: support UNION ALL
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit()
            throws SQLException
    {
        return true;
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
        return true;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback()
            throws SQLException
    {
        return true;
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
        // TODO: support transactions
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions()
            throws SQLException
    {
        // TODO: support transactions
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level)
            throws SQLException
    {
        return level == Connection.TRANSACTION_NONE;
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
        // TODO: support stored procedures
        throw new SQLFeatureNotSupportedException("stored procedures not supported");
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)
            throws SQLException
    {
        // TODO: support stored procedures
        throw new SQLFeatureNotSupportedException("stored procedures not supported");
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException
    {
        throw new UnsupportedOperationException("getTables");
    }

    @Override
    public ResultSet getSchemas()
            throws SQLException
    {
        throw new UnsupportedOperationException("getSchemas");
    }

    @Override
    public ResultSet getCatalogs()
            throws SQLException
    {
        throw new UnsupportedOperationException("getCatalogs");
    }

    @Override
    public ResultSet getTableTypes()
            throws SQLException
    {
        return select("SELECT 'TABLE' table_type FROM dual");
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException
    {
        throw new UnsupportedOperationException("getColumns");
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
        // TODO: implement this
        throw new UnsupportedOperationException("getTypeInfo");
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
        throw new SQLFeatureNotSupportedException("user-defined types not supported");
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
        throw new SQLFeatureNotSupportedException("type hierarchies not supported");
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("type hierarchies not supported");
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("user-defined types not supported");
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
        // TODO: get version from server
        return PrestoDriver.VERSION_MAJOR;
    }

    @Override
    public int getDatabaseMinorVersion()
            throws SQLException
    {
        return PrestoDriver.VERSION_MINOR;
    }

    @Override
    public int getJDBCMajorVersion()
            throws SQLException
    {
        return PrestoDriver.JDBC_VERSION_MAJOR;
    }

    @Override
    public int getJDBCMinorVersion()
            throws SQLException
    {
        return PrestoDriver.JDBC_VERSION_MINOR;
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
        // TODO: implement this
        throw new UnsupportedOperationException("getSchemas");
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
        throw new UnsupportedOperationException("getClientInfoProperties");
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException
    {
        // TODO: implement this
        throw new UnsupportedOperationException("getFunctions");
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)
            throws SQLException
    {
        // TODO: implement this
        throw new UnsupportedOperationException("getFunctionColumns");
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("pseudo columns not supported");
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

    private ResultSet select(String sql)
            throws SQLException
    {
        try (Statement statement = getConnection().createStatement()) {
            return statement.executeQuery(sql);
        }
    }
}
