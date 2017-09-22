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
package com.facebook.presto.plugin.jdbc;

import org.h2.tools.SimpleResultSet;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

public class TestingNameMappingDriver
        implements Driver
{
    private Map<String, List<String>> originalTableNamesBySchema = new TreeMap<>();

    MockConnection connection = new MockConnection();

    public TestingNameMappingDriver(Map<String, List<String>> originalTableNamesBySchema)
    {
        this.originalTableNamesBySchema.putAll(originalTableNamesBySchema);
    }

    @Override
    public Connection connect(String url, Properties info)
            throws SQLException
    {
        return connection;
    }

    @Override
    public boolean acceptsURL(String url)
            throws SQLException
    {
        return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException
    {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion()
    {
        return 0;
    }

    @Override
    public int getMinorVersion()
    {
        return 0;
    }

    @Override
    public boolean jdbcCompliant()
    {
        return false;
    }

    @Override
    public Logger getParentLogger()
            throws SQLFeatureNotSupportedException
    {
        return null;
    }

    private class MockConnection
            implements Connection
    {
        MockMetaData metaData = new MockMetaData();

        @Override
        public Statement createStatement()
                throws SQLException
        {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String sql)
                throws SQLException
        {
            return null;
        }

        @Override
        public CallableStatement prepareCall(String sql)
                throws SQLException
        {
            return null;
        }

        @Override
        public String nativeSQL(String sql)
                throws SQLException
        {
            return null;
        }

        @Override
        public void setAutoCommit(boolean autoCommit)
                throws SQLException
        {
        }

        @Override
        public boolean getAutoCommit()
                throws SQLException
        {
            return false;
        }

        @Override
        public void commit()
                throws SQLException
        {
        }

        @Override
        public void rollback()
                throws SQLException
        {
        }

        @Override
        public void close()
                throws SQLException
        {
        }

        @Override
        public boolean isClosed()
                throws SQLException
        {
            return false;
        }

        @Override
        public DatabaseMetaData getMetaData()
                throws SQLException
        {
            return metaData;
        }

        @Override
        public void setReadOnly(boolean readOnly)
                throws SQLException
        {
        }

        @Override
        public boolean isReadOnly()
                throws SQLException
        {
            return false;
        }

        @Override
        public void setCatalog(String catalog)
                throws SQLException
        {
        }

        @Override
        public String getCatalog()
                throws SQLException
        {
            return null;
        }

        @Override
        public void setTransactionIsolation(int level)
                throws SQLException
        {
        }

        @Override
        public int getTransactionIsolation()
                throws SQLException
        {
            return 0;
        }

        @Override
        public SQLWarning getWarnings()
                throws SQLException
        {
            return null;
        }

        @Override
        public void clearWarnings()
                throws SQLException
        {
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency)
                throws SQLException
        {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
                throws SQLException
        {
            return null;
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
                throws SQLException
        {
            return null;
        }

        @Override
        public Map<String, Class<?>> getTypeMap()
                throws SQLException
        {
            return null;
        }

        @Override
        public void setTypeMap(Map<String, Class<?>> map)
                throws SQLException
        {
        }

        @Override
        public void setHoldability(int holdability)
                throws SQLException
        {
        }

        @Override
        public int getHoldability()
                throws SQLException
        {
            return 0;
        }

        @Override
        public Savepoint setSavepoint()
                throws SQLException
        {
            return null;
        }

        @Override
        public Savepoint setSavepoint(String name)
                throws SQLException
        {
            return null;
        }

        @Override
        public void rollback(Savepoint savepoint)
                throws SQLException
        {
        }

        @Override
        public void releaseSavepoint(Savepoint savepoint)
                throws SQLException
        {
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
                throws SQLException
        {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
                throws SQLException
        {
            return null;
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
                throws SQLException
        {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
                throws SQLException
        {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
                throws SQLException
        {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, String[] columnNames)
                throws SQLException
        {
            return null;
        }

        @Override
        public Clob createClob()
                throws SQLException
        {
            return null;
        }

        @Override
        public Blob createBlob()
                throws SQLException
        {
            return null;
        }

        @Override
        public NClob createNClob()
                throws SQLException
        {
            return null;
        }

        @Override
        public SQLXML createSQLXML()
                throws SQLException
        {
            return null;
        }

        @Override
        public boolean isValid(int timeout)
                throws SQLException
        {
            return false;
        }

        @Override
        public void setClientInfo(String name, String value)
                throws SQLClientInfoException
        {
        }

        @Override
        public void setClientInfo(Properties properties)
                throws SQLClientInfoException
        {
        }

        @Override
        public String getClientInfo(String name)
                throws SQLException
        {
            return null;
        }

        @Override
        public Properties getClientInfo()
                throws SQLException
        {
            return null;
        }

        @Override
        public Array createArrayOf(String typeName, Object[] elements)
                throws SQLException
        {
            return null;
        }

        @Override
        public Struct createStruct(String typeName, Object[] attributes)
                throws SQLException
        {
            return null;
        }

        @Override
        public void setSchema(String schema)
                throws SQLException
        {
        }

        @Override
        public String getSchema()
                throws SQLException
        {
            return null;
        }

        @Override
        public void abort(Executor executor)
                throws SQLException
        {
        }

        @Override
        public void setNetworkTimeout(Executor executor, int milliseconds)
                throws SQLException
        {
        }

        @Override
        public int getNetworkTimeout()
                throws SQLException
        {
            return 0;
        }

        @Override
        public <T> T unwrap(Class<T> iface)
                throws SQLException
        {
            return null;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface)
                throws SQLException
        {
            return false;
        }
    }

    private class MockMetaData
            implements DatabaseMetaData
    {
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
            return null;
        }

        @Override
        public String getUserName()
                throws SQLException
        {
            return null;
        }

        @Override
        public boolean isReadOnly()
                throws SQLException
        {
            return false;
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
            return false;
        }

        @Override
        public String getDatabaseProductName()
                throws SQLException
        {
            return null;
        }

        @Override
        public String getDatabaseProductVersion()
                throws SQLException
        {
            return null;
        }

        @Override
        public String getDriverName()
                throws SQLException
        {
            return null;
        }

        @Override
        public String getDriverVersion()
                throws SQLException
        {
            return null;
        }

        @Override
        public int getDriverMajorVersion()
        {
            return 0;
        }

        @Override
        public int getDriverMinorVersion()
        {
            return 0;
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
            return false;
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
            return false;
        }

        @Override
        public boolean storesMixedCaseQuotedIdentifiers()
                throws SQLException
        {
            return false;
        }

        @Override
        public String getIdentifierQuoteString()
                throws SQLException
        {
            return null;
        }

        @Override
        public String getSQLKeywords()
                throws SQLException
        {
            return null;
        }

        @Override
        public String getNumericFunctions()
                throws SQLException
        {
            return null;
        }

        @Override
        public String getStringFunctions()
                throws SQLException
        {
            return null;
        }

        @Override
        public String getSystemFunctions()
                throws SQLException
        {
            return null;
        }

        @Override
        public String getTimeDateFunctions()
                throws SQLException
        {
            return null;
        }

        @Override
        public String getSearchStringEscape()
                throws SQLException
        {
            return null;
        }

        @Override
        public String getExtraNameCharacters()
                throws SQLException
        {
            return null;
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
            return false;
        }

        @Override
        public boolean nullPlusNonNullIsNull()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsConvert()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsConvert(int fromType, int toType)
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsTableCorrelationNames()
                throws SQLException
        {
            return false;
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
            return false;
        }

        @Override
        public boolean supportsOrderByUnrelated()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsGroupBy()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsGroupByUnrelated()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsGroupByBeyondSelect()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsLikeEscapeClause()
                throws SQLException
        {
            return false;
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
            return false;
        }

        @Override
        public boolean supportsNonNullableColumns()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsMinimumSQLGrammar()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsCoreSQLGrammar()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsExtendedSQLGrammar()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsANSI92EntryLevelSQL()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsANSI92IntermediateSQL()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsANSI92FullSQL()
                throws SQLException
        {
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
            return false;
        }

        @Override
        public boolean supportsFullOuterJoins()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsLimitedOuterJoins()
                throws SQLException
        {
            return false;
        }

        @Override
        public String getSchemaTerm()
                throws SQLException
        {
            return null;
        }

        @Override
        public String getProcedureTerm()
                throws SQLException
        {
            return null;
        }

        @Override
        public String getCatalogTerm()
                throws SQLException
        {
            return null;
        }

        @Override
        public boolean isCatalogAtStart()
                throws SQLException
        {
            return false;
        }

        @Override
        public String getCatalogSeparator()
                throws SQLException
        {
            return null;
        }

        @Override
        public boolean supportsSchemasInDataManipulation()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsSchemasInProcedureCalls()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsSchemasInTableDefinitions()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsSchemasInIndexDefinitions()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsSchemasInPrivilegeDefinitions()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsCatalogsInDataManipulation()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsCatalogsInProcedureCalls()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsCatalogsInTableDefinitions()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsCatalogsInIndexDefinitions()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsCatalogsInPrivilegeDefinitions()
                throws SQLException
        {
            return false;
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
            return false;
        }

        @Override
        public boolean supportsSubqueriesInComparisons()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsSubqueriesInExists()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsSubqueriesInIns()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsSubqueriesInQuantifieds()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsCorrelatedSubqueries()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsUnion()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsUnionAll()
                throws SQLException
        {
            return false;
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
            return 0;
        }

        @Override
        public int getMaxProcedureNameLength()
                throws SQLException
        {
            return 0;
        }

        @Override
        public int getMaxCatalogNameLength()
                throws SQLException
        {
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
            return false;
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
            return 0;
        }

        @Override
        public int getDefaultTransactionIsolation()
                throws SQLException
        {
            return 0;
        }

        @Override
        public boolean supportsTransactions()
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsTransactionIsolationLevel(int level)
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsDataDefinitionAndDataManipulationTransactions()
                throws SQLException
        {
            return false;
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
            return null;
        }

        @Override
        public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)
                throws SQLException
        {
            return null;
        }

        @Override
        public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
                throws SQLException
        {
            SimpleResultSet ret = new SimpleResultSet();

            ret.addColumn("TABLE_SCHEM", Types.VARCHAR, 0, 0);
            ret.addColumn("TABLE_NAME", Types.VARCHAR, 0, 0);
            ret.addColumn("TABLE_CAT", Types.VARCHAR, 0, 0);

            if (tableNamePattern == null) {
                for (String tableName : originalTableNamesBySchema.get(schemaPattern)) {
                    ret.addRow(schemaPattern, tableName, null);
                }
            }
            else {
                for (String tableName : originalTableNamesBySchema.get(schemaPattern)) {
                    if (tableNamePattern.equals(tableName)) {
                        ret.addRow(schemaPattern, tableName, null);
                    }
                }
            }

            return ret;
        }

        @Override
        public ResultSet getSchemas()
                throws SQLException
        {
            SimpleResultSet ret = new SimpleResultSet();

            ret.addColumn("TABLE_SCHEM", Types.VARCHAR, 0, 0);
            ret.addColumn("TABLE_CAT", Types.VARCHAR, 0, 0);

            for (String schemaName : originalTableNamesBySchema.keySet()) {
                ret.addRow(schemaName, null);
            }

            return ret;
        }

        @Override
        public ResultSet getCatalogs()
                throws SQLException
        {
            return null;
        }

        @Override
        public ResultSet getTableTypes()
                throws SQLException
        {
            return null;
        }

        @Override
        public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
                throws SQLException
        {
            return null;
        }

        @Override
        public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
                throws SQLException
        {
            return null;
        }

        @Override
        public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
                throws SQLException
        {
            return null;
        }

        @Override
        public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
                throws SQLException
        {
            return null;
        }

        @Override
        public ResultSet getVersionColumns(String catalog, String schema, String table)
                throws SQLException
        {
            return null;
        }

        @Override
        public ResultSet getPrimaryKeys(String catalog, String schema, String table)
                throws SQLException
        {
            return null;
        }

        @Override
        public ResultSet getImportedKeys(String catalog, String schema, String table)
                throws SQLException
        {
            return null;
        }

        @Override
        public ResultSet getExportedKeys(String catalog, String schema, String table)
                throws SQLException
        {
            return null;
        }

        @Override
        public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable)
                throws SQLException
        {
            return null;
        }

        @Override
        public ResultSet getTypeInfo()
                throws SQLException
        {
            return null;
        }

        @Override
        public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
                throws SQLException
        {
            return null;
        }

        @Override
        public boolean supportsResultSetType(int type)
                throws SQLException
        {
            return false;
        }

        @Override
        public boolean supportsResultSetConcurrency(int type, int concurrency)
                throws SQLException
        {
            return false;
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
            return false;
        }

        @Override
        public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
                throws SQLException
        {
            return null;
        }

        @Override
        public Connection getConnection()
                throws SQLException
        {
            return null;
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
            return false;
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
            return null;
        }

        @Override
        public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
                throws SQLException
        {
            return null;
        }

        @Override
        public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
                throws SQLException
        {
            return null;
        }

        @Override
        public boolean supportsResultSetHoldability(int holdability)
                throws SQLException
        {
            return false;
        }

        @Override
        public int getResultSetHoldability()
                throws SQLException
        {
            return 0;
        }

        @Override
        public int getDatabaseMajorVersion()
                throws SQLException
        {
            return 0;
        }

        @Override
        public int getDatabaseMinorVersion()
                throws SQLException
        {
            return 0;
        }

        @Override
        public int getJDBCMajorVersion()
                throws SQLException
        {
            return 0;
        }

        @Override
        public int getJDBCMinorVersion()
                throws SQLException
        {
            return 0;
        }

        @Override
        public int getSQLStateType()
                throws SQLException
        {
            return 0;
        }

        @Override
        public boolean locatorsUpdateCopy()
                throws SQLException
        {
            return false;
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
            return null;
        }

        @Override
        public ResultSet getSchemas(String catalog, String schemaPattern)
                throws SQLException
        {
            return null;
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
            return null;
        }

        @Override
        public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
                throws SQLException
        {
            return null;
        }

        @Override
        public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)
                throws SQLException
        {
            return null;
        }

        @Override
        public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
                throws SQLException
        {
            return null;
        }

        @Override
        public boolean generatedKeyAlwaysReturned()
                throws SQLException
        {
            return false;
        }

        @Override
        public <T> T unwrap(Class<T> iface)
                throws SQLException
        {
            return null;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface)
                throws SQLException
        {
            return false;
        }
    }
}
