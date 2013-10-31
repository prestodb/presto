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

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.StatementClient;
import com.google.common.base.Objects;
import com.google.common.net.HostAndPort;

import java.net.URI;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.fromProperties;
import static io.airlift.http.client.HttpUriBuilder.uriBuilder;

public class PrestoConnection
        implements Connection
{
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<String> catalog = new AtomicReference<>();
    private final AtomicReference<String> schema = new AtomicReference<>();
    private final URI uri;
    private final HostAndPort address;
    private final String user;
    private final Map<String, String> clientInfo = new ConcurrentHashMap<>();
    private final QueryExecutor queryExecutor;

    PrestoConnection(URI uri, String user, QueryExecutor queryExecutor)
    {
        this.uri = checkNotNull(uri, "uri is null");
        this.address = HostAndPort.fromParts(uri.getHost(), uri.getPort());
        this.user = checkNotNull(user, "user is null");
        this.queryExecutor = checkNotNull(queryExecutor, "queryExecutor is null");
        catalog.set("default");
        schema.set("default");
    }

    @Override
    public Statement createStatement()
            throws SQLException
    {
        checkOpen();
        return new PrestoStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql)
            throws SQLException
    {
        checkOpen();
        return new PrestoPreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql)
            throws SQLException
    {
        throw new UnsupportedOperationException("prepareCall");
    }

    @Override
    public String nativeSQL(String sql)
            throws SQLException
    {
        checkOpen();
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit)
            throws SQLException
    {
        checkOpen();
        if (!autoCommit) {
            throw new SQLFeatureNotSupportedException("Disabling auto-commit mode not supported");
        }
    }

    @Override
    public boolean getAutoCommit()
            throws SQLException
    {
        checkOpen();
        return true;
    }

    @Override
    public void commit()
            throws SQLException
    {
        checkOpen();
        if (getAutoCommit()) {
            throw new SQLException("Connection is in auto-commit mode");
        }
        throw new UnsupportedOperationException("commit");
    }

    @Override
    public void rollback()
            throws SQLException
    {
        checkOpen();
        if (getAutoCommit()) {
            throw new SQLException("Connection is in auto-commit mode");
        }
        throw new UnsupportedOperationException("rollback");
    }

    @Override
    public void close()
            throws SQLException
    {
        closed.set(true);
    }

    @Override
    public boolean isClosed()
            throws SQLException
    {
        return closed.get();
    }

    @Override
    public DatabaseMetaData getMetaData()
            throws SQLException
    {
        return new PrestoDatabaseMetaData(this);
    }

    @Override
    public void setReadOnly(boolean readOnly)
            throws SQLException
    {
        checkOpen();
        if (!readOnly) {
            throw new SQLFeatureNotSupportedException("Disabling read-only mode not supported");
        }
    }

    @Override
    public boolean isReadOnly()
            throws SQLException
    {
        checkOpen();
        return true;
    }

    @Override
    public void setCatalog(String catalog)
            throws SQLException
    {
        checkOpen();
        this.catalog.set(catalog);
    }

    @Override
    public String getCatalog()
            throws SQLException
    {
        checkOpen();
        return catalog.get();
    }

    @Override
    public void setTransactionIsolation(int level)
            throws SQLException
    {
        throw new UnsupportedOperationException("setTransactionIsolation");
    }

    @Override
    public int getTransactionIsolation()
            throws SQLException
    {
        throw new UnsupportedOperationException("getTransactionIsolation");
    }

    @Override
    public SQLWarning getWarnings()
            throws SQLException
    {
        checkOpen();
        return null;
    }

    @Override
    public void clearWarnings()
            throws SQLException
    {
        checkOpen();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException
    {
        throw new UnsupportedOperationException("createStatement");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException
    {
        throw new UnsupportedOperationException("prepareStatement");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException
    {
        throw new UnsupportedOperationException("prepareCall");
    }

    @Override
    public Map<String, Class<?>> getTypeMap()
            throws SQLException
    {
        throw new UnsupportedOperationException("getTypeMap");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map)
            throws SQLException
    {
        throw new UnsupportedOperationException("setTypeMap");
    }

    @Override
    public void setHoldability(int holdability)
            throws SQLException
    {
        checkOpen();
        if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLFeatureNotSupportedException("Changing holdability not supported");
        }
    }

    @Override
    public int getHoldability()
            throws SQLException
    {
        checkOpen();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public Savepoint setSavepoint()
            throws SQLException
    {
        throw new UnsupportedOperationException("setSavepoint");
    }

    @Override
    public Savepoint setSavepoint(String name)
            throws SQLException
    {
        throw new UnsupportedOperationException("setSavepoint");
    }

    @Override
    public void rollback(Savepoint savepoint)
            throws SQLException
    {
        throw new UnsupportedOperationException("rollback");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint)
            throws SQLException
    {
        throw new UnsupportedOperationException("releaseSavepoint");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException
    {
        throw new UnsupportedOperationException("createStatement");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException
    {
        throw new UnsupportedOperationException("prepareStatement");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException
    {
        throw new UnsupportedOperationException("prepareCall");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException
    {
        throw new UnsupportedOperationException("prepareStatement");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException
    {
        throw new UnsupportedOperationException("prepareStatement");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException
    {
        throw new UnsupportedOperationException("prepareStatement");
    }

    @Override
    public Clob createClob()
            throws SQLException
    {
        throw new UnsupportedOperationException("createClob");
    }

    @Override
    public Blob createBlob()
            throws SQLException
    {
        throw new UnsupportedOperationException("createBlob");
    }

    @Override
    public NClob createNClob()
            throws SQLException
    {
        throw new UnsupportedOperationException("createNClob");
    }

    @Override
    public SQLXML createSQLXML()
            throws SQLException
    {
        throw new UnsupportedOperationException("createSQLXML");
    }

    @Override
    public boolean isValid(int timeout)
            throws SQLException
    {
        throw new UnsupportedOperationException("isValid");
    }

    @Override
    public void setClientInfo(String name, String value)
            throws SQLClientInfoException
    {
        checkNotNull(name, "name is null");
        if (value != null) {
            clientInfo.put(name, value);
        }
        else {
            clientInfo.remove(value);
        }
    }

    @Override
    public void setClientInfo(Properties properties)
            throws SQLClientInfoException
    {
        clientInfo.putAll(fromProperties(properties));
    }

    @Override
    public String getClientInfo(String name)
            throws SQLException
    {
        return clientInfo.get(name);
    }

    @Override
    public Properties getClientInfo()
            throws SQLException
    {
        Properties properties = new Properties();
        properties.putAll(clientInfo);
        return properties;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements)
            throws SQLException
    {
        throw new UnsupportedOperationException("createArrayOf");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes)
            throws SQLException
    {
        throw new UnsupportedOperationException("createStruct");
    }

    @Override
    public void setSchema(String schema)
            throws SQLException
    {
        checkOpen();
        this.schema.set(schema);
    }

    @Override
    public String getSchema()
            throws SQLException
    {
        checkOpen();
        return schema.get();
    }

    @Override
    public void abort(Executor executor)
            throws SQLException
    {
        close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds)
            throws SQLException
    {
        throw new UnsupportedOperationException("setNetworkTimeout");
    }

    @Override
    public int getNetworkTimeout()
            throws SQLException
    {
        throw new UnsupportedOperationException("getNetworkTimeout");
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

    URI getURI()
    {
        return uri;
    }

    String getUser()
    {
        return user;
    }

    StatementClient startQuery(String sql)
    {
        URI uri = createHttpUri(address);

        String source = Objects.firstNonNull(clientInfo.get("ApplicationName"), "presto-jdbc");
        ClientSession session = new ClientSession(uri, user, source, catalog.get(), schema.get(), false);
        return queryExecutor.startQuery(session, sql);
    }

    private void checkOpen()
            throws SQLException
    {
        if (isClosed()) {
            throw new SQLException("Connection is closed");
        }
    }

    private static URI createHttpUri(HostAndPort address)
    {
        return uriBuilder()
                .scheme("http")
                .host(address.getHostText())
                .port(address.getPort())
                .build();
    }
}
