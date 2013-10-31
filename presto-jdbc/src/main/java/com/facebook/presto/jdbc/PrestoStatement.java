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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;

public class PrestoStatement
        implements Statement
{
    private final AtomicInteger maxRows = new AtomicInteger();
    private final AtomicInteger queryTimeoutSeconds = new AtomicInteger();
    private final AtomicInteger fetchSize = new AtomicInteger();
    private final AtomicBoolean escapeProcessing = new AtomicBoolean(true);
    private final AtomicBoolean closeOnCompletion = new AtomicBoolean();
    private final AtomicReference<PrestoConnection> connection;

    PrestoStatement(PrestoConnection connection)
    {
        this.connection = new AtomicReference<>(checkNotNull(connection, "connection is null"));
    }

    @Override
    public ResultSet executeQuery(String sql)
            throws SQLException
    {
        try {
            return new PrestoResultSet(connection().startQuery(sql));
        }
        catch (RuntimeException e) {
            throw new SQLException("Error executing query", e);
        }
    }

    @Override
    public int executeUpdate(String sql)
            throws SQLException
    {
        throw new UnsupportedOperationException("executeUpdate");
    }

    @Override
    public void close()
            throws SQLException
    {
        if (connection.getAndSet(null) != null) {
            // TODO
        }
    }

    @Override
    public int getMaxFieldSize()
            throws SQLException
    {
        checkOpen();
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max)
            throws SQLException
    {
        checkOpen();
        if (max < 0) {
            throw new SQLException("Max field size must be positive");
        }
        // ignore: full values are always returned
    }

    @Override
    public int getMaxRows()
            throws SQLException
    {
        checkOpen();
        return maxRows.get();
    }

    @Override
    public void setMaxRows(int max)
            throws SQLException
    {
        checkOpen();
        if (max < 0) {
            throw new SQLException("Max rows must be positive");
        }
        maxRows.set(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable)
            throws SQLException
    {
        checkOpen();
        escapeProcessing.set(enable);
    }

    @Override
    public int getQueryTimeout()
            throws SQLException
    {
        checkOpen();
        return queryTimeoutSeconds.get();
    }

    @Override
    public void setQueryTimeout(int seconds)
            throws SQLException
    {
        checkOpen();
        if (seconds < 0) {
            throw new SQLException("Query timeout seconds must be positive");
        }
        queryTimeoutSeconds.set(seconds);
    }

    @Override
    public void cancel()
            throws SQLException
    {
        throw new UnsupportedOperationException("cancel");
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
    public void setCursorName(String name)
            throws SQLException
    {
        checkOpen();
        // ignore: positioned modifications not supported
    }

    @Override
    public boolean execute(String sql)
            throws SQLException
    {
        throw new UnsupportedOperationException("execute");
    }

    @Override
    public ResultSet getResultSet()
            throws SQLException
    {
        throw new UnsupportedOperationException("getResultSet");
    }

    @Override
    public int getUpdateCount()
            throws SQLException
    {
        throw new UnsupportedOperationException("getUpdateCount");
    }

    @Override
    public boolean getMoreResults()
            throws SQLException
    {
        throw new UnsupportedOperationException("getMoreResults");
    }

    @Override
    public void setFetchDirection(int direction)
            throws SQLException
    {
        checkOpen();
        if (!validFetchDirection(direction)) {
            throw new SQLException("Invalid fetch direction");
        }
        // ignore: fetch direction is always forward
    }

    @Override
    public int getFetchDirection()
            throws SQLException
    {
        checkOpen();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows)
            throws SQLException
    {
        checkOpen();
        if (rows < 0) {
            throw new SQLException("Fetch size must be positive");
        }
        fetchSize.set(rows);
    }

    @Override
    public int getFetchSize()
            throws SQLException
    {
        checkOpen();
        return fetchSize.get();
    }

    @Override
    public int getResultSetConcurrency()
            throws SQLException
    {
        checkOpen();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType()
            throws SQLException
    {
        checkOpen();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql)
            throws SQLException
    {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Batches not supported");
    }

    @Override
    public void clearBatch()
            throws SQLException
    {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Batches not supported");
    }

    @Override
    public int[] executeBatch()
            throws SQLException
    {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Batches not supported");
    }

    @Override
    public Connection getConnection()
            throws SQLException
    {
        return connection();
    }

    @Override
    public boolean getMoreResults(int current)
            throws SQLException
    {
        throw new UnsupportedOperationException("getMoreResults");
    }

    @Override
    public ResultSet getGeneratedKeys()
            throws SQLException
    {
        throw new UnsupportedOperationException("getGeneratedKeys");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException
    {
        throw new UnsupportedOperationException("executeUpdate");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes)
            throws SQLException
    {
        throw new UnsupportedOperationException("executeUpdate");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames)
            throws SQLException
    {
        throw new UnsupportedOperationException("executeUpdate");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys)
            throws SQLException
    {
        throw new UnsupportedOperationException("execute");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes)
            throws SQLException
    {
        throw new UnsupportedOperationException("execute");
    }

    @Override
    public boolean execute(String sql, String[] columnNames)
            throws SQLException
    {
        throw new UnsupportedOperationException("execute");
    }

    @Override
    public int getResultSetHoldability()
            throws SQLException
    {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed()
            throws SQLException
    {
        return connection.get() == null;
    }

    @Override
    public void setPoolable(boolean poolable)
            throws SQLException
    {
        checkOpen();
        // ignore: statement pooling not supported
    }

    @Override
    public boolean isPoolable()
            throws SQLException
    {
        checkOpen();
        return false;
    }

    @Override
    public void closeOnCompletion()
            throws SQLException
    {
        checkOpen();
        closeOnCompletion.set(true);
    }

    @Override
    public boolean isCloseOnCompletion()
            throws SQLException
    {
        checkOpen();
        return closeOnCompletion.get();
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

    private void checkOpen()
            throws SQLException
    {
        connection();
    }

    private PrestoConnection connection()
            throws SQLException
    {
        PrestoConnection connection = this.connection.get();
        if (connection == null) {
            throw new SQLException("Statement is closed");
        }
        if (connection.isClosed()) {
            throw new SQLException("Connection is closed");
        }
        return connection;
    }

    private static boolean validFetchDirection(int direction)
    {
        return (direction == ResultSet.FETCH_FORWARD) ||
                (direction == ResultSet.FETCH_REVERSE) ||
                (direction == ResultSet.FETCH_UNKNOWN);
    }
}
