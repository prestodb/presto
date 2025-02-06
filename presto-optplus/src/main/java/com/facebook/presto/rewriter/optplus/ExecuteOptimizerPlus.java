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
package com.facebook.presto.rewriter.optplus;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.client.Column;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.rewriter.util.OptimizerGuidelineUtil;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.rewriter.util.GovernanceErrorCode.DB2_CONNECTION_FAILURE;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;

public class ExecuteOptimizerPlus
{
    private static final Logger log = Logger.get(ExecuteOptimizerPlus.class);

    private List<Column> columns = new ArrayList<>();

    public List<List<Object>> callExecuteOptimizerPlus(String execOptPlusQuery)
            throws PrestoException
    {
        String query = parseOptPlusQuery(execOptPlusQuery);
        if (query.toLowerCase(Locale.ENGLISH).startsWith("call")) {
            log.debug("OPT+ executeoptplus CALL statements");
            return executeCallStatements(query);
        }
        else {
            log.debug("OPT+ executeoptplus statements");
            return executeStatements(query);
        }
    }

    private String parseOptPlusQuery(String query)
    {
        try {
            // TODO : Rewrite the ExecuteWxdQueryOptimizer matcher to use a regex or real SQL parser
            String passThroughCommandString = "ExecuteWxdQueryOptimizer '";
            int startIndex = query.indexOf(passThroughCommandString);
            int endIndex = query.lastIndexOf("'");
            return query.substring(startIndex + passThroughCommandString.length(), endIndex);
        }
        catch (Exception e) {
            throw e;
        }
    }

    private List<List<Object>> executeCallStatements(String query)
            throws PrestoException
    {
        boolean isExplainFormat = query.toLowerCase(Locale.ENGLISH).contains("explain_format");
        boolean isSyncProcedures = query.toUpperCase(Locale.ENGLISH).contains("REGISTER_EXT_METASTORE") || query.toUpperCase(Locale.ENGLISH).contains("UNREGISTER_EXT_METASTORE") || query.toUpperCase(Locale.ENGLISH).contains("SET_EXT_METASTORE_PROPERTY");
        if (isExplainFormat) {
            return executeCallExplainFormat(query);
        }
        List<List<Object>> resultList = new ArrayList<>();
        Connection conn = null;
        ResultSet rs = null;
        CallableStatement stmt = null;
        try {
            conn = OptimizerGuidelineUtil.getConnection();
            if (conn == null) {
                return disabledOptimizerPlugin(resultList);
            }
            long startTime = System.currentTimeMillis();
            stmt = conn.prepareCall(query);
            log.debug("OPT+ before executeoptplus - %s", query);
            if (isSyncProcedures) {
                stmt.registerOutParameter(1, Types.INTEGER);
                stmt.registerOutParameter(2, Types.VARCHAR);
            }
            boolean result = stmt.execute();
            while (true) {
                if (result) {
                    log.debug("OPT+ in if (result)");
                    rs = stmt.getResultSet();
                    log.debug("OPT+ getResultSet()");
                    ResultSetMetaData metaData = rs.getMetaData();
                    log.info("OPT+ Time taken for executeoptplus CALL DB2 call and return %d millisec", (System.currentTimeMillis() - startTime));
                    resultList = new ArrayList<>();
                    int columnCount = metaData.getColumnCount();
                    log.debug("OPT+ column count %d", columnCount);
                    for (int i = 1; i <= columnCount; i++) {
                        Column column = new Column(metaData.getColumnName(i), VARCHAR);
                        columns.add(column);
                        log.debug("OPT+ added column");
                    }
                    while (rs.next()) {
                        List<Object> row = new ArrayList<>();
                        for (int i = 1; i <= columnCount; i++) {
                            Object value = rs.getObject(i);
                            row.add(value);
                            log.debug("OPT+ added row");
                        }
                        resultList.add(row);
                    }
                }
                else {
                    int updateCount = stmt.getUpdateCount();
                    if (updateCount == -1) {
                        log.debug("OPT+ update count is %d", updateCount);
                        if (isSyncProcedures) {
                            int returnCode = stmt.getInt(1);
                            String returnMsg = stmt.getString(2);
                            columns.add(new Column("Code", BigintType.BIGINT));
                            columns.add(new Column("Message", VARCHAR));
                            List<Object> row = new ArrayList<>();
                            row.add(returnCode);
                            row.add(returnMsg);
                            log.debug("Return code: %d", returnCode);
                            log.debug("Return message: %s", returnMsg);
                            resultList.add(row);
                        }
                        break;
                    }
                    log.debug("OPT+ column type is BIGint and update count is %d", updateCount);
                    columns.add(new Column("result", BigintType.BIGINT));
                    List<Object> row = new ArrayList<>();
                    row.add(updateCount);
                    resultList.add(row);
                }
                result = stmt.getMoreResults();
            }
            return resultList;
        }
        catch (NullPointerException e) {
            log.error(e, "OPT+ executeoptplus failed to connect to OPT+");
            throw new PrestoException(DB2_CONNECTION_FAILURE, "Failed connecting to wxd query optimizer", e);
        }
        catch (Exception e) {
            log.error(e, "OPT+ executeoptplus failed");
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "ExecuteWxdQueryOptimizer failed ", e);
        }
        finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            }
            catch (SQLException e) {
                log.error(e, "OPT+ executeoptplus error in closing");
            }
        }
    }

    private List<List<Object>> disabledOptimizerPlugin(List<List<Object>> resultList)
    {
        List<Object> row = new ArrayList<>();
        String status = "ExecuteWxdQueryOptimizer FAILED Query Optimizer not enabled in this environment";
        row.add(status);
        resultList.add(row);
        columns.add(new Column("result", VARCHAR));
        return resultList;
    }

    private List<List<Object>> executeCallExplainFormat(String query)
    {
        List<List<Object>> resultList = new ArrayList<>();
        Connection conn = null;
        ResultSet rs = null;
        CallableStatement stmt = null;
        try {
            conn = OptimizerGuidelineUtil.getConnection();
            if (conn == null) {
                return disabledOptimizerPlugin(resultList);
            }
            log.debug("OPT+ before executeoptplus - %s", query);
            query = StringUtils.substringBetween(query, "(", ")");
            String[] params = query.split(",");
            String queryStmt = "call sysproc.explain_format(?, ?, ?, ?, ?, ?, ?," + params[7] + "," + params[8]
                    + "," + params[9] + "," + params[10] + "," + params[11] + ")";

            for (int i = 0; i < params.length; i++) {
                params[i] = params[i].trim().replace("'", "");
            }
            stmt = conn.prepareCall(queryStmt);
            stmt.setString("EXPLAIN_SCHEMA", params[0]);
            stmt.setString("EXPLAIN_REQUESTER", params[1]);
            stmt.setString("EXPLAIN_TIME", params[2]);
            stmt.setString("SOURCE_NAME", params[3]);
            stmt.setString("SOURCE_SCHEMA", params[4]);
            stmt.setString("SOURCE_VERSION", params[5]);
            stmt.setInt("SECTION_NUMBER", Integer.parseInt(params[6]));
            stmt.registerOutParameter("EXTRACT_SQL", Types.VARCHAR);

            stmt.executeUpdate();
            String returnMsg = stmt.getString("EXTRACT_SQL");
            columns.add(new Column("Message", VARCHAR));
            log.debug("Return message: %s", returnMsg);
            resultList.add(ImmutableList.of(returnMsg));
            return resultList;
        }
        catch (NullPointerException e) {
            log.error("OPT+ executeoptplus failed to connect to wxd query optimizer", e);
            throw new PrestoException(DB2_CONNECTION_FAILURE, "Failed connecting to wxd query optimizer", e);
        }
        catch (Exception e) {
            log.error("OPT+ executeoptplus failed", e);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "ExecuteWxdQueryOptimizer failed", e);
        }
        finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            }
            catch (SQLException e) {
                log.error("OPT+ executeoptplus error in closing", e);
            }
        }
    }

    private List<List<Object>> executeStatements(String query)
    {
        List<List<Object>> resultList = new ArrayList<>();
        Connection conn = null;
        ResultSet rs = null;
        Statement stmt = null;
        try {
            conn = OptimizerGuidelineUtil.getConnection();
            long startTime = System.currentTimeMillis();
            stmt = conn.createStatement();
            log.debug("OPT+ before executeoptplus - %s", query);
            boolean result = stmt.execute(query);
            log.info("OPT+ Time taken for executeoptplus query DB2 call and return %d millisec", (System.currentTimeMillis() - startTime));
            while (true) {
                if (result) {
                    rs = stmt.getResultSet();
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    log.debug("OPT+ column count %d", columnCount);
                    for (int i = 1; i <= columnCount; i++) {
                        Column column = new Column(metaData.getColumnName(i), VARCHAR);
                        columns.add(column);
                    }
                    resultList = new ArrayList<>();
                    while (rs.next()) {
                        List<Object> row = new ArrayList<>();
                        for (int i = 1; i <= columnCount; i++) {
                            Object value = rs.getString(i);
                            row.add(value);
                        }
                        resultList.add(row);
                    }
                    rs.close();
                }
                else {
                    int updateCount = stmt.getUpdateCount();
                    if (updateCount == -1) {
                        log.debug("OPT+ update count is %d", updateCount);
                        break;
                    }
                    log.debug("OPT+ column type is BIGint and update count is %d", updateCount);
                    columns.add(new Column("rows updated", BigintType.BIGINT));
                    List<Object> row = new ArrayList<>();
                    row.add(updateCount);
                    resultList.add(row);
                }
                result = stmt.getMoreResults();
            }
            stmt.close();
            conn.close();
            return resultList;
        }
        catch (NullPointerException e) {
            log.error(e, "OPT+ executeoptplus failed to connect to wxd query optimizer");
            throw new PrestoException(DB2_CONNECTION_FAILURE, "Failed connecting to wxd query optimizer", e);
        }
        catch (SQLException e) {
            log.error(e, "OPT+ executeoptplus failed");
            String message = String.format(
                    "ExecuteWxdQueryOptimizer failed: SQLCODE=%s, SQLSTATE=%s.",
                    e.getErrorCode(), e.getSQLState());

            throw new PrestoException(SYNTAX_ERROR, message, e);
        }
        catch (Exception e) {
            log.error(e, "OPT+ executeoptplus failed");
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "ExecuteWxdQueryOptimizer failed", e);
        }
        finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            }
            catch (SQLException e) {
                log.error(e, "OPT+ executeoptplus error in closing");
            }
        }
    }

    public List<Column> getColumns()
    {
        return columns;
    }
}
