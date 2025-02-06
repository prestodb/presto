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
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.RuntimeUnit;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.rewriter.QueryRewriter;
import com.facebook.presto.spi.rewriter.QueryRewriterInput;
import com.facebook.presto.spi.rewriter.QueryRewriterOutput;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.rewriter.util.ConfigConstants.IS_QUERY_REWRITER_PLUGIN_ENABLED;
import static com.facebook.presto.rewriter.util.ConfigConstants.IS_QUERY_REWRITER_PLUGIN_SUCCEEDED;
import static com.facebook.presto.rewriter.util.ConfigConstants.REORDER_JOINS;
import static com.facebook.presto.rewriter.util.OptimizerGuidelineUtil.OAAS_URL;
import static com.facebook.presto.rewriter.util.OptimizerGuidelineUtil.getOptimizerConnectionPool;
import static com.facebook.presto.rewriter.util.OptimizerGuidelineUtil.initOptimizerConnectionPool;
import static com.facebook.presto.rewriter.util.OptimizerGuidelineUtil.runOptGuideline;
import static java.lang.Boolean.parseBoolean;

public final class OptplusQueryRewriter
        implements QueryRewriter
{
    private static final Logger log = Logger.get(OptplusQueryRewriter.class);
    private static final String USE_MATERIALIZED_VIEW = "use_materialized_views";
    private final OptPlusConfig config;

    @Inject
    public OptplusQueryRewriter(OptPlusConfig config)
    {
        this.config = config;
        String oaasUrlEnv = Optional.ofNullable(System.getenv(OAAS_URL)).orElse(config.getDb2JdbcUrl());
        if ((oaasUrlEnv != null && !"".equalsIgnoreCase(oaasUrlEnv))) {
            initOptimizerConnectionPool(config.getDb2JdbcUrl(), config.isShowOptimizedQuery());
        }
    }

    @Override
    public QueryRewriterOutput rewriteSQL(QueryRewriterInput queryRewriterInput)
    {
        Optional<QueryWithRuntimeStats> optimizedQuery = rewriteQuery(
                queryRewriterInput.getQueryId(),
                queryRewriterInput.getSchema(),
                queryRewriterInput.getCatalog(),
                queryRewriterInput.getEnabledCatalogs(),
                queryRewriterInput.getQuery(),
                queryRewriterInput.getSessionProperties());
        QueryRewriterOutput.Builder queryRewriterOutputBuilder = new QueryRewriterOutput.Builder().setOriginalQuery(queryRewriterInput.getQuery())
                .setQueryId(queryRewriterInput.getQueryId());
        if (optimizedQuery.isPresent()) {
            try {
                QueryWithRuntimeStats rewrittenWithStats = optimizedQuery.get();
                queryRewriterInput.getQueryPreparer().prepareQuery(queryRewriterInput.getAnalyzerOptions(),
                        rewrittenWithStats.getQuery(), queryRewriterInput.getPreparedStatements(), queryRewriterInput.getWarningCollector());
                queryRewriterOutputBuilder
                        .setRewrittenQuery(rewrittenWithStats.getQuery())
                        .setRuntimeStats(rewrittenWithStats.getRuntimeStats())
                        .setSessionProperties(
                                ImmutableMap.of(IS_QUERY_REWRITER_PLUGIN_SUCCEEDED, "true", REORDER_JOINS, "false"));
            }
            catch (Exception e) {
                if (config.isShowOptimizedQuery()) {
                    log.warn("Opt + failed to optimize query.", e);
                }
                queryRewriterOutputBuilder = fallbackToOriginalQuery(queryRewriterInput, queryRewriterOutputBuilder);
            }
        }
        else {
            queryRewriterOutputBuilder = fallbackToOriginalQuery(queryRewriterInput, queryRewriterOutputBuilder);
        }
        QueryRewriterOutput queryRewriterOutput = queryRewriterOutputBuilder.build();
        if (config.isShowOptimizedQuery()) {
            log.debug("Final query rewriter output : " + queryRewriterOutput);
        }
        return queryRewriterOutput;
    }

    private QueryRewriterOutput.Builder fallbackToOriginalQuery(QueryRewriterInput queryRewriterInput, QueryRewriterOutput.Builder queryRewriterOutputBuilder)
    {
        String lowerOriginalQuery = queryRewriterInput.getQuery().toLowerCase(Locale.ENGLISH);
        if (config.isEnableFallback() || !lowerOriginalQuery.contains("select")) {
            return queryRewriterOutputBuilder
                    .setRewrittenQuery(queryRewriterInput.getQuery())
                    .setSessionProperties(ImmutableMap.of(IS_QUERY_REWRITER_PLUGIN_SUCCEEDED, "false"));
        }
        // This exception is thrown for testing only, in production we should have fallback set to true.
        throw new PrestoException(OptPlusErrorCode.OPT_PLUS_ERROR_CODE, "Opt plus query optimization failed.");
    }

    private String checkForOptGuideline(String opQuery, String queryId, RuntimeStats runtimeStats)
    {
        String coordinatorHost = Optional.ofNullable(System.getenv("COORDINATOR_HOST")).orElse(config.getCoordinatorHost());
        String coordinatorPort = Optional.ofNullable(System.getenv("COORDINATOR_PORT")).orElse(String.valueOf(config.getCoordinatorPort()));
        if ((opQuery != null && !"".equalsIgnoreCase(opQuery)) && (opQuery.indexOf("<OPTGUIDELINES>") != -1)) {
            long start = System.nanoTime();
            log.info("OPT+ optguidelines available run intermediate query");
//            long startTimeTmp = System.currentTimeMillis();
            opQuery = runOptGuideline(opQuery, coordinatorHost, coordinatorPort, config.getOptplusUser(), config.getOptplusPass(),
                    config.isEnableJDBCSSL(), config.getSslTrustStorePath(), config.getSslTrustStorePassword(), config.isShowOptimizedQuery());
            long duration = System.nanoTime() - start;
            runtimeStats.addMetricValue(OptimizerStatus.OPTIMIZER_GUIDELINE_APPLIED.getStatus(), RuntimeUnit.NANO, duration);
        }
        return opQuery;
    }

    private Optional<String> getProperty(String name, Map<String, String> sessionProperties)
    {
        String systemPropertyValue = sessionProperties.getOrDefault(name, null);
        if (systemPropertyValue != null) {
            return Optional.of(systemPropertyValue);
        }
        String environmentVariableValue = System.getenv(name);
        if (environmentVariableValue != null) {
            return Optional.of(environmentVariableValue);
        }
        return Optional.empty();
    }

    private Optional<QueryWithRuntimeStats> rewriteQuery(
            String queryId,
            Optional<String> sessionSchema,
            Optional<String> sessionCatalog,
            Set<String> hiveCatalogs,
            String originalQuery,
            Map<String, String> sessionProperties)
    {
        RuntimeStats runtimeStats = new RuntimeStats();
        String rewrittenQuery = null;
        String lowerOriginalQuery = originalQuery.toLowerCase(Locale.ENGLISH);
        if (!lowerOriginalQuery.contains("select") || !getProperty(IS_QUERY_REWRITER_PLUGIN_ENABLED, sessionProperties).isPresent()
                || lowerOriginalQuery.contains("optguideline") || !checkIsHiveCatalog(hiveCatalogs, sessionCatalog, originalQuery)) {
            log.info("OPT+ skipping not a select statement or information_schema or session set to false");
            return Optional.empty();
        }
        if (getProperty(IS_QUERY_REWRITER_PLUGIN_ENABLED, sessionProperties).get().toLowerCase(Locale.ENGLISH).equals("true")) {
            long startTime = System.nanoTime();
            rewrittenQuery = optimizeQuery(originalQuery, sessionCatalog.orElse(null), sessionSchema.orElse(null), parseBoolean(sessionProperties.get(USE_MATERIALIZED_VIEW)));
            rewrittenQuery = checkForOptGuideline(rewrittenQuery, queryId, runtimeStats);
            if (Strings.isNullOrEmpty(rewrittenQuery) && config.isEnableFallback()) {
                log.debug("OPT+ optimisation failed(null or empty)....moving to original query");
                log.info("OPT+ Time taken for optimiser fallback %d nanosec", (System.nanoTime() - startTime));
                long duration = System.nanoTime() - startTime;
                runtimeStats.addMetricValue(OptimizerStatus.OPTIMIZER_FALLBACK.getStatus(), RuntimeUnit.NANO, duration);
            }
        }
        return Optional.ofNullable(rewrittenQuery).map(query -> new QueryWithRuntimeStats(query, runtimeStats));
    }

    public String optimizeQuery(String query, String catalog, String schema, boolean isMaterializedViewEnabled)
    {
        if ((config.getDb2JdbcUrl() != null && !"".equalsIgnoreCase(config.getDb2JdbcUrl()))) {
            Connection conn = null;
            Statement stmt = null;
            ResultSet rs = null;
            try {
                long startTime = System.currentTimeMillis();
                String wqQryStmt = "";
                conn = getOptimizerConnectionPool().getConnection();
                stmt = conn.createStatement();
                query = query.replace("'", "''");
                if (config.isShowOptimizedQuery()) {
                    log.debug("OPT+ before optimization - %s", query);
                }
                if (schema != null && !schema.isEmpty()) {
                    schema = schema.toUpperCase(Locale.ENGLISH);
                }
                if (config.isEnableMaterializedView()) {
                    log.debug("OPT+ MQT enabled, session - %b", isMaterializedViewEnabled);
                    rs = stmt.executeQuery("VALUES prestosql ( '" + query + " ' , '" + catalog + "', '" + schema + "', " + isMaterializedViewEnabled + ")");
                }
                else {
                    rs = stmt.executeQuery("VALUES prestosql ( '" + query + " ' , '" + catalog + "', '" + schema + "')");
                }
                rs.next();
                wqQryStmt = rs.getString(1);
                log.info("OPT+ Time taken for optimizer DB2 call and return %d millisec", (System.currentTimeMillis() - startTime));
                return wqQryStmt;
            }
            catch (SQLException e) {
                log.error("OPT+ optimization failed to optimize input query, please check the server logs for details");
                if (!config.isEnableFallback()) { // This is used during testing.
                    throw new RuntimeException(e);
                }
                /*
                 * Remove the printing of the error after testing, throws DB2 logs
                 * */
                if (config.isShowOptimizedQuery()) {
                    log.error(e, "Error");
                }
            }
            catch (Exception e) {
                log.error("OPT+ optimizer failed to optimize query");
                if (!config.isEnableFallback()) { // This is used during testing.
                    throw new RuntimeException(e);
                }
                /*
                 * Remove the printing of the error after testing, throws DB2 logs
                 * */
                if (config.isShowOptimizedQuery()) {
                    log.error(e, "Error");
                }
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
                    log.error("OPT+ executeoptplus error in closing");
                    if (config.isShowOptimizedQuery()) {
                        log.error(e, "Error");
                    }
                }
            }
        }
        return null;
    }

    private boolean checkIsHiveCatalog(Set<String> hiveCatalogs, Optional<String> sessionCatalog, String query)
    {
        // TODO: unaddressed problems
        // Problem 1. How to get, what are the catalogs for which OPT+ is enabled.
        // https://github.ibm.com/lakehouse/tracker/issues/22358
        // Problem 2. If session catalog is not defined, how to determine which catalog this query is for.
        // In current approach (i.e. in lakehouse/presto) a simple string search `hiveCatalogs.stream().anyMatch(query::contains)`
        // is performed which can lead to lot of spurious results. for example a catalog names like: mysqlIceberg can match with another
        // catalog Iceberg.
        return (sessionCatalog.isPresent() && hiveCatalogs.contains(sessionCatalog.get())) || hiveCatalogs.stream().anyMatch(query::contains);
    }
}
