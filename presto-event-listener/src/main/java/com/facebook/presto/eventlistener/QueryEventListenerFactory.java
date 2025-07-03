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
package com.facebook.presto.eventlistener;

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public final class QueryEventListenerFactory
        implements EventListenerFactory
{
    public static final String QUERYEVENT_CLUSTER_NAME = "presto.queryevent.clustername";
    public static final String QUERYEVENT_CONFIG_LOCATION = "presto.queryevent.log4j2.configLocation";
    public static final String QUERYEVENT_TRACK_CREATED = "presto.queryevent.log.queryCreatedEvent";
    public static final String QUERYEVENT_TRACK_COMPLETED = "juicyray.queryevent.log.queryCompletedEvent";
    public static final String QUERYEVENT_TRACK_COMPLETED_SPLIT = "presto.queryevent.log.splitCompletedEvent";
    public static final String QUERYEVENT_WEBSOCKET_COLLECT = "juicyray.queryevent.websocket.enabled";
    public static final String QUERYEVENT_WEBSOCKET_URL = "juicyray.queryevent.websocket.url";
    public static final String QUERY_EVENT_FILTERING_ENABLE = "presto.queryevent.filter.enabled";
    public static final String QUERY_EVENT_FILTERING_CATALOGS = "presto.queryevent.filter.catalogs";
    public static final String QUERY_EVENT_FILTERING_SCHEMAS = "presto.queryevent.filter.schemas";
    public static final String QUERY_EVENT_FILTERING_KEYWORD = "presto.queryevent.filter.keyword";
    public static final String QUERY_EVENT_FILTERING_QUERY_ACTION_TYPE = "presto.queryevent.filter.queryactiontype";
    public static final String QUERY_EVENT_FILTERING_QUERY_ACTION_TYPE_ENABLED = "presto.queryevent.filter.queryactiontype.enabled";
    public static final String QUERY_EVENT_FILTERING_CONTEXT_SOURCE = "watsonx.queryevent.filter.context.source";
    public static final String QUERY_EVENT_FILTERING_CONTEXT_SOURCE_ENABLED = "presto.queryevent.filter.context.source.enabled";
    public static final String QUERY_EVENT_FILTERING_CONTEXT_ENVIRONMENT = "presto.queryevent.filter.context.environment";
    public static final String QUERY_EVENT_FILTERING_CONTEXT_ENVIRONMENT_ENABLED = "presto.queryevent.filter.context.environment.enabled";
    public static final String QUERY_EVENT_FILTERING_TABLES = "watsonx.queryevent.filter.tables";
    public static final String QUERYEVENT_MYSQL_COLLECT = "mysql.service.enabled";
    public static final String QUERYEVENT_AUDIT_COLLECT = "audit.service.enabled";
    public static final String AUDITSCHEMA = "AUDITSCHEMA";
    public static final String AUDITSERVICE = "AUDITSERVICE";
    public static final String AUDITPORT = "AUDITPORT";
    public static final String NAMESPACE = "NAMESPACE";
    public static final String PRESTO_QUERY_EVENT_LISTENER = "presto.query-event-listener";
    public String auditUrl = "";
    private static final String QUERYEVENT_CONFIG_LOCATION_ERROR = QUERYEVENT_CONFIG_LOCATION + " is null";
    private static final String QUERYEVENT_CLUSTER_NAME_ERROR = QUERYEVENT_CLUSTER_NAME + " is null";
    private static final String QUERYEVENT_WEBSOCKET_URL_ERROR = QUERYEVENT_WEBSOCKET_URL + " is null";

    public static final String QUERYEVENT_JDBC_URI = "jdbc.uri";
    public static final String QUERYEVENT_JDBC_USER = "jdbc.user";
    public static final String QUERY_EVENT_JDBC_PWD = "jdbc.pwd";

    public String getName()
    {
        return PRESTO_QUERY_EVENT_LISTENER;
    }

    public EventListener create(Map<String, String> config)
    {
        boolean useMysqlServiceCollector = getBooleanConfig(config, QUERYEVENT_MYSQL_COLLECT, false);
        if (useMysqlServiceCollector) {
            /**
             *  We need to obtain cluster operation and information on queries for maintainance of cluster in a form
             *  of storage in relational database.
             *  The code block below is dedicated for storing such information in a mysql database service
             */
            if (!config.containsKey(QUERYEVENT_JDBC_URI)) {
                throw new RuntimeException("/etc/event-listener.properties file missing jdbc.uri");
            }
            if (!config.containsKey(QUERYEVENT_JDBC_USER)) {
                throw new RuntimeException("/etc/event-listener.properties file missing jdbc.user");
            }
            if (!config.containsKey(QUERY_EVENT_JDBC_PWD)) {
                throw new RuntimeException("/etc/event-listener.properties file missing jdbc.pwd");
            }
        }
        String zenAuditEnable = Optional.ofNullable(System.getenv("ENABLE_AUDIT")).orElse("");
        if ("true".equalsIgnoreCase(zenAuditEnable)) {
            String auditschema = requireNonNull(config.get(AUDITSCHEMA), "schema cannot be null");
            String auditservice = requireNonNull(config.get(AUDITSERVICE), "service cannot be null");
            String auditport = requireNonNull(config.get(AUDITPORT), "port cannot be null");
            String namespace = requireNonNull(config.get(NAMESPACE), "namespace cannot be null");
            auditUrl = auditschema + "://" + auditservice + "." + namespace + ".svc.cluster.local" + ":" + auditport;
        }
        // Below is for logging based metric collection
        String log4j2ConfigLocation = requireNonNull(config.get(QUERYEVENT_CONFIG_LOCATION), QUERYEVENT_CONFIG_LOCATION_ERROR);
        String clusterName = requireNonNull(config.get(QUERYEVENT_CLUSTER_NAME), QUERYEVENT_CLUSTER_NAME_ERROR);
        String webSocketCollectUrl = null;
        boolean sendToWebsockeCollector = getBooleanConfig(config, QUERYEVENT_WEBSOCKET_COLLECT, false);
        if (sendToWebsockeCollector) {
            webSocketCollectUrl = requireNonNull(config.get(QUERYEVENT_WEBSOCKET_URL), QUERYEVENT_WEBSOCKET_URL_ERROR);
        }

        boolean isFilteringEnabled = getBooleanConfig(config, QUERY_EVENT_FILTERING_ENABLE, false);
        boolean isFilterCatalog = config.containsKey(QUERY_EVENT_FILTERING_CATALOGS) && !config.get(QUERY_EVENT_FILTERING_CATALOGS).isEmpty();
        List<String> filterCatalogs = (isFilterCatalog) ? Arrays.stream(config.get(QUERY_EVENT_FILTERING_CATALOGS).split(",")).collect(Collectors.toList()) : new ArrayList<>();
        boolean isFilterSchemas = config.containsKey(QUERY_EVENT_FILTERING_SCHEMAS) && !config.get(QUERY_EVENT_FILTERING_SCHEMAS).isEmpty();
        List<String> filterSchemas = (isFilterSchemas) ? Arrays.stream(config.get(QUERY_EVENT_FILTERING_SCHEMAS).split(",")).collect(Collectors.toList()) : new ArrayList<>();
        String filterKeyword = config.get(QUERY_EVENT_FILTERING_KEYWORD);

        String queryActionType = config.get(QUERY_EVENT_FILTERING_QUERY_ACTION_TYPE);
        boolean queryActionTypeEnabled = getBooleanConfig(config, QUERY_EVENT_FILTERING_QUERY_ACTION_TYPE_ENABLED, false);
        String queryContextSource = config.get(QUERY_EVENT_FILTERING_CONTEXT_SOURCE);
        boolean queryContextSourceEnabled = getBooleanConfig(config, QUERY_EVENT_FILTERING_CONTEXT_SOURCE_ENABLED, false);
        String queryContextEnv = config.get(QUERY_EVENT_FILTERING_CONTEXT_ENVIRONMENT);
        boolean queryContextEnvEnabled = getBooleanConfig(config, QUERY_EVENT_FILTERING_CONTEXT_ENVIRONMENT_ENABLED, false);

        FilterConfig filterConfig = new FilterConfig.Builder()
                .filteringEnabled(isFilteringEnabled)
                .isFilterCatalog(isFilterCatalog)
                .filterCatalogs(filterCatalogs)
                .isFilterSchemas(isFilterSchemas)
                .filterSchemas(filterSchemas)
                .filterKeyword(filterKeyword)
                .queryActionType(queryActionType)
                .queryActionTypeEnabled(queryActionTypeEnabled)
                .queryContextSource(queryContextSource)
                .queryContextSourceEnabled(queryContextSourceEnabled)
                .queryContextEnv(queryContextEnv)
                .queryContextEnvEnabled(queryContextEnvEnabled)
                .build();

        LoggerContext loggerContext = Configurator.initialize("presto-queryevent-log", log4j2ConfigLocation);
        boolean trackEventCreated = getBooleanConfig(config, QUERYEVENT_TRACK_CREATED, true);
        boolean trackEventCompleted = getBooleanConfig(config, QUERYEVENT_TRACK_COMPLETED, true);
        boolean trackEventCompletedSplit = getBooleanConfig(config, QUERYEVENT_TRACK_COMPLETED_SPLIT, false);
        boolean useAuditCollector = getBooleanConfig(config, QUERYEVENT_AUDIT_COLLECT, false);
        return new QueryEventListener(clusterName, loggerContext, sendToWebsockeCollector, webSocketCollectUrl, trackEventCreated, trackEventCompleted, trackEventCompletedSplit, useMysqlServiceCollector, useAuditCollector, auditUrl, filterConfig, config);
    }

    /**
     * Get {@code boolean} parameter value, or return default.
     *
     * @param params Map of parameters
     * @param paramName Parameter name
     * @param paramDefault Parameter default value
     * @return Parameter value or default.
     */
    private boolean getBooleanConfig(Map<String, String> params, String paramName, boolean paramDefault)
    {
        String value = params.get(paramName);
        if (value != null && !value.trim().isEmpty()) {
            return Boolean.parseBoolean(value);
        }
        return paramDefault;
    }
}
