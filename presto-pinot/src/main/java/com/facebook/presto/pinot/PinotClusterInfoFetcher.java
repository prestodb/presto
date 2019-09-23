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
package com.facebook.presto.pinot;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.http.client.HttpStatus;
import io.airlift.log.Logger;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.pinot.client.DynamicBrokerSelector;
import org.apache.pinot.common.data.Schema;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_FAILURE_GETTING_TABLE;
import static com.facebook.presto.pinot.PinotUtils.TIME_COLUMN_NAME;
import static com.facebook.presto.pinot.PinotUtils.TIME_COLUMN_VALUE;
import static io.airlift.http.client.HttpStatus.familyForStatusCode;
import static org.apache.pinot.common.config.TableNameBuilder.extractRawTableName;

/**
 * This class handles fetching necessary information from the Pinot cluster, including table schema, broker host, routing table, etc
 */
public class PinotClusterInfoFetcher
        implements Closeable
{
    private static final String APPLICATION_JSON = "application/json";

    private static final String GET_ALL_TABLES_API_TEMPLATE = "http://%s/tables";
    private static final String TABLE_SCHEMA_API_TEMPLATE = "http://%s/tables/%s/schema";
    private static final String ROUTING_TABLE_API_TEMPLATE = "http://%s/debug/routingTable/%s";
    private static final String TIME_BOUNDARY_API_TEMPLATE = "http://%s/debug/timeBoundary/%s";
    private static final CloseableHttpClient HTTP_CLIENT = HttpClients.createDefault();

    private static final Logger log = Logger.get(PinotClusterInfoFetcher.class);

    private final String controllerUrl;
    private final String zookeeperServers;
    private DynamicBrokerSelector dynamicBrokerSelector;
    private ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    public PinotClusterInfoFetcher(PinotConfig pinotConfig)
    {
        this(pinotConfig.getZookeeperUrl(), pinotConfig.getPinotCluster(), pinotConfig.getControllerUrl());
    }

    public PinotClusterInfoFetcher(String zookeeperUrl, String pinotCluster, String controllerUrl)
    {
        log.info("Trying to init PinotClusterInfoFetcher with Zookeeper: %s, PinotCluster %s, ControllerUrl: %s.", zookeeperUrl, pinotCluster, controllerUrl);
        zookeeperServers = zookeeperUrl + "/" + pinotCluster;
        this.controllerUrl = controllerUrl;
    }

    public static String sendHttpGet(final String url)
            throws Exception
    {
        HttpUriRequest request = RequestBuilder.get(url).setHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON).build();
        return HTTP_CLIENT.execute(request, getStringResponseHandler());
    }

    private static ResponseHandler<String> getStringResponseHandler()
    {
        return response -> {
            int status = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (familyForStatusCode(response.getStatusLine().getStatusCode()) == HttpStatus.Family.SUCCESSFUL && entity != null) {
                return EntityUtils.toString(entity);
            }
            else {
                if (entity == null) {
                    throw new ClientProtocolException("Entity is null");
                }
                throw new ClientProtocolException("Unexpected status: " + status);
            }
        };
    }

    public void close()
            throws IOException
    {
        HTTP_CLIENT.close();
    }

    @SuppressWarnings("unchecked")
    public List<String> getAllTables()
            throws Exception
    {
        final String url = String.format(GET_ALL_TABLES_API_TEMPLATE, getControllerUrl());
        String responseBody = sendHttpGet(url);
        Map<String, List<String>> responseMap = objectMapper.readValue(responseBody, Map.class);
        return responseMap.get("tables");
    }

    public Schema getTableSchema(String table)
            throws Exception
    {
        final String url = String.format(TABLE_SCHEMA_API_TEMPLATE, getControllerUrl(), table);
        String responseBody = sendHttpGet(url);
        return Schema.fromString(responseBody);
    }

    public String getBrokerHost(String table)
    {
        if (dynamicBrokerSelector == null) {
            dynamicBrokerSelector = new DynamicBrokerSelector(zookeeperServers);
        }
        return this.dynamicBrokerSelector.selectBroker(table);
    }

    public Map<String, Map<String, List<String>>> getRoutingTableForTable(String tableName)
            throws Exception
    {
        ImmutableMap.Builder<String, Map<String, List<String>>> routingTableMapBuilder = ImmutableMap.builder();
        String url = String.format(ROUTING_TABLE_API_TEMPLATE, getBrokerHost(tableName), tableName);

        String responseBody = sendHttpGet(url);
        JSONObject resp = JSONObject.parseObject(responseBody);
        JSONArray routingTableSnapshots = resp.getJSONArray("routingTableSnapshot");

        for (int i = 0; i < routingTableSnapshots.size(); i++) {
            JSONObject snapshot = routingTableSnapshots.getJSONObject(i);
            String tableNameWithType = snapshot.getString("tableName");
            // Response could contain info for tableName that matches the original table by prefix.
            // e.g. when table name is "table1", response could contain routingTable for "table1_staging".
            // The bug is fixed in the latest Pinot but not in the release version 0.1.0
            if (!tableName.equals(extractRawTableName(tableNameWithType))) {
                log.debug("Ignoring routingTable for %s", tableNameWithType);
                continue;
            }
            JSONArray routingTableEntriesArray = snapshot.getJSONArray("routingTableEntries");
            if (routingTableEntriesArray.size() == 0) {
                throw new PrestoException(
                        PINOT_FAILURE_GETTING_TABLE,
                        "RoutingTable is empty for " + tableName);
            }

            String routingTableEntries = routingTableEntriesArray.getJSONObject(new Random().nextInt(routingTableEntriesArray.size())).toJSONString();
            Map<String, List<String>> routingTable = objectMapper.readValue(routingTableEntries, Map.class);
            routingTableMapBuilder.put(tableNameWithType, routingTable);
        }
        return routingTableMapBuilder.build();
    }

    public Map<String, String> getTimeBoundaryForTable(String table)
            throws Exception
    {
        final String url = String.format(TIME_BOUNDARY_API_TEMPLATE, getBrokerHost(table), table);
        String responseBody = sendHttpGet(url);
        JSONObject response = JSONObject.parseObject(responseBody);
        ImmutableMap.Builder<String, String> timeBoundaryBuilder = ImmutableMap.builder();
        if (response.containsKey(TIME_COLUMN_NAME)) {
            timeBoundaryBuilder.put(TIME_COLUMN_NAME, response.getString(TIME_COLUMN_NAME));
        }
        if (response.containsKey(TIME_COLUMN_VALUE)) {
            timeBoundaryBuilder.put(TIME_COLUMN_VALUE, response.getString(TIME_COLUMN_VALUE));
        }
        return timeBoundaryBuilder.build();
    }

    private String getControllerUrl()
    {
        return this.controllerUrl;
    }
}
