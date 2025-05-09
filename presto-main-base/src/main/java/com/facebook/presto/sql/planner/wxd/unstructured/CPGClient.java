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
package com.facebook.presto.sql.planner.wxd.unstructured;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.QualifiedObjectName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CPGClient
{
    private static final Logger log = Logger.get(CPGClient.class);
    private static final String LH_INSTANCE_ID_CPD = "LH_INSTANCE_ID";
    private static final String CPG_URL = "CPG_URL";
    private static final String LH_INSTANCE_ID_SAAS = "ID";
    private static final String LH_CONTEXT = "LH_CONTEXT";
    private static String baseUrl;
    private final LoadingCache<String, QualifiedObjectName> aclTableCache;
    private final LoadingCache<String, Boolean> isUnstructuredTableCache;
    private final LoadingCache<String, HashSet<String>> groupDetailsCache;
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public CPGClient()
    {
        this.httpClient = new OkHttpClient();
        this.aclTableCache = CacheBuilder.newBuilder()
                .maximumSize(5000)
                .expireAfterWrite(1, TimeUnit.HOURS)
                .build(new CacheLoader<String, QualifiedObjectName>()
                {
                    @Override
                    public QualifiedObjectName load(String bearerToken)
                    {
                        try {
                            return fetchAclFromRemote(bearerToken);
                        }
                        catch (Exception e) {
                            log.error(e, "Exception occurred in load function for getting ACL table Name ");
                            throw new RuntimeException("ACL table name read fails", e);
                        }
                    }
                });
        this.isUnstructuredTableCache = CacheBuilder.newBuilder()
                .maximumSize(5000)
                .expireAfterWrite(1, TimeUnit.HOURS)
                .build(new CacheLoader<String, Boolean>()
                {
                    @Override
                    public Boolean load(String cacheKey)
                    {
                        try {
                            return fetchUnstructuredStatusFromRemote(cacheKey);
                        }
                        catch (Exception e) {
                            log.error(e, "Exception occurred in load function");
                            return false;
                        }
                    }
                });
        this.groupDetailsCache = CacheBuilder.newBuilder()
                .maximumSize(5000)
                .expireAfterWrite(1, TimeUnit.HOURS)
                .build(new CacheLoader<String, HashSet<String>>()
                {
                    @Override
                    public HashSet<String> load(String bearerToken)
                            throws Exception
                    {
                        Set<String> result = fetchGroupDetailsFromRemote(bearerToken);
                        return new HashSet<>(result);
                    }
                });
    }

    public boolean isUnstructuredTable(String bearerToken, QualifiedObjectName qualifiedTableName)
    {
        String cacheKey = bearerToken + "_token_" + qualifiedTableName.toString();
        try {
            return isUnstructuredTableCache.get(cacheKey);
        }
        catch (Exception e) {
            log.error(e, "Failed to load unstructured table status ");
            return false;
        }
    }

    public QualifiedObjectName getaclTable(String bearerToken)
    {
        try {
            return aclTableCache.get(bearerToken);
        }
        catch (Exception e) {
            log.warn(e, "Failed to fetch ACL table");
            throw new RuntimeException("Malformed response", e);
        }
    }

    public Set<String> getGroupDetails(String bearerToken)
    {
        try {
            return groupDetailsCache.get(bearerToken);
        }
        catch (Exception e) {
            log.error(e, "Failed to load group details");
            return Collections.emptySet();
        }
    }

    private QualifiedObjectName fetchAclFromRemote(String bearerToken)
    {
        String apiEndpoint = getBaseUrl() + "/v1/access/acls";
        Request request = addRequestHeaders(new Request.Builder().url(apiEndpoint), bearerToken)
                .build();
        try (Response response = httpClient.newCall(request).execute()) {
            if (response.isSuccessful()) {
                AclTableNameResponseDto aclTableNameResponseDto = objectMapper.readValue(response.body().string(), AclTableNameResponseDto.class);
                String aclTable = aclTableNameResponseDto.getAclTableName();
                if (aclTable == null) {
                    throw new RuntimeException("The CPG call to get ACL table returned null");
                }
                String[] tableNames = aclTable.split("\\.");
                if (tableNames.length != 3) {
                    throw new RuntimeException(String.format("Invalid ACL table format '%s'. Expected format: <catalog>.<schema>.<table>", aclTable));
                }
                return new QualifiedObjectName(tableNames[0], tableNames[1], tableNames[2]);
            }
            else {
                throw new RuntimeException(String.format("Request failed with code : %s", Integer.toString(response.code())));
            }
        }
        catch (IOException ex) {
            log.error(ex, "CPG call to /v1/access/acls failed ");
            throw new RuntimeException("Error calling/parsing CPG /v1/access/acls API", ex);
        }
    }

    private boolean fetchUnstructuredStatusFromRemote(String cacheKey)
    {
        String[] parts = cacheKey.split("_token_", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid cache key format");
        }
        String bearerToken = parts[0];
        String qualifiedTableName = parts[1];
        String encodedTableName = null;
        try {
            encodedTableName = URLEncoder.encode(qualifiedTableName, StandardCharsets.UTF_8.toString());
        }
        catch (UnsupportedEncodingException e) {
            log.error(e, "Table name encoding failed");
            throw new RuntimeException(e);
        }
        String apiEndpoint = getBaseUrl() + "/v1/access/unstructured_objects?objectType=iceberg_table&objectId=" + encodedTableName;
        Request request = addRequestHeaders(new Request.Builder().url(apiEndpoint), bearerToken)
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (response.isSuccessful()) {
                UnstructuredTableResponseDto unstructuredTableResponseDto = objectMapper.readValue(response.body().string(), UnstructuredTableResponseDto.class);
                return unstructuredTableResponseDto.isUnstructured();
            }
            else {
                return false;
            }
        }
        catch (IOException ex) {
            log.error(ex, "CPG call to /v1/access/unstructured_objects failed for table");
            return false;
        }
    }

    private Set<String> fetchGroupDetailsFromRemote(String bearerToken)
    {
        String apiEndpoint = getBaseUrl() + "/v1/access/user_groups";
        Request request = addRequestHeaders(new Request.Builder().url(apiEndpoint), bearerToken)
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (response.isSuccessful()) {
                GroupListResponseDto groupListResponseDto = objectMapper.readValue(response.body().string(), GroupListResponseDto.class);
                return groupListResponseDto.getGroups();
            }
            else {
                return Collections.emptySet();
            }
        }
        catch (IOException ex) {
            log.error(ex, "CPG call to /v1/access/user_groups failed: %s");
            throw new RuntimeException("Malformed response from CPG", ex);
        }
    }

    public Request.Builder addRequestHeaders(Request.Builder builder, String bearerToken)
    {
        String lhContext = System.getenv(LH_CONTEXT);
        String lhInstanceId;
        if ("sw_dev".equals(lhContext) || "sw_ent".equals(lhContext) || "sw_env".equals(lhContext)) {
            lhInstanceId = System.getenv(LH_INSTANCE_ID_CPD);
        }
        else {
            lhInstanceId = System.getenv(LH_INSTANCE_ID_SAAS);
        }
        log.debug("cpglogs:lhInstanceId = %s", lhInstanceId);
        return builder
                .addHeader("Authorization", "Bearer " + bearerToken)
                .addHeader("LhInstanceId", lhInstanceId)
                .addHeader("Content-Type", "application/json");
    }

    public static String getBaseUrl()
    {
        String lhContext = System.getenv(LH_CONTEXT);
        String cpgEnvUrl = System.getenv(CPG_URL);
        if ("sw_dev".equals(lhContext) || "sw_ent".equals(lhContext) || "sw_env".equals(lhContext)) {
            baseUrl = cpgEnvUrl;
        }
        else {
            String filePath = "/conf/configmap/CPG_HTTPS_URL";
            try {
                baseUrl = Files.readString(Paths.get(filePath)).trim();
                if (baseUrl.isEmpty()) {
                    baseUrl = "https://" + cpgEnvUrl;
                }
            }
            catch (Exception e) {
                log.error(e, "Error reading the config file");
                baseUrl = "https://" + cpgEnvUrl;
            }
        }
        return baseUrl;
    }

    private static class AclTableNameResponseDto
    {
        private final String aclTableName;

        @JsonCreator
        public AclTableNameResponseDto(
                @JsonProperty("table") String aclTableName)
        {
            this.aclTableName = aclTableName;
        }

        @JsonProperty
        public String getAclTableName()
        {
            return aclTableName;
        }

        @Override
        public String toString()
        {
            return "AclTableNameResponseDto{ " +
                    ", aclTableName=" + aclTableName +
                    '}';
        }
    }

    private static class UnstructuredTableResponseDto
    {
        private final boolean isUnstructured;

        @JsonCreator
        public UnstructuredTableResponseDto(
                @JsonProperty("apply") boolean isUnstructured)
        {
            this.isUnstructured = isUnstructured;
        }

        @JsonProperty
        public boolean isUnstructured()
        {
            return isUnstructured;
        }

        @Override
        public String toString()
        {
            return "UnstructuredTableResponseDto{ " +
                    ",Introduce local variable" + isUnstructured +
                    '}';
        }
    }

    private static class GroupListResponseDto
    {
        private final String groupList;

        @JsonCreator
        public GroupListResponseDto(
                @JsonProperty("user_groups") String groupList)
        {
            this.groupList = groupList;
        }

        @JsonProperty
        public Set<String> getGroups()
        {
            if (groupList == null || groupList.isEmpty()) {
                return Collections.emptySet();
            }
            return Arrays.stream(groupList.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toSet());
        }

        @Override
        public String toString()
        {
            return "GroupListResponseDto{ " +
                    ", Group list=" + groupList +
                    '}';
        }
    }
}
