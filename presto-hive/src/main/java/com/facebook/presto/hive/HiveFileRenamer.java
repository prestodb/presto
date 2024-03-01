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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import org.weakref.jmx.Managed;

import javax.annotation.PreDestroy;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Verify.verify;

public class HiveFileRenamer
{
    private final Map<QueryId, Map<HiveMetadataUpdateKey, AtomicLong>> queryPartitionFileCounterMap = new ConcurrentHashMap<>();
    private final Map<QueryId, Map<HiveMetadataUpdateHandle, String>> queryHiveMetadataResultMap = new ConcurrentHashMap<>();

    public List<ConnectorMetadataUpdateHandle> getMetadataUpdateResults(List<ConnectorMetadataUpdateHandle> metadataUpdateRequests, QueryId queryId)
    {
        ImmutableList.Builder<ConnectorMetadataUpdateHandle> metadataUpdateResults = ImmutableList.builder();

        for (ConnectorMetadataUpdateHandle connectorMetadataUpdateHandle : metadataUpdateRequests) {
            HiveMetadataUpdateHandle request = (HiveMetadataUpdateHandle) connectorMetadataUpdateHandle;
            String fileName = getFileName(request, queryId);
            metadataUpdateResults.add(new HiveMetadataUpdateHandle(request.getRequestId(), request.getSchemaTableName(), request.getPartitionName(), Optional.of(fileName)));
        }
        return metadataUpdateResults.build();
    }

    public void cleanup(QueryId queryId)
    {
        queryPartitionFileCounterMap.remove(queryId);
        queryHiveMetadataResultMap.remove(queryId);
    }

    private String getFileName(HiveMetadataUpdateHandle request, QueryId queryId)
    {
        if (!queryPartitionFileCounterMap.containsKey(queryId) || !queryHiveMetadataResultMap.containsKey(queryId)) {
            queryPartitionFileCounterMap.putIfAbsent(queryId, new ConcurrentHashMap<>());
            queryHiveMetadataResultMap.putIfAbsent(queryId, new ConcurrentHashMap<>());
        }

        // To keep track of the file counter per query per partition
        Map<HiveMetadataUpdateKey, AtomicLong> partitionFileCounterMap = queryPartitionFileCounterMap.get(queryId);

        // To keep track of the file name result per query per request
        // This is to make sure that request - fileName mapping is 1:1
        Map<HiveMetadataUpdateHandle, String> hiveMetadataResultMap = queryHiveMetadataResultMap.get(queryId);

        // If we have seen this request before then directly return the result.
        if (hiveMetadataResultMap.containsKey(request)) {
            // We come here if for some reason the worker did not receive the fileName and it retried the request.
            return hiveMetadataResultMap.get(request);
        }

        HiveMetadataUpdateKey key = new HiveMetadataUpdateKey(request);
        // File names start from 0
        partitionFileCounterMap.putIfAbsent(key, new AtomicLong(0));

        AtomicLong fileCount = partitionFileCounterMap.get(key);
        String fileName = Long.valueOf(fileCount.getAndIncrement()).toString();

        // Store the request - fileName mapping
        hiveMetadataResultMap.put(request, fileName);

        return fileName;
    }

    @PreDestroy
    public void stop()
    {
        // Mappings should be deleted when query finishes. So verify that map is empty before its closed.
        verify(queryPartitionFileCounterMap.isEmpty(), "Query partition file counter map has %s entries left behind", queryPartitionFileCounterMap.size());
        verify(queryHiveMetadataResultMap.isEmpty(), "Query hive metadata result map has %s entries left behind", queryHiveMetadataResultMap.size());
    }

    @Managed
    public int getQueryPartitionFileCounterMapSize()
    {
        return queryPartitionFileCounterMap.size();
    }

    private static class HiveMetadataUpdateKey
    {
        private final SchemaTableName schemaTableName;
        private final Optional<String> partitionName;

        private HiveMetadataUpdateKey(HiveMetadataUpdateHandle hiveMetadataUpdateHandle)
        {
            this.schemaTableName = hiveMetadataUpdateHandle.getSchemaTableName();
            this.partitionName = hiveMetadataUpdateHandle.getPartitionName();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            HiveMetadataUpdateKey o = (HiveMetadataUpdateKey) obj;
            return schemaTableName.equals(o.schemaTableName) &&
                    partitionName.equals(o.partitionName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(schemaTableName, partitionName);
        }
    }
}
