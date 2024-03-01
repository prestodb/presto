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
package com.facebook.presto.router.cluster;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.log.Logger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class RemoteClusterInfo
        extends RemoteState
{
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger log = Logger.get(RemoteClusterInfo.class);
    private static final String RUNNING_QUERIES = "runningQueries";
    private static final String BLOCKED_QUERIES = "blockedQueries";
    private static final String QUEUED_QUERIES = "queuedQueries";
    private static final String ACTIVE_WORKERS = "activeWorkers";
    private static final String RUNNING_DRIVERS = "runningDrivers";

    private final AtomicLong runningQueries = new AtomicLong();
    private final AtomicLong blockedQueries = new AtomicLong();
    private final AtomicLong queuedQueries = new AtomicLong();
    private final AtomicLong activeWorkers = new AtomicLong();
    private final AtomicLong runningDrivers = new AtomicLong();

    public RemoteClusterInfo(HttpClient httpClient, URI remoteUri)
    {
        super(httpClient, remoteUri);
    }

    @Override
    public void handleResponse(JsonNode response)
    {
        Map<String, Integer> fields = mapper.convertValue(response, Map.class);
        runningQueries.set(fields.get(RUNNING_QUERIES));
        blockedQueries.set(fields.get(BLOCKED_QUERIES));
        queuedQueries.set(fields.get(QUEUED_QUERIES));
        activeWorkers.set(fields.get(ACTIVE_WORKERS));
        runningDrivers.set(fields.get(RUNNING_DRIVERS));
    }

    public long getRunningQueries()
    {
        return runningQueries.get();
    }

    public long getBlockedQueries()
    {
        return blockedQueries.get();
    }

    public long getQueuedQueries()
    {
        return queuedQueries.get();
    }

    public long getActiveWorkers()
    {
        return activeWorkers.get();
    }

    public long getRunningDrivers()
    {
        return runningDrivers.get();
    }
}
