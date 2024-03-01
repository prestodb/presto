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

import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.StringResponseHandler.StringResponse;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.TimeUnit;

import static java.util.Locale.ENGLISH;

@ThreadSafe
public class PinotMetrics
{
    private final PinotMetricsStats getStats = new PinotMetricsStats(false);
    private final PinotMetricsStats queryStats = new PinotMetricsStats(true);
    private final PinotMetricsStats tablesStats = new PinotMetricsStats(true);
    private final PinotMetricsStats schemaStats = new PinotMetricsStats(true);
    private final PinotMetricsStats brokerTimeBoundaryStats = new PinotMetricsStats(false);
    private final PinotMetricsStats brokerRoutingTableStats = new PinotMetricsStats(true);

    @Managed
    @Nested
    public PinotMetricsStats getQueryStats()
    {
        return queryStats;
    }

    @Managed
    @Nested
    public PinotMetricsStats getGetStats()
    {
        return getStats;
    }

    @Managed
    @Nested
    public PinotMetricsStats getTablesStats()
    {
        return tablesStats;
    }

    @Managed
    @Nested
    public PinotMetricsStats getSchemaStats()
    {
        return schemaStats;
    }

    @Managed
    @Nested
    public PinotMetricsStats getBrokerTimeBoundaryStats()
    {
        return brokerTimeBoundaryStats;
    }

    @Managed
    @Nested
    public PinotMetricsStats getBrokerRoutingTableStats()
    {
        return brokerRoutingTableStats;
    }

    public void monitorRequest(
            Request request,
            StringResponse response,
            long duration,
            TimeUnit timeUnit)
    {
        String[] split = request.getUri().getPath().split("/");
        String secondLast = split.length >= 2 ? split[split.length - 2].toLowerCase(ENGLISH) : null;
        String last = split[split.length - 1].toLowerCase(ENGLISH);
        if ("post".equalsIgnoreCase(request.getMethod()) && "query".equalsIgnoreCase(last)) {
            queryStats.record(response, duration, timeUnit);
        }
        else if ("get".equalsIgnoreCase(request.getMethod())) {
            switch (last) {
                case "tables":
                    tablesStats.record(response, duration, timeUnit);
                    break;
                case "schema":
                    schemaStats.record(response, duration, timeUnit);
                    break;
                case "debug":
                    if (secondLast != null) {
                        switch (secondLast) {
                            case "routingtable":
                                brokerRoutingTableStats.record(response, duration, timeUnit);
                                break;
                            case "timeboundary":
                                brokerTimeBoundaryStats.record(response, duration, timeUnit);
                                break;
                        }
                    }
            }
            getStats.record(response, duration, timeUnit);
        }
    }
}
