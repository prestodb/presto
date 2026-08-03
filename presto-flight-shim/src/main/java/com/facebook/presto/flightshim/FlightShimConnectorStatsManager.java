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
package com.facebook.presto.flightshim;

import com.facebook.airlift.log.Logger;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.ObjectNames;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public class FlightShimConnectorStatsManager
{
    private static final Logger log = Logger.get(FlightShimConnectorStatsManager.class);

    private final MBeanExporter exporter;
    private final ConcurrentHashMap<String, FlightShimConnectorStats> statsByConnectorId = new ConcurrentHashMap<>();
    @GuardedBy("this")
    private final List<String> exportedObjectNames = new ArrayList<>();

    @Inject
    public FlightShimConnectorStatsManager(MBeanExporter exporter)
    {
        this.exporter = requireNonNull(exporter, "exporter is null");
    }

    public FlightShimConnectorStats getOrCreate(String connectorId)
    {
        FlightShimConnectorStats existing = statsByConnectorId.get(connectorId);
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            existing = statsByConnectorId.get(connectorId);
            if (existing != null) {
                return existing;
            }

            FlightShimConnectorStats stats = new FlightShimConnectorStats(connectorId);
            String objectName = ObjectNames.builder(FlightShimConnectorStats.class, connectorId).build();
            try {
                exporter.export(objectName, stats);
                exportedObjectNames.add(objectName);
            }
            catch (JmxException e) {
                log.warn(e, "Failed to export MBean %s", objectName);
            }
            statsByConnectorId.put(connectorId, stats);
            return stats;
        }
    }

    @PreDestroy
    public synchronized void destroy()
    {
        for (String objectName : exportedObjectNames) {
            try {
                exporter.unexport(objectName);
            }
            catch (JmxException e) {
                log.warn(e, "Failed to unexport MBean %s", objectName);
            }
        }
        exportedObjectNames.clear();
        statsByConnectorId.clear();
    }
}
