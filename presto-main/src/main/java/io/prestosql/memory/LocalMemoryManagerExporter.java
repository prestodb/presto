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
package io.prestosql.memory;

import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.ObjectNames;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public final class LocalMemoryManagerExporter
{
    private final MBeanExporter exporter;
    @GuardedBy("this")
    private final List<MemoryPool> pools = new ArrayList<>();

    @Inject
    public LocalMemoryManagerExporter(LocalMemoryManager memoryManager, MBeanExporter exporter)
    {
        this.exporter = requireNonNull(exporter, "exporter is null");
        for (MemoryPool pool : memoryManager.getPools()) {
            addPool(pool);
        }
    }

    private synchronized void addPool(MemoryPool pool)
    {
        try {
            exporter.exportWithGeneratedName(pool, MemoryPool.class, pool.getId().toString());
            pools.add(pool);
        }
        catch (JmxException e) {
            // ignored
        }
    }

    @PreDestroy
    public synchronized void destroy()
    {
        for (MemoryPool pool : pools) {
            String objectName = ObjectNames.builder(MemoryPool.class, pool.getId().toString()).build();
            try {
                exporter.unexport(objectName);
            }
            catch (JmxException e) {
                // ignored
            }
        }
        pools.clear();
    }
}
