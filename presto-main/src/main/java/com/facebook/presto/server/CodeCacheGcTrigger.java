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
package com.facebook.presto.server;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.PostConstruct;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

final class CodeCacheGcTrigger
{
    private static final Logger log = Logger.get(CodeCacheGcTrigger.class);
    private static final AtomicBoolean installed = new AtomicBoolean();

    private final Duration interval;

    @Inject
    public CodeCacheGcTrigger(CodeCacheGcConfig config)
    {
        this.interval = config.getCodeCacheCheckInterval();
    }

    @PostConstruct
    public void start()
    {
        installCodeCacheGcTrigger();
    }

    public void installCodeCacheGcTrigger()
    {
        if (installed.getAndSet(true)) {
            return;
        }

        // Hack to work around bugs in java 8 (8u45+) related to code cache management.
        // See http://openjdk.5641.n7.nabble.com/JIT-stops-compiling-after-a-while-java-8u45-td259603.html for more info.
        MemoryPoolMXBean codeCacheMbean = findCodeCacheMBean();

        Thread gcThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                long used = codeCacheMbean.getUsage().getUsed();
                long max = codeCacheMbean.getUsage().getMax();

                if (used > 0.95 * max) {
                    log.error("Code Cache is more than 95% full. JIT may stop working.");
                }
                if (used > 0.7 * max) {
                    // Due to some obscure bug in hotspot (java 8), once the code cache fills up the JIT stops compiling
                    // By forcing a GC, we let the code cache evictor make room before the cache fills up.
                    log.info("Triggering GC to avoid Code Cache eviction bugs");
                    System.gc();
                }

                try {
                    TimeUnit.MILLISECONDS.sleep(interval.toMillis());
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        gcThread.setDaemon(true);
        gcThread.setName("Code-Cache-GC-Trigger");
        gcThread.start();
    }

    private static MemoryPoolMXBean findCodeCacheMBean()
    {
        for (MemoryPoolMXBean bean : ManagementFactory.getMemoryPoolMXBeans()) {
            if (bean.getName().equals("Code Cache")) {
                return bean;
            }
        }
        throw new RuntimeException("Could not obtain a reference to the 'Code Cache' MemoryPoolMXBean");
    }
}
