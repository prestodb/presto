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

import io.airlift.log.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.util.concurrent.atomic.AtomicBoolean;

final class CodeCacheGcTrigger
{
    private static final AtomicBoolean installed = new AtomicBoolean();

    private CodeCacheGcTrigger() {}

    public static void installCodeCacheGcTrigger()
    {
        if (installed.getAndSet(true)) {
            return;
        }

        // Hack to work around bugs in java 7 related to code cache management.
        // See http://mail.openjdk.java.net/pipermail/hotspot-compiler-dev/2013-August/011333.html for more info.
        final MemoryPoolMXBean codeCacheMbean = findCodeCacheMBean();

        Thread gcThread = new Thread(new Runnable()
        {
            @SuppressWarnings("CallToSystemGC")
            @Override
            public void run()
            {
                Logger log = Logger.get("Code-Cache-GC-Trigger");

                while (!Thread.currentThread().isInterrupted()) {
                    long used = codeCacheMbean.getUsage().getUsed();
                    long max = codeCacheMbean.getUsage().getMax();

                    if (used > 0.95 * max) {
                        log.error("Code Cache is more than 95% full. JIT may stop working.");
                    }
                    if (used > 0.7 * max) {
                        // Due to some obscure bug in hotspot (java 7), once the code cache fills up the JIT stops compiling and never recovers from this condition.
                        // By forcing classes to unload from the perm gen, we let the code cache evictor make room before the cache fills up.
                        // For best results, the server should be run with -XX:+UseConcMarkSweepGC -XX:+ExplicitGCInvokesConcurrent -XX:+CMSClassUnloadingEnabled
                        log.info("Triggering GC to avoid Code Cache eviction bugs");
                        System.gc();
                    }

                    try {
                        Thread.sleep(1000);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
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
