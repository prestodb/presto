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
package com.facebook.presto.util;

import com.facebook.airlift.log.Logger;
import com.sun.management.HotSpotDiagnosticMXBean;

import javax.management.MBeanServer;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicBoolean;

public final class HeapDumper
{
    private static final Logger log = Logger.get(HeapDumper.class);
    private static final String HOTSPOT_BEAN_NAME = "com.sun.management:type=HotSpotDiagnostic";
    private static final AtomicBoolean IS_HEAPDUMP_TRIGGERED = new AtomicBoolean(false);

    private static volatile HotSpotDiagnosticMXBean hotspotMBean;

    private HeapDumper() {}

    /**
     * Call this method from your application whenever you
     * want to dump the heap snapshot into a file.
     *
     * @param fileName name of the heap dump file
     */
    public static void dumpHeap(String fileName)
    {
        if (IS_HEAPDUMP_TRIGGERED.compareAndSet(false, true)) {
            log.info("Performing heapdump to file: " + fileName);
            try {
                if (hotspotMBean == null) {
                    hotspotMBean = getHotspotMBean();
                }
                hotspotMBean.dumpHeap(fileName, false);
            }
            catch (Throwable throwable) {
                // Consume the error as we do not want to fail during heapdump
                log.error(throwable, "Unable to perform heap dump");
            }
        }
    }

    private static HotSpotDiagnosticMXBean getHotspotMBean()
            throws IOException
    {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        return ManagementFactory.newPlatformMXBeanProxy(server, HOTSPOT_BEAN_NAME, HotSpotDiagnosticMXBean.class);
    }
}
