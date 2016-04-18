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
package com.facebook.presto.connector.jmx;

import com.facebook.presto.spi.SchemaTableName;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class JmxHistoryDumper
        implements Runnable
{
    private static final Logger log = Logger.get(JmxHistoryDumper.class);

    private final JmxHistoryHolder jmxHistoryHolder;
    private final JmxMetadata jmxMetadata;
    private final JmxRecordSetProvider jmxRecordSetProvider;
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
    private final long period;

    public JmxHistoryDumper(JmxHistoryHolder jmxHistoryHolder, JmxMetadata jmxMetadata, JmxRecordSetProvider jmxRecordSetProvider, Duration period)
    {
        this.jmxHistoryHolder = requireNonNull(jmxHistoryHolder, "jmxStatsHolder is null");
        this.jmxMetadata = requireNonNull(jmxMetadata, "jmxMetadata is null");
        this.jmxRecordSetProvider = requireNonNull(jmxRecordSetProvider, "jmxRecordSetProvider is null");
        this.period = requireNonNull(period.toMillis(), "period is null");
    }

    public void start()
    {
        long nowMillis = getNowMillis();
        long initialDelay = nowMillis - roundToPeriod(nowMillis + period);
        executor.scheduleAtFixedRate(this, initialDelay, period, MILLISECONDS);
    }

    private long roundToPeriod(long millis)
    {
        return ((millis + period / 2) / period) * period;
    }

    @Override
    public void run()
    {
        // we are using rounded up timestamp, so that records from different nodes and different
        // tables will have matching timestamps (for joining/grouping etc)
        long dumpTs = roundToPeriod(getNowMillis());

        for (String tableName : jmxHistoryHolder.getTables()) {
            try {
            JmxTableHandle tableHandle = requireNonNull(
                    jmxMetadata.getTableHandle(new SchemaTableName(JmxMetadata.HISTORY_SCHEMA_NAME, tableName)),
                    format("tableHandle is null for table [%s]", tableName));

                List<Object> row = jmxRecordSetProvider.getLiveRow(
                        tableHandle,
                        jmxRecordSetProvider.getColumnTypes(tableHandle.getColumns()),
                        dumpTs);
                jmxHistoryHolder.addRow(tableName, row);
            }
            catch (Throwable ex) {
                log.error(ex, "Error starting JmxHistoryDumper");
            }
        }
    }

    public void shutdown()
    {
        executor.shutdown();
    }

    private static long getNowMillis()
    {
        return MILLISECONDS.convert(System.nanoTime(), NANOSECONDS);
    }
}
