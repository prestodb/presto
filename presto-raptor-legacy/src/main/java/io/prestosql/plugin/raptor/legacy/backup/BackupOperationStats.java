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
package io.prestosql.plugin.raptor.legacy.backup;

import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;
import io.prestosql.spi.PrestoException;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.function.Supplier;

import static io.prestosql.plugin.raptor.legacy.RaptorErrorCode.RAPTOR_BACKUP_NOT_FOUND;
import static io.prestosql.plugin.raptor.legacy.RaptorErrorCode.RAPTOR_BACKUP_TIMEOUT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BackupOperationStats
{
    private final TimeStat time = new TimeStat(MILLISECONDS);
    private final CounterStat successes = new CounterStat();
    private final CounterStat failures = new CounterStat();
    private final CounterStat timeouts = new CounterStat();

    @Managed
    @Nested
    public TimeStat getTime()
    {
        return time;
    }

    @Managed
    @Nested
    public CounterStat getSuccesses()
    {
        return successes;
    }

    @Managed
    @Nested
    public CounterStat getFailures()
    {
        return failures;
    }

    @Managed
    @Nested
    public CounterStat getTimeouts()
    {
        return timeouts;
    }

    public void run(Runnable runnable)
    {
        run(() -> {
            runnable.run();
            return null;
        });
    }

    public <V> V run(Supplier<V> supplier)
    {
        try (TimeStat.BlockTimer ignored = time.time()) {
            V value = supplier.get();
            successes.update(1);
            return value;
        }
        catch (PrestoException e) {
            if (e.getErrorCode().equals(RAPTOR_BACKUP_NOT_FOUND.toErrorCode())) {
                successes.update(1);
            }
            else if (e.getErrorCode().equals(RAPTOR_BACKUP_TIMEOUT.toErrorCode())) {
                timeouts.update(1);
            }
            else {
                failures.update(1);
            }
            throw e;
        }
    }
}
