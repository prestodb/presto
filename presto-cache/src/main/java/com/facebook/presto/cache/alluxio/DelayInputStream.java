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
package com.facebook.presto.cache.alluxio;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.hive.HiveFileContext;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.common.RuntimeUnit.NANO;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DelayInputStream
        extends FSDataInputStream
{
    private static final Logger log = Logger.get(DelayInputStream.class);

    private final FSDataInputStream cachingInputStream;
    private final RuntimeStats runtimeStats;
    private final int percentile;
    private final int delayInMs;
    private final int minNonSlo;
    private final int maxNonSlo;

    public DelayInputStream(FSDataInputStream cachingInputStream, HiveFileContext hiveFileContext)
    {
        super(cachingInputStream);
        this.cachingInputStream = requireNonNull(cachingInputStream, "cachingInputStream is null");
        this.runtimeStats = hiveFileContext.getStats();
        this.percentile = (int) (100 * hiveFileContext.getPercentile());
        this.delayInMs = hiveFileContext.getDelayInMs();
        this.minNonSlo = hiveFileContext.getMinNonSlo();
        this.maxNonSlo = hiveFileContext.getMaxNonSlo();
    }

    @Override
    public int read()
            throws IOException
    {
        int outByte = cachingInputStream.read();

        try {
            int x = getSleepDuration();
            log.info(format("NIKHIL sleeping for %d ms", x));
            Thread.sleep(x);
        }
        catch (InterruptedException e) {
            // ignored
        }
        return outByte;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        int bytes = cachingInputStream.read(position, buffer, offset, length);

        try {
            int y = getSleepDuration();
            log.info(format("NIKHIL sleeping for %d ms", y));
            Thread.sleep(y);
        }
        catch (InterruptedException e) {
            // ignored
        }
        return bytes;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        int totalBytesRead = 0;
        while (totalBytesRead < length) {
            int bytesRead = read(
                    position + totalBytesRead,
                    buffer,
                    offset + totalBytesRead,
                    length - totalBytesRead);
            if (bytesRead == -1) {
                throw new EOFException();
            }
            totalBytesRead += bytesRead;
        }
    }

    private int getSleepDuration()
    {
        int value = ThreadLocalRandom.current().nextInt(10000);
        /* lets say percentile = 9900 and delayInMs = 200ms . Then we pick a random value X between 0 and 10000.
         * If X <= 9900 , we will wait for Y ms where 50ms <= Y <= 200ms (delayInMs)
         * else If X > 9900, we will wait for Z ms where 2sec < Z <= 10sec
         */
        if (value <= percentile) {
            return ThreadLocalRandom.current().nextInt(50, delayInMs);
        }
        else {
            int slowLatency = ThreadLocalRandom.current().nextInt(minNonSlo, maxNonSlo);
            log.info(format("NIKHIL HIGH NON-SLO compliant sleeping for %d ms", slowLatency));
            runtimeStats.addMetricValue("HIGHER_WS_LATENCY", NANO, slowLatency * 1000000L);
            return slowLatency;
        }
    }

    @Override
    public void readFully(long position, byte[] buffer)
            throws IOException
    {
        readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public void seek(long position)
            throws IOException
    {
        cachingInputStream.seek(position);
    }

    @Override
    public long getPos()
            throws IOException
    {
        return cachingInputStream.getPos();
    }

    @Override
    public boolean seekToNewSource(long target)
    {
        throw new UnsupportedOperationException();
    }
}
