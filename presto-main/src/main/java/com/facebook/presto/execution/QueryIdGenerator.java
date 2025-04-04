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
package com.facebook.presto.execution;

import com.facebook.presto.spi.QueryId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Chars;
import com.google.common.util.concurrent.Uninterruptibles;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.concurrent.GuardedBy;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class QueryIdGenerator
{
    // a-z, 0-9, except: l, o, 0, 1
    private static final char[] BASE_32 = {
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
            'i', 'j', 'k', 'm', 'n', 'p', 'q', 'r',
            's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
            '2', '3', '4', '5', '6', '7', '8', '9'};

    static {
        checkState(BASE_32.length == 32);
        checkState(ImmutableSet.copyOf(Chars.asList(BASE_32)).size() == 32);
    }

    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormat.forPattern("YYYYMMdd_HHmmss").withZoneUTC();
    private static final long BASE_SYSTEM_TIME_MILLIS = System.currentTimeMillis();
    private static final long BASE_NANO_TIME = System.nanoTime();

    private final String coordinatorId;
    @GuardedBy("this")
    private long lastTimeInDays;
    @GuardedBy("this")
    private long lastTimeInSeconds;
    @GuardedBy("this")
    private String lastTimestamp;
    @GuardedBy("this")
    private int counter;

    public QueryIdGenerator()
    {
        StringBuilder coordinatorId = new StringBuilder(5);
        for (int i = 0; i < 5; i++) {
            coordinatorId.append(BASE_32[ThreadLocalRandom.current().nextInt(32)]);
        }
        this.coordinatorId = coordinatorId.toString();
    }

    public String getCoordinatorId()
    {
        return coordinatorId;
    }

    /**
     * Generate next queryId using the following format:
     * <tt>YYYYMMdd_HHmmss_index_coordId</tt>
     * <p/>
     * Index rolls at the start of every day or when it reaches 99,999, and the
     * coordId is a randomly generated when this instance is created.
     */
    public synchronized QueryId createNextQueryId()
    {
        // only generate 100,000 ids per day
        if (counter > 99_999) {
            // wait for the second to rollover
            while (MILLISECONDS.toSeconds(nowInMillis()) == lastTimeInSeconds) {
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
            counter = 0;
        }

        // if it has been a second since the last id was generated, generate a new timestamp
        long now = nowInMillis();
        if (MILLISECONDS.toSeconds(now) != lastTimeInSeconds) {
            // generate new timestamp
            lastTimeInSeconds = MILLISECONDS.toSeconds(now);
            lastTimestamp = TIMESTAMP_FORMAT.print(now);

            // if the day has rolled over, restart the counter
            if (MILLISECONDS.toDays(now) != lastTimeInDays) {
                lastTimeInDays = MILLISECONDS.toDays(now);
                counter = 0;
            }
        }

        return new QueryId(String.format("%s_%05d_%s", lastTimestamp, counter++, coordinatorId));
    }

    @VisibleForTesting
    protected long nowInMillis()
    {
        // avoid problems with the clock moving backwards
        return BASE_SYSTEM_TIME_MILLIS + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - BASE_NANO_TIME);
    }
}
