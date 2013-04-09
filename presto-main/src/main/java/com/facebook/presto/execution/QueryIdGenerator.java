/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Chars;
import com.google.common.util.concurrent.Uninterruptibles;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.concurrent.GuardedBy;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;

public class QueryIdGenerator
{
    private static final char[] BASE_32 = new char[] {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', /*'l',*/ 'm', 'n', /*'o',*/ 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', /*'0', '1',*/ '2', '3', '4', '5', '6', '7', '8', '9'};
    static {
        checkState(BASE_32.length == 32);
        checkState(ImmutableSet.copyOf(Chars.asList(BASE_32)).size() == 32);
    }

    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormat.forPattern("yyyyMMdd_hhmmss").withZoneUTC();
    private static final long BASE_SYSTEM_TIME_MILLIS = System.currentTimeMillis();
    private static final long BASE_NANO_TIME = System.nanoTime();

    private final String coordinatorId;
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

    public synchronized QueryId createNextQueryId()
    {
        // only generate 1000 ids per second
        if (counter > 999) {
            // this will never happen so just be safe
            while (now() / 1000 <= lastTimeInSeconds) {
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
        }

        // if it has been a second since the last id was generated, generate a new timestamp and reset the counter
        long now = now();
        if (now / 1000 != lastTimeInSeconds) {
            // generate new timestamp
            lastTimeInSeconds = now / 1000;
            lastTimestamp = TIMESTAMP_FORMAT.print(now);
            counter = 0;
        }

        // yyMMddhhmmssIIICCC
        return new QueryId(String.format("%s_%03d_%s", lastTimestamp, counter++, coordinatorId));
    }

    private long now()
    {
        // avoid problems with the clock moving backwards
        return BASE_SYSTEM_TIME_MILLIS + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - BASE_NANO_TIME);
    }
}
