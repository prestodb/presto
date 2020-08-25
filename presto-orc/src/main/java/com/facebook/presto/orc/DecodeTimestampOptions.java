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
package com.facebook.presto.orc;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DecodeTimestampOptions
{
    private final long unitsPerSecond;
    private final long nanosecondsPerUnit;
    private final long baseSeconds;

    public DecodeTimestampOptions(DateTimeZone hiveStorageTimeZone, boolean enableMicroPrecision)
    {
        TimeUnit timeUnit = enableMicroPrecision ? MICROSECONDS : MILLISECONDS;

        requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");

        this.unitsPerSecond = timeUnit.convert(1, TimeUnit.SECONDS);
        this.nanosecondsPerUnit = TimeUnit.NANOSECONDS.convert(1, timeUnit);

        this.baseSeconds = MILLISECONDS.toSeconds(new DateTime(2015, 1, 1, 0, 0, hiveStorageTimeZone).getMillis());
    }

    public long getUnitsPerSecond()
    {
        return unitsPerSecond;
    }

    public long getNanosPerUnit()
    {
        return nanosecondsPerUnit;
    }

    /**
     * @return Seconds since 01/01/2015 (see https://orc.apache.org/specification/ORCv1/) in hive storage timezone (see {@link DecodeTimestampOptions#DecodeTimestampOptions(DateTimeZone, boolean)} })
     */
    public long getBaseSeconds()
    {
        return baseSeconds;
    }
}
