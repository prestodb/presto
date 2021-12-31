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

package com.facebook.presto.hive.functions.type;

import java.sql.Date;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

public final class DateTimeUtils
{
    private DateTimeUtils() {}

    public static Date createDate(Object days)
    {
        long millis = TimeUnit.DAYS.toMillis(((long) days));
        Instant instant = Instant.ofEpochMilli((millis));
        OffsetDateTime dt = OffsetDateTime.ofInstant(instant, ZoneId.of("UTC"));
        // A trick to prevent including zone info
        return new Date(dt.getYear() - 1900, dt.getMonthValue() - 1, dt.getDayOfMonth());
    }
}
