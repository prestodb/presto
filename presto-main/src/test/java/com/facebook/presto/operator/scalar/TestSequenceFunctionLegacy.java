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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.type.SqlTimestamp;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class TestSequenceFunctionLegacy
        extends TestSequenceFunctionBase
{
    public TestSequenceFunctionLegacy()
    {
        super(true);
    }

    @Override
    protected SqlTimestamp sqlTimestamp(String dateString)
    {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        ZonedDateTime zonedDateTime = LocalDateTime.parse(dateString, dateTimeFormatter).atZone(DATE_TIME_ZONE.toTimeZone().toZoneId());
        return new SqlTimestamp(zonedDateTime.toEpochSecond() * 1000 + zonedDateTime.getNano() / 1_000_000, session.getTimeZoneKey());
    }
}
