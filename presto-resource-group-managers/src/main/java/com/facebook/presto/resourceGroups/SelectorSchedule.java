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
package com.facebook.presto.resourceGroups;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.logging.log4j.core.util.CronExpression;

import java.text.ParseException;
import java.time.ZoneId;
import java.util.Objects;
import java.util.TimeZone;

import static java.util.Objects.requireNonNull;

public final class SelectorSchedule
{
    private final CronExpression expression;
    private final ZoneId timeZoneId;

    @JsonCreator
    public SelectorSchedule(@JsonProperty("schedule") String schedule)
    {
        requireNonNull(schedule, "schedule is null");

        String[] parsedSchedule = schedule.split("; ");
        if (parsedSchedule.length < 2) {
            throw new IllegalArgumentException("Invalid schedule provided");
        }

        try {
            timeZoneId = parseScheduleTimeZoneId(parsedSchedule[0]);
            expression = parseScheduleExpression(parsedSchedule[1]);

            expression.setTimeZone(TimeZone.getTimeZone(timeZoneId));
        }
        catch (ParseException e) {
            throw new IllegalArgumentException("Invalid expression provided", e);
        }
    }

    @JsonProperty
    public CronExpression getExpression()
    {
        return expression;
    }

    @JsonProperty
    public ZoneId getTimeZoneId()
    {
        return timeZoneId;
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }

        if (!(other instanceof SelectorSchedule)) {
            return false;
        }
        else {
            SelectorSchedule that = (SelectorSchedule) other;
            return this.expression.equals(that.expression) && this.timeZoneId.equals(that.timeZoneId);
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, timeZoneId);
    }

    private ZoneId parseScheduleTimeZoneId(String timeZoneExpression)
    {
        return ZoneId.of(timeZoneExpression.replaceFirst("TZ=", ""));
    }

    private CronExpression parseScheduleExpression(String expression)
            throws ParseException
    {
        return new CronExpression(expression);
    }
}
