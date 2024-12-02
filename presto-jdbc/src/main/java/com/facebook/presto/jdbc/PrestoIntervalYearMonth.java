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
package com.facebook.presto.jdbc;

import java.util.Objects;

import static com.facebook.presto.common.type.IntervalYearMonth.formatMonths;
import static com.facebook.presto.common.type.IntervalYearMonth.toMonths;

public class PrestoIntervalYearMonth
        implements Comparable<PrestoIntervalYearMonth>
{
    private final int months;

    public PrestoIntervalYearMonth(int months)
    {
        this.months = months;
    }

    public PrestoIntervalYearMonth(int year, int months)
    {
        this.months = toMonths(year, months);
    }

    public int getMonths()
    {
        return months;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(months);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PrestoIntervalYearMonth other = (PrestoIntervalYearMonth) obj;
        return this.months == other.months;
    }

    @Override
    public int compareTo(PrestoIntervalYearMonth o)
    {
        return Integer.compare(months, o.months);
    }

    @Override
    public String toString()
    {
        return formatMonths(months);
    }
}
