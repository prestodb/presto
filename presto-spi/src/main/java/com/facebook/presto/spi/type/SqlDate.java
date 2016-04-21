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
package com.facebook.presto.spi.type;

import com.fasterxml.jackson.annotation.JsonValue;

import java.time.LocalDate;
import java.util.Objects;

public final class SqlDate
{
    private final int days;

    public SqlDate(int days)
    {
        this.days = days;
    }

    public int getDays()
    {
        return days;
    }

    @Override
    public int hashCode()
    {
        return days;
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
        SqlDate other = (SqlDate) obj;
        return Objects.equals(days, other.days);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return LocalDate.ofEpochDay(days).toString();
    }
}
