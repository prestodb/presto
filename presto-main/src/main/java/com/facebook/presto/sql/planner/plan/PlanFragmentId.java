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
package com.facebook.presto.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import javax.annotation.concurrent.Immutable;

@Immutable
public class PlanFragmentId
        implements Comparable<PlanFragmentId>
{
    private final int id;

    @JsonCreator
    public PlanFragmentId(int id)
    {
        this.id = id;
    }

    @JsonValue
    public int getId()
    {
        return id;
    }

    @Override
    public String toString()
    {
        return Integer.toString(id);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PlanFragmentId that = (PlanFragmentId) o;

        return id == that.id;
    }

    @Override
    public int hashCode()
    {
        return Integer.hashCode(id);
    }

    @Override
    public int compareTo(PlanFragmentId o)
    {
        return Integer.compare(id, o.id);
    }
}
