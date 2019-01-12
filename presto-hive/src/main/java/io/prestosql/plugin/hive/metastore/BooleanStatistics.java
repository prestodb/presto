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
package io.prestosql.plugin.hive.metastore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class BooleanStatistics
{
    private final OptionalLong trueCount;
    private final OptionalLong falseCount;

    @JsonCreator
    public BooleanStatistics(
            @JsonProperty("trueCount") OptionalLong trueCount,
            @JsonProperty("falseCount") OptionalLong falseCount)
    {
        this.trueCount = requireNonNull(trueCount, "trueCount is null");
        this.falseCount = requireNonNull(falseCount, "falseCount is null");
    }

    @JsonProperty
    public OptionalLong getTrueCount()
    {
        return trueCount;
    }

    @JsonProperty
    public OptionalLong getFalseCount()
    {
        return falseCount;
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
        BooleanStatistics that = (BooleanStatistics) o;
        return Objects.equals(trueCount, that.trueCount) &&
                Objects.equals(falseCount, that.falseCount);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(trueCount, falseCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("trueCount", trueCount)
                .add("falseCount", falseCount)
                .toString();
    }
}
