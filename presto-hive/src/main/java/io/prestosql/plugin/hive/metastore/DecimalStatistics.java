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

import java.math.BigDecimal;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class DecimalStatistics
{
    private final Optional<BigDecimal> min;
    private final Optional<BigDecimal> max;

    @JsonCreator
    public DecimalStatistics(
            @JsonProperty("min") Optional<BigDecimal> min,
            @JsonProperty("max") Optional<BigDecimal> max)
    {
        this.min = requireNonNull(min, "min is null");
        this.max = requireNonNull(max, "max is null");
    }

    @JsonProperty
    public Optional<BigDecimal> getMin()
    {
        return min;
    }

    @JsonProperty
    public Optional<BigDecimal> getMax()
    {
        return max;
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
        DecimalStatistics that = (DecimalStatistics) o;
        return Objects.equals(min, that.min) &&
                Objects.equals(max, that.max);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(min, max);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("min", min)
                .add("max", max)
                .toString();
    }
}
