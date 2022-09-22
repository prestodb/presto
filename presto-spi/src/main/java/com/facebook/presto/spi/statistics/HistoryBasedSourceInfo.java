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
package com.facebook.presto.spi.statistics;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Describes plan statistics which are derived from history based optimizer.
 */
public class HistoryBasedSourceInfo
        extends SourceInfo
{
    private final Optional<String> hash;
    private final Optional<List<PlanStatistics>> inputTableStatistics;

    public HistoryBasedSourceInfo(Optional<String> hash, Optional<List<PlanStatistics>> inputTableStatistics)
    {
        this.hash = requireNonNull(hash, "hash is null");
        this.inputTableStatistics = requireNonNull(inputTableStatistics, "inputTableStatistics is null");
    }

    public Optional<String> getHash()
    {
        return hash;
    }

    public Optional<List<PlanStatistics>> getInputTableStatistics()
    {
        return inputTableStatistics;
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
        HistoryBasedSourceInfo that = (HistoryBasedSourceInfo) o;
        return Objects.equals(hash, that.hash) && Objects.equals(inputTableStatistics, that.inputTableStatistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(hash, inputTableStatistics);
    }
}
