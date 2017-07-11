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
package com.facebook.presto.connector.jmx;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;

public class JmxConnectorConfig
{
    private Set<String> dumpTables = ImmutableSet.of();
    private Duration dumpPeriod = new Duration(10, SECONDS);
    private int maxEntries = 24 * 60 * 60;

    @NotNull
    public Set<String> getDumpTables()
    {
        return dumpTables;
    }

    @Config("jmx.dump-tables")
    public JmxConnectorConfig setDumpTables(String tableNames)
    {
        this.dumpTables = Splitter.on(Pattern.compile("(?<!\\\\),")) // match "," not preceded by "\"
                .omitEmptyStrings()
                .splitToList(tableNames)
                .stream()
                .map(part -> part.replace("\\,", ",")) // unescape all escaped commas
                .collect(Collectors.toSet());
        return this;
    }

    @VisibleForTesting
    JmxConnectorConfig setDumpTables(Set<String> tableNames)
    {
        this.dumpTables = ImmutableSet.copyOf(tableNames);
        return this;
    }

    @MinDuration("1ms")
    public Duration getDumpPeriod()
    {
        return dumpPeriod;
    }

    @Config("jmx.dump-period")
    public JmxConnectorConfig setDumpPeriod(Duration dumpPeriod)
    {
        this.dumpPeriod = dumpPeriod;
        return this;
    }

    @Min(1)
    public int getMaxEntries()
    {
        return maxEntries;
    }

    @Config("jmx.max-entries")
    public JmxConnectorConfig setMaxEntries(int maxEntries)
    {
        this.maxEntries = maxEntries;
        return this;
    }
}
