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
package io.prestosql.execution.warnings;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.WarningCode;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public class DefaultWarningCollector
        implements WarningCollector
{
    @GuardedBy("this")
    private final Map<WarningCode, PrestoWarning> warnings = new LinkedHashMap<>();
    private final WarningCollectorConfig config;

    public DefaultWarningCollector(WarningCollectorConfig config)
    {
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public synchronized void add(PrestoWarning warning)
    {
        requireNonNull(warning, "warning is null");
        if (warnings.size() < config.getMaxWarnings()) {
            warnings.putIfAbsent(warning.getWarningCode(), warning);
        }
    }

    @Override
    public synchronized List<PrestoWarning> getWarnings()
    {
        return ImmutableList.copyOf(warnings.values());
    }
}
