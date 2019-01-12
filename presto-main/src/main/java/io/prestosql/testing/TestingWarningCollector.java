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
package io.prestosql.testing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.execution.warnings.WarningCollectorConfig;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.WarningCode;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class TestingWarningCollector
        implements WarningCollector
{
    @GuardedBy("this")
    private final Map<WarningCode, PrestoWarning> warnings = new LinkedHashMap<>();
    private final WarningCollectorConfig config;

    private final boolean addWarnings;
    private final AtomicInteger warningCode = new AtomicInteger();

    public TestingWarningCollector(WarningCollectorConfig config, TestingWarningCollectorConfig testConfig)
    {
        this.config = requireNonNull(config, "config is null");
        requireNonNull(testConfig, "testConfig is null");
        addWarnings = testConfig.getAddWarnings();
        // Start warning codes at 1
        for (int warningCode = 1; warningCode <= testConfig.getPreloadedWarnings(); warningCode++) {
            add(createTestWarning(warningCode));
        }
        warningCode.set(testConfig.getPreloadedWarnings());
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
        if (addWarnings) {
            add(createTestWarning(warningCode.incrementAndGet()));
        }
        return ImmutableList.copyOf(warnings.values());
    }

    @VisibleForTesting
    public static PrestoWarning createTestWarning(int code)
    {
        // format string below is a hack to construct a vendor specific SQLState value
        // 01 is the class of warning code and 5 is the first allowed vendor defined prefix character
        // See the SQL Standard ISO_IEC_9075-2E_2016 24.1: SQLState for more information
        return new PrestoWarning(new WarningCode(code, format("015%02d", code % 100)), "Test warning " + code);
    }
}
