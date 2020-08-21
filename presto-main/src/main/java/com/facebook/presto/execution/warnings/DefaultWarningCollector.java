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
package com.facebook.presto.execution.warnings;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.WarningCode;
import com.facebook.presto.spi.WarningCollector;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.WARNING_AS_ERROR;
import static com.facebook.presto.spi.StandardWarningCode.PARSER_WARNING;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class DefaultWarningCollector
        implements WarningCollector
{
    @GuardedBy("this")
    private final Map<WarningCode, PrestoWarning> warnings = new LinkedHashMap<>();
    private final WarningCollectorConfig config;
    private final WarningHandlingLevel warningHandlingLevel;

    public DefaultWarningCollector(WarningCollectorConfig config, WarningHandlingLevel warningHandlingLevel)
    {
        this.config = requireNonNull(config, "config is null");
        this.warningHandlingLevel = warningHandlingLevel;
    }

    @Override
    public synchronized void add(PrestoWarning warning)
    {
        requireNonNull(warning, "warning is null");
        switch (warningHandlingLevel) {
            case SUPPRESS:
                break;
            case NORMAL:
                addWarningIfNumWarningsLessThanConfig(warning);
                break;
            case AS_ERROR:
                /* Parser warnings must be handled differently since we should not throw an exception when parsing.
                 * Warnings are collected and an exception with all warnings is thrown in {@link QueryPreparer#prepareQuery}
                 */
                if (warning.getWarningCode() == PARSER_WARNING.toWarningCode()) {
                    addWarningIfNumWarningsLessThanConfig(warning);
                }
                else {
                    throw new PrestoException(WARNING_AS_ERROR, warning.toString());
                }
        }
    }

    @Override
    public synchronized List<PrestoWarning> getWarnings()
    {
        return ImmutableList.copyOf(warnings.values());
    }

    private synchronized void addWarningIfNumWarningsLessThanConfig(PrestoWarning warning)
    {
        if (warnings.size() < config.getMaxWarnings()) {
            warnings.putIfAbsent(warning.getWarningCode(), warning);
        }
    }

    @Override
    public synchronized boolean hasWarnings()
    {
        return !warnings.isEmpty();
    }
}
