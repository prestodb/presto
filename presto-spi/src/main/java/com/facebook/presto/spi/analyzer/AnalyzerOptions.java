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
package com.facebook.presto.spi.analyzer;

import com.facebook.presto.common.WarningHandlingLevel;
import com.facebook.presto.spi.WarningCollector;

import java.util.Optional;

import static com.facebook.presto.common.WarningHandlingLevel.NORMAL;
import static java.util.Objects.requireNonNull;

/**
 * Various options required at different stage of query analysis.
 */
public class AnalyzerOptions
{
    private final boolean isParseDecimalLiteralsAsDouble;
    private final boolean isLogFormattedQueryEnabled;
    private final WarningHandlingLevel warningHandlingLevel;
    private final Optional<String> sessionCatalogName;
    private final Optional<String> sessionSchemaName;
    private final WarningCollector warningCollector;

    private AnalyzerOptions(
            boolean isParseDecimalLiteralsAsDouble,
            boolean isLogFormattedQueryEnabled,
            WarningCollector warningCollector,
            Optional<String> sessionCatalogName,
            Optional<String> sessionSchemaName,
            WarningHandlingLevel warningHandlingLevel)
    {
        this.isParseDecimalLiteralsAsDouble = isParseDecimalLiteralsAsDouble;
        this.isLogFormattedQueryEnabled = isLogFormattedQueryEnabled;
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.sessionCatalogName = requireNonNull(sessionCatalogName, "sessionCatalogName is null");
        this.sessionSchemaName = requireNonNull(sessionSchemaName, "sessionSchemaName is null");
        this.warningHandlingLevel = requireNonNull(warningHandlingLevel, "warningHandlingLevel is null");
    }

    public boolean isParseDecimalLiteralsAsDouble()
    {
        return isParseDecimalLiteralsAsDouble;
    }

    public boolean isLogFormattedQueryEnabled()
    {
        return isLogFormattedQueryEnabled;
    }

    public WarningCollector getWarningCollector()
    {
        return warningCollector;
    }

    public Optional<String> getSessionCatalogName()
    {
        return sessionCatalogName;
    }

    public Optional<String> getSessionSchemaName()
    {
        return sessionSchemaName;
    }

    public WarningHandlingLevel getWarningHandlingLevel()
    {
        return warningHandlingLevel;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private boolean isParseDecimalLiteralsAsDouble;
        private boolean isLogFormattedQueryEnabled;
        private WarningCollector warningCollector = WarningCollector.NOOP;
        private WarningHandlingLevel warningHandlingLevel = NORMAL;
        private Optional<String> sessionCatalogName = Optional.empty();
        private Optional<String> sessionSchemaName = Optional.empty();

        private Builder() {}

        public Builder setParseDecimalLiteralsAsDouble(boolean parseDecimalLiteralsAsDouble)
        {
            isParseDecimalLiteralsAsDouble = parseDecimalLiteralsAsDouble;
            return this;
        }

        public Builder setLogFormattedQueryEnabled(boolean logFormattedQueryEnabled)
        {
            isLogFormattedQueryEnabled = logFormattedQueryEnabled;
            return this;
        }

        public Builder setWarningCollector(WarningCollector warningCollector)
        {
            this.warningCollector = warningCollector;
            return this;
        }

        public Builder setWarningHandlingLevel(WarningHandlingLevel warningHandlingLevel)
        {
            this.warningHandlingLevel = warningHandlingLevel;
            return this;
        }

        public Builder setSessionCatalogName(Optional<String> sessionCatalogName)
        {
            this.sessionCatalogName = sessionCatalogName;
            return this;
        }

        public Builder setSessionSchemaName(Optional<String> sessionSchemaName)
        {
            this.sessionSchemaName = sessionSchemaName;
            return this;
        }

        public AnalyzerOptions build()
        {
            return new AnalyzerOptions(isParseDecimalLiteralsAsDouble, isLogFormattedQueryEnabled, warningCollector, sessionCatalogName, sessionSchemaName, warningHandlingLevel);
        }
    }
}
