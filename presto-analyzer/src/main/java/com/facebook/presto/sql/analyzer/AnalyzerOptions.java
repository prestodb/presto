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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.common.WarningHandlingLevel;
import com.facebook.presto.sql.parser.ParsingOptions;

/**
 * Various options required at different stage of query analysis.
 */
public class AnalyzerOptions
{
    private final ParsingOptions parsingOptions;
    private final boolean isLogFormattedQueryEnabled;
    private final WarningHandlingLevel warningHandlingLevel;

    private AnalyzerOptions(
            ParsingOptions parsingOptions,
            boolean isLogFormattedQueryEnabled,
            WarningHandlingLevel warningHandlingLevel)
    {
        this.parsingOptions = parsingOptions;
        this.isLogFormattedQueryEnabled = isLogFormattedQueryEnabled;
        this.warningHandlingLevel = warningHandlingLevel;
    }

    public ParsingOptions getParsingOptions()
    {
        return parsingOptions;
    }

    public boolean isLogFormattedQueryEnabled()
    {
        return isLogFormattedQueryEnabled;
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
        private ParsingOptions parsingOptions;
        private boolean isLogFormattedQueryEnabled;
        private WarningHandlingLevel warningHandlingLevel;

        private Builder() {}

        public Builder setParsingOptions(ParsingOptions parsingOptions)
        {
            this.parsingOptions = parsingOptions;
            return this;
        }

        public Builder setLogFormattedQueryEnabled(boolean logFormattedQueryEnabled)
        {
            isLogFormattedQueryEnabled = logFormattedQueryEnabled;
            return this;
        }

        public Builder setWarningHandlingLevel(WarningHandlingLevel warningHandlingLevel)
        {
            this.warningHandlingLevel = warningHandlingLevel;
            return this;
        }

        public AnalyzerOptions build()
        {
            return new AnalyzerOptions(parsingOptions, isLogFormattedQueryEnabled, warningHandlingLevel);
        }
    }
}
