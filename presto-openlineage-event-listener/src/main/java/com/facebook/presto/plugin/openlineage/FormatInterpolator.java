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
package com.facebook.presto.plugin.openlineage;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * Simple replacement for Trino's FormatInterpolator from trino-plugin-toolkit.
 * Replaces $PLACEHOLDER tokens in a format string with values from the provided context.
 */
public class FormatInterpolator
{
    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\$([A-Z_]+)");
    // Valid format: only letters, digits, underscores, hyphens, commas, spaces, equal signs, and $PLACEHOLDER tokens
    private static final Pattern VALID_FORMAT_PATTERN = Pattern.compile("^([a-zA-Z0-9_\\-,= ]|\\$(" +
            "QUERY_ID|USER|SOURCE|CLIENT_IP))*$");

    private final String format;
    private final OpenLineageJobInterpolatedValues[] values;

    public FormatInterpolator(String format, OpenLineageJobInterpolatedValues[] values)
    {
        this.format = requireNonNull(format, "format is null");
        this.values = requireNonNull(values, "values is null");
    }

    public String interpolate(OpenLineageJobContext context)
    {
        Matcher matcher = PLACEHOLDER_PATTERN.matcher(format);
        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            String placeholder = matcher.group(1);
            String replacement = getValueForPlaceholder(placeholder, context);
            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    private String getValueForPlaceholder(String placeholder, OpenLineageJobContext context)
    {
        for (OpenLineageJobInterpolatedValues value : values) {
            if (value.name().equals(placeholder)) {
                return value.value(context);
            }
        }
        return "$" + placeholder;
    }

    public static boolean hasValidPlaceholders(String format, OpenLineageJobInterpolatedValues[] values)
    {
        return VALID_FORMAT_PATTERN.matcher(format).matches();
    }
}
