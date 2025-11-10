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
package com.facebook.presto.plugin.clp.split.filter;

import com.facebook.presto.plugin.clp.ClpConfig;
import com.google.inject.Inject;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;

/**
 * Uber-specific split filter provider for Pinot metadata databases.
 * <p>
 * This provider extends the standard Pinot filter provider and adds TEXT_MATCH
 * transformations specific to Uber's Pinot infrastructure. It inherits all range
 * mapping functionality from the parent class while adding support for transforming
 * equality predicates into TEXT_MATCH expressions for efficient querying against
 * Uber's merged text indices.
 * </p>
 * <p>
 * Example transformations:
 * <ul>
 *   <li>{@code "x" = 1} → {@code TEXT_MATCH("__mergedTextIndex", '/1:x/')}</li>
 *   <li>{@code "x" = 'abc'} → {@code TEXT_MATCH("__mergedTextIndex", '/abc:x/')}</li>
 *   <li>{@code "timestamp" >= 1234} → {@code end_timestamp >= 1234} (via inherited range mapping)</li>
 * </ul>
 * </p>
 */
public class ClpUberPinotSplitFilterProvider
        extends ClpPinotSplitFilterProvider
{
    private static final String MERGED_TEXT_INDEX_COLUMN = "__mergedTextIndex";

    // Pattern to match quoted column = value expressions (both numeric and string values)
    // Pre-compiled for performance
    private static final Pattern EQUALITY_PATTERN = Pattern.compile(
            "\"([^\"]+)\"\\s*=\\s*(?:(-?[0-9]+(?:\\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)|'([^']*)')");

    @Inject
    public ClpUberPinotSplitFilterProvider(ClpConfig config)
    {
        super(config);
    }

    /**
     * Transforms SQL predicates into Uber Pinot-compatible TEXT_MATCH expressions.
     * <p>
     * First applies inherited range mappings from the parent class, then transforms
     * remaining equality predicates to TEXT_MATCH format for Uber's merged text indices.
     * </p>
     *
     * @param scope the filter's scope (catalog.schema.table)
     * @param pushDownExpression the SQL expression to be transformed
     * @return the transformed Uber Pinot-compatible expression
     */
    @Override
    public String remapSplitFilterPushDownExpression(String scope, String pushDownExpression)
    {
        // First, apply inherited range mappings from parent class
        String remappedSql = super.remapSplitFilterPushDownExpression(scope, pushDownExpression);

        // Then, apply Uber-specific TEXT_MATCH transformations
        // Range-mapped columns won't match our pattern since they've already been transformed
        return transformToTextMatch(remappedSql);
    }

    /**
     * Transforms equality predicates to Pinot TEXT_MATCH expressions for Uber's infrastructure.
     * <p>
     * Converts {@code "columnName" = value} to {@code TEXT_MATCH("__mergedTextIndex", '/value:columnName/')}
     * This transformation enables efficient querying against Uber's merged text indices.
     * </p>
     *
     * @param expression the SQL expression to transform
     * @return the expression with equality predicates transformed to TEXT_MATCH
     */
    private String transformToTextMatch(String expression)
    {
        StringBuilder result = new StringBuilder();
        Matcher matcher = EQUALITY_PATTERN.matcher(expression);
        int lastEnd = 0;

        while (matcher.find()) {
            String columnName = matcher.group(1);
            // Group 2 contains numeric value, Group 3 contains string value
            String numericValue = matcher.group(2);
            String stringValue = matcher.group(3);
            String value = (numericValue != null) ? numericValue : stringValue;

            // Append text before the match
            result.append(expression, lastEnd, matcher.start());

            // Transform to TEXT_MATCH pattern: TEXT_MATCH("__mergedTextIndex", '/value:columnName/')
            String textMatchExpr = format(
                    "TEXT_MATCH(\"%s\", '/%s:%s/')",
                    MERGED_TEXT_INDEX_COLUMN,
                    value,
                    columnName);
            result.append(textMatchExpr);

            lastEnd = matcher.end();
        }

        // Append remaining text after last match
        result.append(expression, lastEnd, expression.length());

        return result.toString();
    }
}
