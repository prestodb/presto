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
package com.facebook.presto.iceberg;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

import java.util.List;
import java.util.function.Consumer;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;

public final class PartitionFields
{
    private static final String NAME = "[a-z_][a-z0-9_]*";
    private static final String FUNCTION_NAME = "\\((" + NAME + ")\\)";
    private static final String FUNCTION_NAME_INT = "\\((" + NAME + "), *(\\d+)\\)";

    private static final Pattern IDENTITY_PATTERN = Pattern.compile(NAME);
    private static final Pattern YEAR_PATTERN = Pattern.compile("year" + FUNCTION_NAME);
    private static final Pattern MONTH_PATTERN = Pattern.compile("month" + FUNCTION_NAME);
    private static final Pattern DAY_PATTERN = Pattern.compile("day" + FUNCTION_NAME);
    private static final Pattern HOUR_PATTERN = Pattern.compile("hour" + FUNCTION_NAME);
    private static final Pattern BUCKET_PATTERN = Pattern.compile("bucket" + FUNCTION_NAME_INT);
    private static final Pattern TRUNCATE_PATTERN = Pattern.compile("truncate" + FUNCTION_NAME_INT);

    private static final Pattern ICEBERG_BUCKET_PATTERN = Pattern.compile("bucket\\[(\\d+)]");
    private static final Pattern ICEBERG_TRUNCATE_PATTERN = Pattern.compile("truncate\\[(\\d+)]");

    private PartitionFields() {}

    public static PartitionSpec parsePartitionFields(Schema schema, List<String> fields)
    {
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        for (String field : fields) {
            parsePartitionField(builder, field);
        }
        return builder.build();
    }

    public static void parsePartitionField(PartitionSpec.Builder builder, String field)
    {
        @SuppressWarnings("PointlessBooleanExpression")
        boolean matched = false ||
                tryMatch(field, IDENTITY_PATTERN, match -> builder.identity(match.group())) ||
                tryMatch(field, YEAR_PATTERN, match -> builder.year(match.group(1))) ||
                tryMatch(field, MONTH_PATTERN, match -> builder.month(match.group(1))) ||
                tryMatch(field, DAY_PATTERN, match -> builder.day(match.group(1))) ||
                tryMatch(field, HOUR_PATTERN, match -> builder.hour(match.group(1))) ||
                tryMatch(field, BUCKET_PATTERN, match -> builder.bucket(match.group(1), parseInt(match.group(2)))) ||
                tryMatch(field, TRUNCATE_PATTERN, match -> builder.truncate(match.group(1), parseInt(match.group(2))));
        if (!matched) {
            throw new IllegalArgumentException("Invalid partition field declaration: " + field);
        }
    }

    private static boolean tryMatch(CharSequence value, Pattern pattern, Consumer<MatchResult> match)
    {
        Matcher matcher = pattern.matcher(value);
        if (matcher.matches()) {
            match.accept(matcher.toMatchResult());
            return true;
        }
        return false;
    }

    public static List<String> toPartitionFields(PartitionSpec spec)
    {
        return spec.fields().stream()
                .map(field -> toPartitionField(spec, field))
                .collect(toImmutableList());
    }

    private static String toPartitionField(PartitionSpec spec, PartitionField field)
    {
        String name = spec.schema().findColumnName(field.sourceId());
        String transform = field.transform().toString();

        switch (transform) {
            case "identity":
                return name;
            case "year":
            case "month":
            case "day":
            case "hour":
                return format("%s(%s)", transform, name);
        }

        Matcher matcher = ICEBERG_BUCKET_PATTERN.matcher(transform);
        if (matcher.matches()) {
            return format("bucket(%s, %s)", name, matcher.group(1));
        }

        matcher = ICEBERG_TRUNCATE_PATTERN.matcher(transform);
        if (matcher.matches()) {
            return format("truncate(%s, %s)", name, matcher.group(1));
        }

        throw new UnsupportedOperationException("Unsupported partition transform: " + field);
    }
}
