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

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nullable;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Term;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.apache.iceberg.expressions.Expressions.bucket;
import static org.apache.iceberg.expressions.Expressions.day;
import static org.apache.iceberg.expressions.Expressions.hour;
import static org.apache.iceberg.expressions.Expressions.month;
import static org.apache.iceberg.expressions.Expressions.ref;
import static org.apache.iceberg.expressions.Expressions.truncate;
import static org.apache.iceberg.expressions.Expressions.year;

public final class PartitionFields
{
    // Column identifier: unquoted (any case) or double-quoted (e.g. "MyCol")
    private static final String UNQUOTED_IDENTIFIER = "[a-zA-Z_][a-zA-Z0-9_]*";
    private static final String QUOTED_IDENTIFIER = "\"(?:\"\"|[^\"])*\"";
    // IDENTIFIER is a single capture group matching either form; used by both partition and sort parsing
    public static final String IDENTIFIER = "(" + UNQUOTED_IDENTIFIER + "|" + QUOTED_IDENTIFIER + ")";
    private static final Pattern UNQUOTED_IDENTIFIER_PATTERN = Pattern.compile(UNQUOTED_IDENTIFIER);
    private static final Pattern QUOTED_IDENTIFIER_PATTERN = Pattern.compile(QUOTED_IDENTIFIER);

    // Transform function patterns: case-insensitive keyword + IDENTIFIER column argument
    private static final String FUNCTION_NAME = "\\(" + IDENTIFIER + "\\)";
    private static final String FUNCTION_NAME_INT = "\\(" + IDENTIFIER + ", *(\\d+)\\)";

    // Identity: bare or double-quoted column name; transforms: case-insensitive keyword + column
    private static final Pattern IDENTITY_PATTERN = Pattern.compile(IDENTIFIER);
    private static final Pattern YEAR_PATTERN = Pattern.compile("(?i:year)" + FUNCTION_NAME);
    private static final Pattern MONTH_PATTERN = Pattern.compile("(?i:month)" + FUNCTION_NAME);
    private static final Pattern DAY_PATTERN = Pattern.compile("(?i:day)" + FUNCTION_NAME);
    private static final Pattern HOUR_PATTERN = Pattern.compile("(?i:hour)" + FUNCTION_NAME);
    private static final Pattern BUCKET_PATTERN = Pattern.compile("(?i:bucket)" + FUNCTION_NAME_INT);
    private static final Pattern TRUNCATE_PATTERN = Pattern.compile("(?i:truncate)" + FUNCTION_NAME_INT);

    private static final Pattern COLUMN_BUCKET_PATTERN = Pattern.compile("bucket\\((\\d+)\\)");
    private static final Pattern COLUMN_TRUNCATE_PATTERN = Pattern.compile("truncate\\((\\d+)\\)");
    private static final Pattern ICEBERG_BUCKET_PATTERN = Pattern.compile("bucket\\[(\\d+)]");
    private static final Pattern ICEBERG_TRUNCATE_PATTERN = Pattern.compile("truncate\\[(\\d+)]");

    private PartitionFields() {}

    public static PartitionSpec parsePartitionFields(Schema schema, List<String> fields)
    {
        return parsePartitionFields(schema, fields, null, s -> s.toLowerCase(ENGLISH));
    }

    public static PartitionSpec parsePartitionFields(Schema schema, List<String> fields, UnaryOperator<String> identifierNormalizer)
    {
        return parsePartitionFields(schema, fields, null, identifierNormalizer);
    }

    public static PartitionSpec parsePartitionFields(Schema schema, List<String> fields, @Nullable Integer specId)
    {
        return parsePartitionFields(schema, fields, specId, s -> s.toLowerCase(ENGLISH));
    }

    public static PartitionSpec parsePartitionFields(Schema schema, List<String> fields, @Nullable Integer specId, UnaryOperator<String> identifierNormalizer)
    {
        PartitionSpec.Builder builder = Optional.ofNullable(specId)
                .map(id -> PartitionSpec.builderFor(schema).withSpecId(id))
                .orElseGet(() -> PartitionSpec.builderFor(schema));

        for (String field : fields) {
            buildPartitionField(builder, field, identifierNormalizer);
        }
        return builder.build();
    }

    public static PartitionSpec parseIcebergPartitionFields(Schema schema, List<IcebergPartitionField> fields, @Nullable Integer specId)
    {
        PartitionSpec.Builder builder = Optional.ofNullable(specId)
                .map(id -> PartitionSpec.builderFor(schema).withSpecId(id))
                .orElseGet(() -> PartitionSpec.builderFor(schema));

        for (IcebergPartitionField field : fields) {
            buildPartitionSpec(builder, field);
        }
        return builder.build();
    }

    @VisibleForTesting
    static void buildPartitionField(PartitionSpec.Builder builder, String field)
    {
        buildPartitionField(builder, field, s -> s.toLowerCase(ENGLISH));
    }

    @VisibleForTesting
    static void buildPartitionField(PartitionSpec.Builder builder, String field, UnaryOperator<String> identifierNormalizer)
    {
        // group(1) is the column token which may be a double-quoted identifier like "MyCol";
        // fromIdentifierToColumn strips the quotes and applies identifierNormalizer to the bare name.
        @SuppressWarnings("PointlessBooleanExpression")
        boolean matched = false ||
                tryMatch(field, IDENTITY_PATTERN, match -> builder.identity(fromIdentifierToColumn(match.group(1), identifierNormalizer))) ||
                tryMatch(field, YEAR_PATTERN, match -> builder.year(fromIdentifierToColumn(match.group(1), identifierNormalizer))) ||
                tryMatch(field, MONTH_PATTERN, match -> builder.month(fromIdentifierToColumn(match.group(1), identifierNormalizer))) ||
                tryMatch(field, DAY_PATTERN, match -> builder.day(fromIdentifierToColumn(match.group(1), identifierNormalizer))) ||
                tryMatch(field, HOUR_PATTERN, match -> builder.hour(fromIdentifierToColumn(match.group(1), identifierNormalizer))) ||
                tryMatch(field, BUCKET_PATTERN, match -> builder.bucket(fromIdentifierToColumn(match.group(1), identifierNormalizer), parseInt(match.group(2)))) ||
                tryMatch(field, TRUNCATE_PATTERN, match -> builder.truncate(fromIdentifierToColumn(match.group(1), identifierNormalizer), parseInt(match.group(2))));
        if (!matched) {
            throw new IllegalArgumentException("Invalid partition field declaration: " + field);
        }
    }

    private static void buildPartitionSpec(PartitionSpec.Builder builder, IcebergPartitionField partitionField)
    {
        String field = partitionField.getName();
        PartitionTransformType type = partitionField.getTransform();
        OptionalInt parameter = partitionField.getParameter();
        switch (type) {
            case IDENTITY:
                builder.identity(field);
                break;
            case YEAR:
                builder.year(field);
                break;
            case MONTH:
                builder.month(field);
                break;
            case DAY:
                builder.day(field);
                break;
            case HOUR:
                builder.hour(field);
                break;
            case BUCKET:
                builder.bucket(field, parameter.getAsInt());
                break;
            case TRUNCATE:
                builder.truncate(field, parameter.getAsInt());
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

    public static List<IcebergPartitionField> toIcebergPartitionFields(PartitionSpec spec)
    {
        return spec.fields().stream()
                .map(field -> toIcebergPartitionField(spec, field))
                .collect(toImmutableList());
    }

    // Keep consistency with PartitionSpec.Builder
    protected static String getPartitionColumnName(String columnName, String transform)
    {
        switch (transform) {
            case "identity":
                return columnName;
            case "year":
            case "month":
            case "day":
            case "hour":
                return columnName + "_" + transform;
        }

        Matcher matcher = COLUMN_BUCKET_PATTERN.matcher(transform);
        if (matcher.matches()) {
            return columnName + "_bucket";
        }

        matcher = ICEBERG_BUCKET_PATTERN.matcher(transform);
        if (matcher.matches()) {
            return columnName + "_bucket";
        }

        matcher = COLUMN_TRUNCATE_PATTERN.matcher(transform);
        if (matcher.matches()) {
            return columnName + "_trunc";
        }

        matcher = ICEBERG_TRUNCATE_PATTERN.matcher(transform);
        if (matcher.matches()) {
            return columnName + "_trunc";
        }

        throw new UnsupportedOperationException("Unknown partition transform: " + transform);
    }

    protected static Term getTransformTerm(String columnName, String transform)
    {
        switch (transform) {
            case "identity":
                return ref(columnName);
            case "year":
                return year(columnName);
            case "month":
                return month(columnName);
            case "day":
                return day(columnName);
            case "hour":
                return hour(columnName);
        }

        Matcher matcher = COLUMN_BUCKET_PATTERN.matcher(transform);
        if (matcher.matches()) {
            return bucket(columnName, Integer.valueOf(matcher.group(1)));
        }

        matcher = COLUMN_TRUNCATE_PATTERN.matcher(transform);
        if (matcher.matches()) {
            return truncate(columnName, Integer.valueOf(matcher.group(1)));
        }

        throw new UnsupportedOperationException("Unknown partition transform: " + transform);
    }

    private static IcebergPartitionField toIcebergPartitionField(PartitionSpec spec, PartitionField field)
    {
        String name = spec.schema().findColumnName(field.sourceId());
        String transform = field.transform().toString();
        IcebergPartitionField.Builder builder = IcebergPartitionField.builder();
        builder.setTransform(PartitionTransformType.fromStringOrFail(transform)).setFieldId(field.fieldId()).setSourceId(field.sourceId()).setName(name);
        Matcher matcher = ICEBERG_BUCKET_PATTERN.matcher(transform);
        if (matcher.matches()) {
            builder.setParameter(OptionalInt.of(Integer.parseInt(matcher.group(1))));
            return builder.build();
        }
        matcher = ICEBERG_TRUNCATE_PATTERN.matcher(transform);
        if (matcher.matches()) {
            builder.setParameter(OptionalInt.of(Integer.parseInt(matcher.group(1))));
            return builder.build();
        }
        return builder.build();
    }

    public static String quotedName(String name)
    {
        if (UNQUOTED_IDENTIFIER_PATTERN.matcher(name).matches()) {
            return name;
        }
        return '"' + name.replace("\"", "\"\"") + '"';
    }

    public static String fromIdentifierToColumn(String identifier)
    {
        return fromIdentifierToColumn(identifier, s -> s.toLowerCase(ENGLISH));
    }

    public static String fromIdentifierToColumn(String identifier, UnaryOperator<String> identifierNormalizer)
    {
        if (QUOTED_IDENTIFIER_PATTERN.matcher(identifier).matches()) {
            return identifierNormalizer.apply(identifier.substring(1, identifier.length() - 1).replace("\"\"", "\""));
        }
        return identifierNormalizer.apply(identifier);
    }
}
