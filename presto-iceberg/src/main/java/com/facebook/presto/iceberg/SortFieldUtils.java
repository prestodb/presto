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

import com.facebook.presto.spi.PrestoException;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderBuilder;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.iceberg.IcebergTableProperties.SORTED_BY_PROPERTY;
import static com.facebook.presto.iceberg.PartitionFields.fromIdentifierToColumn;
import static com.facebook.presto.iceberg.PartitionFields.quotedName;
import static com.facebook.presto.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;

public class SortFieldUtils
{
    private SortFieldUtils() {}

    private static final Pattern PATTERN = Pattern.compile(
            "\\s*(?<identifier>" + PartitionFields.IDENTIFIER + ")"
                    + "(?i:\\s+(?<ordering>ASC|DESC))?"
                    + "(?i:\\s+NULLS\\s+(?<nullOrder>FIRST|LAST))?"
                    + "\\s*");

    public static SortOrder parseSortFields(Schema schema, List<String> fields)
    {
        SortOrder.Builder builder = SortOrder.builderFor(schema);
        parseSortFields(builder, fields);
        SortOrder sortOrder;
        try {
            sortOrder = builder.build();
        }
        catch (RuntimeException e) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, "Invalid " + SORTED_BY_PROPERTY + " definition", e);
        }

        Set<Integer> baseColumnFieldIds = schema.columns().stream()
                .map(Types.NestedField::fieldId)
                .collect(toImmutableSet());
        for (SortField field : sortOrder.fields()) {
            if (!baseColumnFieldIds.contains(field.sourceId())) {
                throw new PrestoException(COLUMN_NOT_FOUND, "Column not found: " + schema.findColumnName(field.sourceId()));
            }
        }

        return sortOrder;
    }

    public static void parseSortFields(SortOrderBuilder<?> sortOrderBuilder, List<String> fields)
    {
        fields.forEach(field -> parseSortField(sortOrderBuilder, field));
    }

    private static void parseSortField(SortOrderBuilder<?> builder, String field)
    {
        Matcher matcher = PATTERN.matcher(field);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(format("Unable to parse sort field: [%s]", field));
        }

        String columnName = fromIdentifierToColumn(matcher.group("identifier"));
        boolean ascending;
        String ordering = firstNonNull(matcher.group("ordering"), "ASC").toUpperCase(Locale.ENGLISH);

        switch (ordering) {
            case "ASC":
                ascending = true;
                break;
            case "DESC":
                ascending = false;
                break;
            default:
                throw new IllegalStateException("Unexpected ordering value: " + ordering);
        }

        String nullOrderDefault = ascending ? "FIRST" : "LAST";

        NullOrder nullOrder;
        String nullOrderValue = firstNonNull(matcher.group("nullOrder"), nullOrderDefault).toUpperCase(Locale.ENGLISH);

        switch (nullOrderValue) {
            case "FIRST":
                nullOrder = NullOrder.NULLS_FIRST;
                break;
            case "LAST":
                nullOrder = NullOrder.NULLS_LAST;
                break;
            default:
                throw new IllegalStateException("Unexpected null ordering value: " + nullOrderValue);
        }

        if (ascending) {
            builder.asc(columnName, nullOrder);
        }
        else {
            builder.desc(columnName, nullOrder);
        }
    }

    public static List<String> toSortFields(SortOrder spec)
    {
        return spec.fields().stream()
                .map(field -> toSortField(spec, field))
                .collect(toImmutableList());
    }

    private static String toSortField(SortOrder spec, SortField field)
    {
        verify(field.transform().isIdentity(), "Iceberg sort transforms are not supported");

        String name = quotedName(spec.schema().findColumnName(field.sourceId()));
        return format("%s %s %s", name, field.direction(), field.nullOrder());
    }

    public static List<String> getNonIdentityColumns(SortOrder spec)
    {
        return spec.fields().stream()
                .filter(sortField -> !sortField.transform().isIdentity())
                .map(sortField -> {
                    String name = quotedName(spec.schema().findColumnName(sortField.sourceId()));
                    return format("%s %s %s", name, sortField.direction(), sortField.nullOrder());
                })
                .collect(toImmutableList());
    }
}
