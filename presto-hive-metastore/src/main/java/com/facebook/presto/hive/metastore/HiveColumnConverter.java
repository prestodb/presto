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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.hive.ColumnConverter;
import com.facebook.presto.hive.HiveType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Strings.emptyToNull;

public class HiveColumnConverter
        implements ColumnConverter
{
    public HiveColumnConverter() {}

    // Matches struct field-name tokens (before ':') in an HMS type string.
    // Used to sanitize names with special chars (e.g. hyphens) that Hive's
    // TypeInfoParser cannot handle.
    private static final Pattern STRUCT_FIELD_NAME_PATTERN =
            Pattern.compile("(?<=struct<|(?<=,))([^<>,:]+)(?=:)");

    // Replaces non-[a-zA-Z0-9_] characters in struct field-name positions with '_'
    // so that HiveType.valueOf() does not crash on HMS type strings written by external
    // engines (e.g. Spark) with hyphenated field names. Safe because the HMS type string
    // is not used for query execution — actual field names come from Iceberg metadata JSON.
    static String sanitizeHmsTypeString(String typeString)
    {
        StringBuffer result = new StringBuffer();
        Matcher matcher = STRUCT_FIELD_NAME_PATTERN.matcher(typeString);
        while (matcher.find()) {
            matcher.appendReplacement(result,
                    Matcher.quoteReplacement(matcher.group(1).replaceAll("[^a-zA-Z0-9_]", "_")));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    @Override
    public Column toColumn(FieldSchema fieldSchema)
    {
        return new Column(fieldSchema.getName(), HiveType.valueOf(sanitizeHmsTypeString(fieldSchema.getType())), Optional.ofNullable(emptyToNull(fieldSchema.getComment())), Optional.empty());
    }

    @Override
    public FieldSchema fromColumn(Column column)
    {
        return new FieldSchema(column.getName(), column.getType().getHiveTypeName().toString(), column.getComment().orElse(null));
    }

    @Override
    public TypeSignature getTypeSignature(HiveType hiveType, Optional<String> typeMetadata)
    {
        return hiveType.getTypeSignature();
    }

    @Override
    public Optional<String> getTypeMetadata(HiveType hiveType, TypeSignature typeSignature)
    {
        return Optional.empty();
    }
}
