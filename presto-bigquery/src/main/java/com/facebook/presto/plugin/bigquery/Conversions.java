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
package com.facebook.presto.plugin.bigquery;

import com.facebook.presto.spi.ColumnMetadata;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.cloud.bigquery.Field.Mode;
import static com.google.cloud.bigquery.Field.Mode.NULLABLE;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class Conversions
{
    private Conversions() {}

    static BigQueryColumnHandle toColumnHandle(Field field)
    {
        FieldList subFields = field.getSubFields();
        List<BigQueryColumnHandle> subColumns = subFields == null ?
                Collections.emptyList() :
                subFields.stream()
                        .map(Conversions::toColumnHandle)
                        .collect(Collectors.toList());
        return new BigQueryColumnHandle(
                field.getName(),
                BigQueryType.valueOf(field.getType().name()),
                getMode(field),
                subColumns,
                field.getDescription());
    }

    static ColumnMetadata toColumnMetadata(Field field)
    {
        return new ColumnMetadata(
                field.getName(), // name
                adapt(field).getPrestoType(),
                getMode(field) == NULLABLE, //nullable
                field.getDescription(), // comment
                null, // extraInfo
                false, // hidden
                ImmutableMap.of()); // properties
    }

    static BigQueryType.Adaptor adapt(Field field)
    {
        return new BigQueryType.Adaptor()
        {
            @Override
            public BigQueryType getBigQueryType()
            {
                return BigQueryType.valueOf(field.getType().name());
            }

            @Override
            public ImmutableMap<String, BigQueryType.Adaptor> getBigQuerySubTypes()
            {
                FieldList subFields = field.getSubFields();
                if (subFields == null) {
                    return ImmutableMap.of();
                }
                return subFields.stream().collect(toImmutableMap(Field::getName, Conversions::adapt));
            }

            @Override
            public Field.Mode getMode()
            {
                return Conversions.getMode(field);
            }
        };
    }

    private static Mode getMode(Field field)
    {
        return firstNonNull(field.getMode(), NULLABLE);
    }
}
