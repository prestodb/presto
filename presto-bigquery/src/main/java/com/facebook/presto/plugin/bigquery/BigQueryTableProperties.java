package com.facebook.presto.plugin.bigquery;/*
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

import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static java.util.Objects.requireNonNull;

/**
 * Class contains all table properties for the BigQuery connector. Used when creating a table
 */
public final class BigQueryTableProperties
{
    public static final String COLUMN_MAPPING = "column_mapping";
    public static final String INDEX_COLUMNS = "index_columns";
    public static final String EXTERNAL = "external";
    public static final String LOCALITY_GROUPS = "locality_groups";
    public static final String ROW_ID = "row_id";
    public static final String SCAN_AUTHS = "scan_auths";

    private final List<PropertyMetadata<?>> tableProperties;

    public BigQueryTableProperties()
    {
        PropertyMetadata<String> s1 = stringProperty(
                COLUMN_MAPPING,
                "Comma-delimited list of column metadata: col_name:col_family:col_qualifier,[...]. Required for external tables. Not setting this property results in auto-generated column names.",
                null,
                false);

        PropertyMetadata<String> s2 = stringProperty(
                INDEX_COLUMNS,
                "A comma-delimited list of Presto columns that are indexed in this table's corresponding index table. Default is no indexed columns.",
                "",
                false);

        PropertyMetadata<Boolean> s3 = booleanProperty(
                EXTERNAL,
                "If true, Presto will only do metadata operations for the table. Else, Presto will create and drop Accumulo tables where appropriate. Default false.",
                false,
                false);

        PropertyMetadata<String> s4 = stringProperty(
                LOCALITY_GROUPS,
                "List of locality groups to set on the Accumulo table. Only valid on internal tables. String format is locality group name, colon, comma delimited list of Presto column names in the group. Groups are delimited by pipes. Example: group1:colA,colB,colC|group2:colD,colE,colF|etc.... Default is no locality groups.",
                null,
                false);

        PropertyMetadata<String> s5 = stringProperty(
                ROW_ID,
                "Presto column name that maps to the Accumulo row ID. Default is the first column.",
                null,
                false);

        PropertyMetadata<String> s6 = stringProperty(
                SCAN_AUTHS,
                "Scan-time authorizations set on the batch scanner. Default is all scan authorizations for the user",
                null,
                false);

        tableProperties = ImmutableList.of(s1, s2, s3, s4, s5, s6);
    }

    public static Optional<List<String>> getIndexColumns(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        @SuppressWarnings("unchecked")
        String indexColumns = (String) tableProperties.get(INDEX_COLUMNS);
        if (indexColumns == null) {
            return Optional.empty();
        }

        return Optional.of(Arrays.asList(StringUtils.split(indexColumns, ',')));
    }
    public static Optional<String> getScanAuthorizations(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        @SuppressWarnings("unchecked")
        String scanAuths = (String) tableProperties.get(SCAN_AUTHS);
        return Optional.ofNullable(scanAuths);
    }

    public static Optional<String> getRowId(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        @SuppressWarnings("unchecked")
        String rowId = (String) tableProperties.get(ROW_ID);
        return Optional.ofNullable(rowId);
    }


    public static boolean isExternal(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        @SuppressWarnings("unchecked")
        Boolean serializerClass = (Boolean) tableProperties.get(EXTERNAL);
        return serializerClass;
    }
}
