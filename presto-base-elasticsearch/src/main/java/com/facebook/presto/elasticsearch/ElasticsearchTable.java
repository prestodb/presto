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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.elasticsearch.metadata.EsField;
import com.facebook.presto.elasticsearch.metadata.EsIndex;
import com.facebook.presto.elasticsearch.model.ElasticsearchColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class ElasticsearchTable
{
    private final String schema;
    private final String table;
    private final List<ElasticsearchColumnHandle> columns;
    private final List<ColumnMetadata> columnsMetadata;

    public ElasticsearchTable(EsTypeManager typeManager, String schema, String table, final EsIndex esIndex)
    {
        requireNonNull(esIndex, "esIndex is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = table;

        //---------------------------------
        final BiFunction<String, Type, ElasticsearchColumnHandle> addHiddenFunc = (name, type) ->
                new ElasticsearchColumnHandle(name, type, "", false, true);
        ImmutableList.Builder<ElasticsearchColumnHandle> columnHandleBuilder = ImmutableList.builder();
        columnHandleBuilder.add(addHiddenFunc.apply("_dsl", VarcharType.VARCHAR));
        columnHandleBuilder.add(addHiddenFunc.apply("_type", VarcharType.VARCHAR));
        columnHandleBuilder.add(addHiddenFunc.apply("_id", VarcharType.VARCHAR));
        columnHandleBuilder.add(addHiddenFunc.apply("_score", DoubleType.DOUBLE));

        for (EsField esField : esIndex.mapping().values()) {
            Type type = typeManager.toPrestoType(esField);
            String comment = "";  //字段注释
            ElasticsearchColumnHandle columnHandle = new ElasticsearchColumnHandle(
                    esField.getName(),
                    type,
                    comment,
                    true,
                    false);
            columnHandleBuilder.add(columnHandle);
            //---- add _column
            if (VarcharType.VARCHAR.equals(type)) {
                columnHandleBuilder.add(new ElasticsearchColumnHandle(
                        "_" + esField.getName(),
                        type,
                        "_Expansion use match_query",
                        true,
                        true));
            }
        }
        this.columns = columnHandleBuilder.build();
        this.columnsMetadata = columns.stream().map(x -> x.getColumnMetadata()).collect(Collectors.toList());
    }

    public String getSchema()
    {
        return schema;
    }

    public String getTable()
    {
        return table;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return this.columnsMetadata;
    }

    public List<ElasticsearchColumnHandle> getColumns()
    {
        return columns;
    }
}
