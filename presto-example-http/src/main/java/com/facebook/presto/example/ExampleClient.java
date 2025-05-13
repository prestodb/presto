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
package com.facebook.presto.example;

import com.facebook.airlift.json.JsonCodec;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class ExampleClient
{
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private final Supplier<Map<String, Map<String, ExampleTable>>> schemas;

    @Inject
    public ExampleClient(ExampleConfig config, JsonCodec<Map<String, List<ExampleTable>>> catalogCodec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");

        schemas = Suppliers.memoize(schemasSupplier(catalogCodec, config.getMetadata()));
    }

    public Set<String> getSchemaNames()
    {
        return schemas.get().keySet();
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        Map<String, ExampleTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    public ExampleTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, ExampleTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return null;
        }
        return tables.get(tableName);
    }

    private static Supplier<Map<String, Map<String, ExampleTable>>> schemasSupplier(final JsonCodec<Map<String, List<ExampleTable>>> catalogCodec, final URI metadataUri)
    {
        return () -> {
            try {
                return lookupSchemas(metadataUri, catalogCodec);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    private static Map<String, Map<String, ExampleTable>> lookupSchemas(URI metadataUri, JsonCodec<Map<String, List<ExampleTable>>> catalogCodec)
            throws IOException
    {
        URL result = metadataUri.toURL();
        String json = Resources.toString(result, UTF_8);
        //Map<String, List<ExampleTable>>  String是schema名字，List是schema下面的表
        Map<String, List<ExampleTable>> catalog = catalogCodec.fromJson(json);

        return ImmutableMap.copyOf(transformValues(catalog, resolveAndIndexTables(metadataUri)));
        //将Map<String, List<ExampleTable>>中的每个List<ExampleTable>转化成Map<String, ExampleTable>
    }

    //Function<List<ExampleTable>, Map<String, ExampleTable>>
    //Function<F,T>, F input T output,将List<ExampleTable>转换为Map<String, ExampleTable>，这个不是函数式接口喔，也可以将lambda表达式传进来
    private static Function<List<ExampleTable>, Map<String, ExampleTable>> resolveAndIndexTables(final URI metadataUri)
    {
        //lambda表达式的输入为tables，也就是接口相应方法的输入，这个方法限定输入为F，F为List<ExampleTable>
        return tables -> {
            //需要转换成Iterable<ExampleTable>
            Iterable<ExampleTable> resolvedTables = transform(tables, tableUriResolver(metadataUri));
            //将Iterable<ExampleTable>转换为Map<String, ExampleTable>，String为ExampleTable::getName)，ExampleTable为
            //resolvedTables的元素，相当于stream().xx()
            return ImmutableMap.copyOf(uniqueIndex(resolvedTables, ExampleTable::getName));
        };
    }

    private static Function<ExampleTable, ExampleTable> tableUriResolver(final URI baseUri)
    {
        //这一步是将获取到的metadata解析，把baseuri的信息(etc/catalog下面的properties文件写的)传进ExampleTale里面，
        // 对uri进行拼接成绝对路径再传，假如这个字符串不够完整就需要将其进行拼成绝对路径，比如元数据文件中source只有/table1，则拼成http://xxx/table1
        return table -> {
            List<URI> sources = ImmutableList.copyOf(transform(table.getSources(), baseUri::resolve));
            return new ExampleTable(table.getName(), table.getColumns(), sources);
        };
    }
}
