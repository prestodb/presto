package com.facebook.presto.example.prac;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Inject;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * @Author LTR
 * @Date 2025/4/16 14:04
 * @注释 负责从一个 JSON 文件中加载 schema 和表的元数据，并提供对这些数据的访问能力
 */
public class ExamplePracClient {

    private final Supplier<Map<String,Map<String,ExamplePracTable>>> schemas;
    private final ExamplePracConfig examplePracConfig;

    /**
     *
     * @param examplePracConfig
     */
    @Inject
    public ExamplePracClient(ExamplePracConfig examplePracConfig, JsonCodec<Map<String,List<ExamplePracTable>>> catalogCodeC){
        requireNonNull(examplePracConfig,"The config is null!");
        requireNonNull(catalogCodeC,"The catalog CodeC is null!");

        this.examplePracConfig = examplePracConfig;
        schemas = Suppliers.memoize(schemasSupplier(catalogCodeC, examplePracConfig.getMetadata()));

    }

    public Set<String> getSchemaNames(){
        //TODO:发送http请求
        return schemas.get().keySet();
    }

    public Set<String> getTableNames(String schemaName){
        return schemas.get().get(schemaName).keySet();
    }

    private static Supplier<Map<String,Map<String,ExamplePracTable>>> schemasSupplier(JsonCodec<Map<String,List<ExamplePracTable>>> catalogCodeC, URI metadatauri){
        return () ->{
            try{
                return lookupSchema(catalogCodeC,metadatauri);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static Map<String,Map<String,ExamplePracTable>> lookupSchema(JsonCodec<Map<String,List<ExamplePracTable>>> catalogCodeC, URI metadataUri) throws IOException {
        URL result = metadataUri.toURL();
        String json = Resources.toString(result,UTF_8);
        Map<String,List<ExamplePracTable>> catalog = catalogCodeC.fromJson(json);
        return ImmutableMap.copyOf(com.google.common.collect.Maps.transformValues(catalog, resolveAndIndexTables(metadataUri)));
    }

    private static Function<List<ExamplePracTable>,Map<String,ExamplePracTable>> resolveAndIndexTables(URI metadatauri){
        return tables -> {
            //1.iterator
            //2.uniqueIndex
            Iterable<ExamplePracTable> resolvedTables = transform(tables,resolveTableUri(metadatauri));
            return ImmutableMap.copyOf(uniqueIndex(resolvedTables,ExamplePracTable::getName));
        };
    }

    private static Function<ExamplePracTable,ExamplePracTable> resolveTableUri(URI metadatauri){
        return table -> {
            //1.get sources
            List<URI> sources = ImmutableList.copyOf(transform(table.getSources(),metadatauri::resolve));
            //2.concat
            return new ExamplePracTable(table.getName(),table.getCols(),sources);
        };
    }
}
