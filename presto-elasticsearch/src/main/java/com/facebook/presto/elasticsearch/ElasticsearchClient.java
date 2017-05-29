
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.json.JSONException;
import org.json.JSONObject;

import javax.inject.Inject;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static java.nio.charset.StandardCharsets.UTF_8;



public class ElasticsearchClient
{
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private Supplier<Map<String, Map<String, ElasticsearchTable>>> schemas;
    private ElasticsearchConfig config;
    private JsonCodec<Map<String, List<ElasticsearchTable>>> catalogCodec;

    @Inject
    public ElasticsearchClient(ElasticsearchConfig config, JsonCodec<Map<String, List<ElasticsearchTable>>> catalogCodec)
            throws IOException
    {
        checkNotNull(config, "config is null");
        checkNotNull(catalogCodec, "catalogCodec is null");

        this.config = config;
        this.catalogCodec = catalogCodec;

        schemas = Suppliers.memoize(schemasSupplier(catalogCodec, config.getMetadata()));
    }

    private ElasticsearchColumn makeColumn(String fieldPath_Type) throws JSONException, IOException {
        String[] items = fieldPath_Type.split(":");

        if(items.length != 2) {
            /*
            System.out.println("The items are :");
            for (String it : items)
            {
                System.out.println(it);
            }
            */
            return null;
            //assert (items.length == 2);
        }

        String type = items[1];
        String path = items[0];
        Type prestoType = VARCHAR;   // default. It will be corrected below

        // take only properties from dimensions and measurements for now
        if(!(path.startsWith("measurements"))) return null;


        if(path.endsWith(".type"))
        {
            path = path.substring(0, path.lastIndexOf('.'));

            // replace '.properties.' with '.'
            path = path.replaceAll("\\.properties\\.", ".");
        }

        if(type.equals("double") || type.equals("float") || type.equals("integer") || type.equals("string"))
        {
            if(type.equals("double")) prestoType = DOUBLE;
            else if(type.equals("float")) prestoType = DOUBLE;
            else if(type.equals("integer")) prestoType = BIGINT;
            else if(type.equals("long")) prestoType = BIGINT;
            else if(type.equals("string")) prestoType = VARCHAR;
        }
        else return null;

        ElasticsearchColumn column = new ElasticsearchColumn(path.replaceAll("\\.","_"), prestoType, path, type);
        return column;
    }

    private void getColumns(ElasticsearchTableSource src, Set<ElasticsearchColumn> columns) throws ExecutionException, InterruptedException, IOException, JSONException {

        /*
        Get the current set of columns for one of the sources of a table
         */
        String hostaddress = src.getHostaddress();
        int port = src.getPort();
        String clusterName = src.getClusterName();
        String index = src.getIndex();
        String type = src.getType();

        System.out.println("connecting ....");
        System.out.println("hostaddress :" + hostaddress);
        System.out.println("port :" + port);
        System.out.println("clusterName :" + clusterName);
        System.out.println("index :" + index);
        System.out.println("type :" + type);



        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", clusterName)
                .build();

        Client client = new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(
                        hostaddress, port));

        GetMappingsResponse res = client.admin().indices().getMappings(new GetMappingsRequest().indices(index).types(type)).get();


        ImmutableOpenMap<String, MappingMetaData> mapping  = res.mappings().get(index);
        for (ObjectObjectCursor<String, MappingMetaData> c : mapping) {
            //System.out.println(c.key+" = "+c.value.source());
            String data = c.value.source().toString();
            JSONObject json = new JSONObject(data);
            json = json.getJSONObject(type).getJSONObject("properties");
            //System.out.println(json.toString(2));

            List<String> leaves = (new MyJSONTest()).getListJson(json);
            for (String fieldPath_Type : leaves)
            {
                ElasticsearchColumn clm = makeColumn(fieldPath_Type);
                if (!(clm == null)) {
                    columns.add(clm);
                }
            }
            //System.out.println("--------------------");
        }

        client.close();
    }

    private List<ElasticsearchColumnMetadata> getColumnsMetadata(List<ElasticsearchColumn> columns)
    {
        List<ElasticsearchColumnMetadata> columnsMetadata = new ArrayList<>();
        for (ElasticsearchColumn clm : columns)
        {
            columnsMetadata.add(new ElasticsearchColumnMetadata(clm));
        }
        return columnsMetadata;
    }


    private void updateTableColumns_ColumnsMetadata(ElasticsearchTable table)
    {
        Set<ElasticsearchColumn> columns = new HashSet<ElasticsearchColumn>();
        for(ElasticsearchTableSource src : table.getSources())
        {
            try {
                getColumns(src,columns);
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (JSONException e) {
                System.out.println("JSONException caught !!!");
                e.printStackTrace();
                System.out.println("JSONException caught !!!");
            }
        }

        List<ElasticsearchColumn> columnsList = new ArrayList<ElasticsearchColumn>(columns);
        table.setColumns(columnsList);
        table.setColumnsMetadata(getColumnsMetadata(columnsList));
    }

    private void updateSchemas() throws IOException
    {
        // load from the metadata json file
        schemas = Suppliers.memoize(schemasSupplier(catalogCodec, config.getMetadata()));

        Map<String, Map<String, ElasticsearchTable>> schemasMap = schemas.get();
        for (Map.Entry<String, Map<String, ElasticsearchTable>> entry : schemasMap.entrySet()) {

            Map<String, ElasticsearchTable> tablesMap = entry.getValue();
            for (Map.Entry<String, ElasticsearchTable> tableEntry : tablesMap.entrySet()) {
                updateTableColumns_ColumnsMetadata(tableEntry.getValue());
            }
        }
        //schemas = Suppliers.ofInstance(schemasMap);
        schemas = Suppliers.memoize(Suppliers.ofInstance(schemasMap));
    }

    public Set<String> getSchemaNames() {

        //System.out.println("mark : getSchemaNames()");
        return schemas.get().keySet();
    }

    public Set<String> getTableNames(String schema)
    {
        checkNotNull(schema, "schema is null");
        Map<String, ElasticsearchTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    public ElasticsearchTable getTable(String schema, String tableName)
    {
        try {
            this.updateSchemas();
        } catch (IOException e) {
            e.printStackTrace();
        }

        checkNotNull(schema, "schema is null");
        checkNotNull(tableName, "tableName is null");
        Map<String, ElasticsearchTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return null;
        }
        return tables.get(tableName);
    }

    private static Supplier<Map<String, Map<String, ElasticsearchTable>>> schemasSupplier(final JsonCodec<Map<String, List<ElasticsearchTable>>> catalogCodec, final URI metadataUri)
    {

        return () -> {
            try {
                //System.out.println("mark : executing method schemasSupplier() :");
                return lookupSchemas(metadataUri, catalogCodec);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        };


    }

    private static Map<String, Map<String, ElasticsearchTable>> lookupSchemas(URI metadataUri, JsonCodec<Map<String, List<ElasticsearchTable>>> catalogCodec)
            throws IOException
    {
        // This function is called in the constructor of ElasticsearchClient
        //System.out.println("mark : in method lookupSchemas()");

        URL result = metadataUri.toURL();
        System.out.println("result : " + result);

        String json = Resources.toString(result, UTF_8);
        System.out.println("json : " + json);

        Map<String, List<ElasticsearchTable>> catalog = catalogCodec.fromJson(json);

        return ImmutableMap.copyOf(transformValues(catalog, resolveAndIndexTables(metadataUri)));
    }

    private static Function<List<ElasticsearchTable>, Map<String, ElasticsearchTable>> resolveAndIndexTables(final URI metadataUri)
    {
        return tables -> {
            Iterable<ElasticsearchTable> resolvedTables = transform(tables, tableUriResolver(metadataUri));
            return ImmutableMap.copyOf(uniqueIndex(resolvedTables, ElasticsearchTable::getName));
        };
    }

    private static Function<ElasticsearchTable, ElasticsearchTable> tableUriResolver(final URI baseUri)
    {
        return table -> {
            //List<URI> sources = ImmutableList.copyOf(transform(table.getSources(), baseUri::resolve));
            List<ElasticsearchTableSource> sources = table.getSources();
            return new ElasticsearchTable(table.getName(), table.getColumns(), sources);
        };
    }
}
