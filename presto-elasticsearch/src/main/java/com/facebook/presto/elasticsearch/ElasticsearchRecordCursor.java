
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import com.google.common.io.CountingInputStream;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.*;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ElasticsearchRecordCursor
        implements RecordCursor
{
    //private static final Splitter LINE_SPLITTER = Splitter.on(",").trimResults();

    private final List<ElasticsearchColumnHandle> columnHandles;
    //private final int[] fieldToColumnIndex;
    Map<String, Integer> jsonpathToIndex = new HashMap<String, Integer>();

    private final Iterator<SearchHit> lines;
    private long totalBytes;

    private List<String> fields;

    public ElasticsearchRecordCursor(List<ElasticsearchColumnHandle> columnHandles, ElasticsearchTableSource tableSource)
    {
        this.columnHandles = columnHandles;

        //fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            ElasticsearchColumnHandle columnHandle = columnHandles.get(i);
            //fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
            //jsonpathToIndex.put(columnHandle.getColumnJsonPath(), columnHandle.getOrdinalPosition());

            jsonpathToIndex.put(columnHandle.getColumnJsonPath(), i);
        }

        /*
        try (CountingInputStream input = new CountingInputStream(byteSource.openStream())) {
            lines = byteSource.asCharSource(UTF_8).readLines().iterator();
            totalBytes = input.getCount();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        */

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", tableSource.getClusterName())
                /*.put("client.transport.sniff", true)*/.build();

        Client client = new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(
                        tableSource.getHostaddress(), tableSource.getPort()));


        //String[] fields = new String[] {"user", "dim.age" , "measurements.FACEBOOK_PAGE_CONSUMPTIONS_UNIQUE"};
        ArrayList<String> fieldsNeeded = new ArrayList<String>();
        for (int i = 0; i < columnHandles.size(); i++) {
            ElasticsearchColumnHandle columnHandle = columnHandles.get(i);
            fieldsNeeded.add(columnHandle.getColumnJsonPath());
        }

        /*SearchResponse response = client.prepareSearch(tableSource.getIndex())
                .setTypes(tableSource.getType())
                        //.setQuery(QueryBuilders.termQuery("dimensions.SN_TYPE", "facebook"))
                .addFields(fieldsNeeded.toArray(new String[fieldsNeeded.size()]))
                .setFrom(0).setSize(1000000).setExplain(true)
                .execute()
                .actionGet();
        lines = Arrays.asList(response.getHits().getHits()).iterator();*/



        lines = getRows_faster(client, tableSource, fieldsNeeded).iterator();

        totalBytes = 0;

        client.close();
    }

    private List<SearchHit> getRows(Client client, ElasticsearchTableSource tableSource, ArrayList<String> fieldsNeeded)
    {
        SearchResponse response = client.prepareSearch(tableSource.getIndex())
                .setTypes(tableSource.getType())
                        //.setQuery(QueryBuilders.termQuery("dimensions.SN_TYPE", "facebook"))
                .addFields(fieldsNeeded.toArray(new String[fieldsNeeded.size()]))
                .setFrom(0).setSize(1000000).setExplain(true)
                .execute()
                .actionGet();
        return Arrays.asList(response.getHits().getHits());
    }

    private List<SearchHit> getRows_faster(Client client, ElasticsearchTableSource tableSource, ArrayList<String> fieldsNeeded)
    {
        List<SearchHit> rows = new ArrayList<>();
        SearchResponse scrollResp = client.prepareSearch(tableSource.getIndex())
                .setTypes(tableSource.getType())
                .addFields(fieldsNeeded.toArray(new String[fieldsNeeded.size()]))
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000))
                .setSize(20000).execute().actionGet(); //20000 hits per shard will be returned for each scroll
        //Scroll until no hits are returned
        while (true) {

            for (SearchHit hit : scrollResp.getHits().getHits()) {
                //Handle the hit...
                rows.add(hit);
            }
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
            //Break condition: No hits are returned
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }
        return rows;
    }

    @Override
    public long getTotalBytes()
    {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!lines.hasNext()) {
            return false;
        }
        SearchHit hit = lines.next();

        //fields = LINE_SPLITTER.splitToList(line);
        fields = new ArrayList<String>(Collections.nCopies(columnHandles.size(), "-1"));

        Map<String, SearchHitField> map =  hit.getFields();
        for (Map.Entry<String, SearchHitField> entry : map.entrySet()) {
            String jsonPath = entry.getKey().toString();
            SearchHitField fieldvar = entry.getValue();

            // we get the value , wrapped in a list (of size 1 ofcourse) -> [value] (The java api returns in this way)
            ArrayList<Object> lis = new ArrayList<Object>(fieldvar.getValues());
            // get the value
            String value = String.valueOf(lis.get(0));

            fields.set(jsonpathToIndex.get(jsonPath), value);


            //System.out.println("key, " + path + " value " + lis.get(0) );
        }

        totalBytes += fields.size();


        return true;
    }

    private String getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");

        //int columnIndex = fieldToColumnIndex[field];
        //return fields.get(columnIndex);
        return fields.get(field);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT);
        return Long.parseLong(getFieldValue(field));
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, VARCHAR);
        return Slices.utf8Slice(getFieldValue(field));
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
    }
}
