package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;

/**
 * Created by sprinklr on 03/07/15.
 */
public class ElasticsearchColumnMetadata extends ColumnMetadata
{
    private final String jsonPath;
    private final String jsonType;

    public ElasticsearchColumnMetadata(String name, Type type, String jsonPath, String jsonType, boolean partitionKey) {
        super(name, type, partitionKey, null, false);
        this.jsonPath = jsonPath;
        this.jsonType = jsonType;
    }

    public ElasticsearchColumnMetadata(ElasticsearchColumn column)
    {
        this(column.getName(), column.getType(), column.getJsonPath(), column.getJsonType(), false);
    }

    public String getJsonPath() {
        return jsonPath;
    }

    public String getJsonType() {
        return jsonType;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ElasticsearchColumnMetadata that = (ElasticsearchColumnMetadata) o;

        if (!jsonPath.equals(that.jsonPath)) return false;
        return jsonType.equals(that.jsonType);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + jsonPath.hashCode();
        result = 31 * result + jsonType.hashCode();
        return result;
    }
}
