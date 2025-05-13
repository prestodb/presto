package com.facebook.presto.example.prac;

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.List;

/**
 * @Author LTR
 * @Date 2025/4/16 15:27
 * @注释
 */
public class ExamplePracTable {
    private final String name;

    private final List<ExamplePracColumn> cols;

    private final List<ColumnMetadata> colMetadata;

    private final List<URI> sources;

    @JsonCreator
    public ExamplePracTable(
            @JsonProperty("name") String name,
            @JsonProperty("columns") List<ExamplePracColumn> cols,
            @JsonProperty("sources") List<URI> sources) {
        this.name = name;
        this.cols = cols;
        this.sources = sources;
        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
        for(ExamplePracColumn col : cols){
            builder.add(new ColumnMetadata(col.getName(),col.getType()));
        }
        this.colMetadata = builder.build();
    }

    public String getName() {
        return name;
    }

    public List<ExamplePracColumn> getCols() {
        return cols;
    }

    public List<ColumnMetadata> getColMetadata() {
        return colMetadata;
    }

    public List<URI> getSources() {
        return sources;
    }
}
