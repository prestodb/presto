
package com.facebook.presto.elasticsearch;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.net.URI;

public class ElasticsearchConfig
{
    private URI metadata;

    @NotNull
    public URI getMetadata()
    {
        return metadata;
    }

    @Config("metadata-uri")
    public ElasticsearchConfig setMetadata(URI metadata)
    {
        this.metadata = metadata;
        return this;
    }

}
