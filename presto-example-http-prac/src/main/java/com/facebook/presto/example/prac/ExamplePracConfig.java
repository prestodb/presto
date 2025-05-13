package com.facebook.presto.example.prac;

import com.facebook.airlift.configuration.Config;

import java.net.URI;

/**
 * @Author LTR
 * @Date 2025/4/15 16:44
 * @注释
 */
public class ExamplePracConfig {
    private URI metadata;
    @Config("metadata-uri")
    public void setMetadata(URI metadata) {
        this.metadata = metadata;
    }
    public URI getMetadata(){
        return metadata;
    }
}
