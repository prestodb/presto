package com.facebook.presto.event.scribe.payload;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.io.File;

public class EventMappingConfiguration
{
    private File eventMappingFile;

    public File getEventMappingFile()
    {
        return eventMappingFile;
    }

    @Config("scribe.payload.event-mapping-file")
    @ConfigDescription("File path to a properties file describing the map between event names (key) and Scribe category names (value)")
    public EventMappingConfiguration setEventMappingFile(File eventMappingFile)
    {
        this.eventMappingFile = eventMappingFile;
        return this;
    }
}
