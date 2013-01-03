package com.facebook.presto.event.scribe.payload;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Map;
import java.util.Properties;

public class EventCategoryMapProvider
{
    // Map of EventType to corresponding Scribe categories
    private final Map<String, String> eventCategoryMap;

    @Inject
    public EventCategoryMapProvider(EventMappingConfiguration eventMappingConfiguration)
    {
        eventCategoryMap = processConfig(eventMappingConfiguration);
    }

    private static Map<String, String> processConfig(EventMappingConfiguration configuration)
    {
        // Return an empty map if the configuration file is not specified
        if (configuration.getEventMappingFile() == null) {
            return ImmutableMap.of();
        }

        Properties properties = new Properties();
        try {
            Reader reader = new FileReader(configuration.getEventMappingFile());
            try {
                properties.load(reader);
            }
            finally {
                reader.close();
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            builder.put(entry.getKey().toString(), entry.getValue().toString());
        }
        return builder.build();
    }

    public Map<String, String> getEventCategoryMap()
    {
        return eventCategoryMap;
    }
}
