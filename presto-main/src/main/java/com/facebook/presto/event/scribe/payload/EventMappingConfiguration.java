package com.facebook.presto.event.scribe.payload;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class EventMappingConfiguration
{
    // Map of EventType to corresponding Scribe categories
    private Map<String, String> eventCategoryMap = ImmutableMap.of();

    @NotNull
    public Map<String, String> getEventCategoryMap()
    {
        return eventCategoryMap;
    }

    /**
     * This is a very ghetto technique to import a mapping from configuration.
     * Values are expected to look like the following:
     *
     *   event1:category1,event2:category2,...
     */
    @Config("scribe.payload.event-category-map")
    @ConfigDescription("Map format: event1:category1,event2:category2,...")
    public EventMappingConfiguration setEventCategoryMap(String formattedMap)
    {
        Iterable<String> entries = Splitter.on(",").omitEmptyStrings().split(formattedMap);
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (String entry : entries) {
            List<String> eventCategory = ImmutableList.copyOf(Splitter.on(":").omitEmptyStrings().split(entry));
            checkArgument(eventCategory.size() == 2, "Expected only a key and a value");
            builder.put(eventCategory.get(0), eventCategory.get(1));
        }
        eventCategoryMap = builder.build();
        return this;
    }
}
