package com.facebook.presto.event.scribe.nectar;

import com.facebook.presto.event.Event;
import com.facebook.presto.event.scribe.client.AsyncScribeLogger;
import com.facebook.presto.event.scribe.client.LogEntry;
import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import io.airlift.event.client.EventClient;
import io.airlift.event.client.EventSubmissionFailedException;
import io.airlift.event.client.JsonEventSerializer;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.util.TokenBuffer;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class ScribeNectarEventClient
        implements EventClient
{
    private final static Logger log = Logger.get(ScribeNectarEventClient.class);

    private final AsyncScribeLogger asyncScribeLogger;
    private final Map<String, String> eventToCategoryMap;
    private final ObjectMapper objectMapper;
    private final JsonEventSerializer eventSerializer;
    private final JsonCodec<Map<String, Object>> jsonCodec;

    public ScribeNectarEventClient(AsyncScribeLogger asyncScribeLogger, ObjectMapper objectMapper, JsonEventSerializer eventSerializer, JsonCodec<Map<String, Object>> jsonCodec, Map<String, String> eventToCategoryMap)
    {
        this.asyncScribeLogger = checkNotNull(asyncScribeLogger, "asyncScribeLogger is null");
        this.eventToCategoryMap = ImmutableMap.copyOf(checkNotNull(eventToCategoryMap, "eventToCategoryMap is null"));
        this.objectMapper = checkNotNull(objectMapper, "objectMapper is null");
        this.eventSerializer = checkNotNull(eventSerializer, "eventSerializer is null");
        this.jsonCodec = checkNotNull(jsonCodec, "jsonCodec is null");
    }

    @Inject
    public ScribeNectarEventClient(AsyncScribeLogger asyncScribeLogger, ObjectMapper objectMapper, JsonEventSerializer eventSerializer, JsonCodec<Map<String, Object>> jsonCodec, NectarEventMappingConfiguration config)
    {
        this(asyncScribeLogger, objectMapper, eventSerializer, jsonCodec, checkNotNull(config, "config is null").getEventCategoryMap());
    }

    @Override
    public <T> CheckedFuture<Void, RuntimeException> post(T... event)
            throws IllegalArgumentException
    {
        checkNotNull(event, "event is null");
        return post(Arrays.asList(event));
    }

    @Override
    public <T> CheckedFuture<Void, RuntimeException> post(final Iterable<T> events)
            throws IllegalArgumentException
    {
        checkNotNull(events, "events is null");
        return post(new EventGenerator<T>()
        {
            @Override
            public void generate(EventPoster<T> eventPoster)
                    throws IOException
            {
                for (T event : events) {
                    eventPoster.post(event);
                }
            }
        });
    }

    @Override
    public <T> CheckedFuture<Void, RuntimeException> post(EventGenerator<T> eventGenerator)
            throws IllegalArgumentException
    {
        checkNotNull(eventGenerator, "eventGenerator is null");
        try {

            eventGenerator.generate(new EventPoster<T>()
            {
                @Override
                public void post(T event)
                        throws IOException
                {
                    checkNotNull(event, "event is null");
                    Event normalizedEvent = normalizeToEvent(event);
                    String scribeCategory = eventToCategoryMap.get(normalizedEvent.getType());
                    if (scribeCategory == null) {
                        log.debug("No Scribe category configured for event '%s'. Skipping event.", normalizedEvent.getType());
                    }
                    else {
                        NectarPayloadBuilder nectarPayloadBuilder = new NectarPayloadBuilder(jsonCodec)
                                .setAppEventType(normalizedEvent.getType())
                                .addAppData("event_type", normalizedEvent.getType())
                                .addAppData("event_host", normalizedEvent.getHost())
                                .addAppData("event_uuid", normalizedEvent.getUuid())
                                .addAppData("event_time", normalizedEvent.getTimestamp());
                        for (Map.Entry<String, ?> entry : normalizedEvent.getData().entrySet()) {
                            // Convert lower camel keys to lower underscore keys for Nectar compatibility
                            String key = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, entry.getKey());
                            nectarPayloadBuilder.addAppData(key, entry.getValue());
                        }
                        String payload = nectarPayloadBuilder.build();
                        asyncScribeLogger.log(new LogEntry(scribeCategory, payload));
                    }
                }
            });
        }
        catch (IOException e) {
            return Futures.<Void, RuntimeException>immediateFailedCheckedFuture(failedException(e));
        }
        return Futures.immediateCheckedFuture(null);
    }

    /**
     * Normalize annotated event into an actual Event class
     */
    private <T> Event normalizeToEvent(T event)
            throws IOException
    {
        TokenBuffer tokenBuffer = new TokenBuffer(objectMapper);
        eventSerializer.serialize(event, tokenBuffer);
        return objectMapper.readValue(tokenBuffer.asParser(), Event.class);
    }

    private static <T extends Throwable> EventSubmissionFailedException failedException(T e)
    {
        return new EventSubmissionFailedException("scribe", "general", ImmutableMap.of(URI.create("scribe:/"), e));
    }
}
