package com.facebook.presto.event.scribe.payload;

import com.facebook.presto.event.Event;
import com.facebook.presto.event.scribe.client.AsyncScribeLogger;
import com.facebook.presto.event.scribe.client.LogEntry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;
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

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class ScribeEventClient
        implements EventClient
{
    private final static Logger log = Logger.get(ScribeEventClient.class);

    private final AsyncScribeLogger asyncScribeLogger;
    private final Map<String, String> eventToCategoryMap;
    private final ObjectMapper objectMapper;
    private final JsonEventSerializer eventSerializer;
    private final JsonCodec<Map<String, Object>> jsonCodec;

    public ScribeEventClient(AsyncScribeLogger asyncScribeLogger, ObjectMapper objectMapper, JsonEventSerializer eventSerializer, JsonCodec<Map<String, Object>> jsonCodec, Map<String, String> eventToCategoryMap)
    {
        this.asyncScribeLogger = checkNotNull(asyncScribeLogger, "asyncScribeLogger is null");
        this.eventToCategoryMap = ImmutableMap.copyOf(checkNotNull(eventToCategoryMap, "eventToCategoryMap is null"));
        this.objectMapper = checkNotNull(objectMapper, "objectMapper is null");
        this.eventSerializer = checkNotNull(eventSerializer, "eventSerializer is null");
        this.jsonCodec = checkNotNull(jsonCodec, "jsonCodec is null");
    }

    @Inject
    public ScribeEventClient(AsyncScribeLogger asyncScribeLogger, ObjectMapper objectMapper, JsonEventSerializer eventSerializer, JsonCodec<Map<String, Object>> jsonCodec, EventCategoryMapProvider mapProvider)
    {
        this(asyncScribeLogger, objectMapper, eventSerializer, jsonCodec, checkNotNull(mapProvider, "mapProvider is null").getEventCategoryMap());
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
        // Skip logging if Scribe logging is disabled
        if (eventToCategoryMap.isEmpty()) {
            return Futures.immediateCheckedFuture(null);
        }

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
                        // See PayloadBuilder for message format
                        PayloadBuilder payloadBuilder = new PayloadBuilder(jsonCodec)
                                .setAppEventType(normalizedEvent.getType())
                                .addAppData("event_type", normalizedEvent.getType())
                                .addAppData("event_host", normalizedEvent.getHost())
                                .addAppData("event_uuid", normalizedEvent.getUuid())
                                .addAppData("event_time", normalizedEvent.getTimestamp());
                        for (Map.Entry<String, ?> entry : normalizedEvent.getData().entrySet()) {
                            String key = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, entry.getKey());
                            payloadBuilder.addAppData(key, entry.getValue());
                        }
                        String payload = payloadBuilder.build();
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
     * Normalize annotated event into an actual Event class.
     * The only reason this exists is because the Event metadata extraction is tightly coupled with JSON serialization
     * and does not lend itself well to just metadata extraction.
     *
     * TODO: Once the Event metadata introspection is decoupled from JSON, we should switch to using that directly, instead
     * of this workaround.
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
