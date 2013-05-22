package com.facebook.presto.operator;

import com.facebook.presto.client.PrestoHeaders;
import com.facebook.presto.serde.PagesSerde;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableListMultimap;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.units.DataSize;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.PrestoMediaTypes.PRESTO_PAGES;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_SEQUENCE_ID;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class MockExchangeRequestProcessor
        implements Function<Request, Response>
{
    private final ConcurrentMap<URI, BlockingQueue<Page>> pagesByLocation = new ConcurrentHashMap<>();
    private final ConcurrentMap<URI, Boolean> completeByLocation = new ConcurrentHashMap<>();
    private final DataSize expectedMaxSize;
    private final ConcurrentMap<URI, Long> sequenceIdByLocation = new ConcurrentHashMap<>();

    public MockExchangeRequestProcessor(DataSize expectedMaxSize)
    {
        this.expectedMaxSize = expectedMaxSize;
    }

    public void addPage(URI location, Page page)
    {
        checkState(completeByLocation.get(location) != Boolean.TRUE, "Location %s is complete", location);
        BlockingQueue<Page> queue = pagesByLocation.get(location);
        if (queue == null) {
            queue = new LinkedBlockingQueue<>();
            BlockingQueue<Page> existingValue = pagesByLocation.putIfAbsent(location, queue);
            queue = (existingValue != null ? existingValue : queue);
            sequenceIdByLocation.put(location, 0L);
        }
        queue.add(page);
    }

    public void setComplete(URI location)
    {
        completeByLocation.put(location, true);
    }

    @Override
    public Response apply(Request request)
    {
        if (request.getMethod().equalsIgnoreCase("DELETE")) {
            return new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.<String, String>of(), new byte[0]);
        }

        // verify we got a data size and it parses correctly
        assertTrue(!request.getHeaders().get(PrestoHeaders.PRESTO_MAX_SIZE).isEmpty());
        DataSize maxSize = DataSize.valueOf(request.getHeader(PrestoHeaders.PRESTO_MAX_SIZE));
        assertEquals(maxSize, expectedMaxSize);

        RequestLocation requestLocation = new RequestLocation(request.getUri());
        URI location = requestLocation.getLocation();

        BlockingQueue<Page> pages = pagesByLocation.get(location);
        long sequenceId = sequenceIdByLocation.get(location);
        // if location is complete return GONE
        if (completeByLocation.get(location) == Boolean.TRUE && (pages == null || pages.isEmpty())) {
            return new TestingResponse(HttpStatus.GONE, ImmutableListMultimap.of(PRESTO_PAGE_SEQUENCE_ID, String.valueOf(sequenceId)), new byte[0]);
        }
        // if no pages, return NO CONTENT
        if (pages == null) {
            return new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.of(PRESTO_PAGE_SEQUENCE_ID, String.valueOf(sequenceId)), new byte[0]);
        }

        assertEquals(requestLocation.getSequenceId(), sequenceId, "sequenceId");

        // wait for a single page to arrive
        Page page = null;
        try {
            page = pages.poll(10, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // if no page, return NO CONTENT
        if (page == null) {
            return new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.of(PRESTO_PAGE_SEQUENCE_ID, String.valueOf(sequenceId)), new byte[0]);
        }

        // add pages up to the size limit
        List<Page> responsePages = new ArrayList<>();
        responsePages.add(page);
        long responseSize = page.getDataSize().toBytes();
        while (responseSize < maxSize.toBytes()) {
            page = pages.poll();
            if (page == null) {
                break;
            }
            responsePages.add(page);
            responseSize += page.getDataSize().toBytes();
        }

        // update sequence id
        sequenceIdByLocation.put(location, sequenceId + responsePages.size());

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(64);
        PagesSerde.writePages(sliceOutput, responsePages);
        byte[] bytes = sliceOutput.slice().getBytes();
        return new TestingResponse(HttpStatus.OK,
                ImmutableListMultimap.of(
                        CONTENT_TYPE, PRESTO_PAGES,
                        PRESTO_PAGE_SEQUENCE_ID, String.valueOf(sequenceId)
                ),
                bytes);
    }

    private class RequestLocation
    {
        private final URI location;
        private final long sequenceId;

        public RequestLocation(URI uri)
        {
            String string = uri.toString();
            int index = string.lastIndexOf('/');
            location = URI.create(string.substring(0, index));
            sequenceId = Long.parseLong(string.substring(index + 1));
        }

        public URI getLocation()
        {
            return location;
        }

        public long getSequenceId()
        {
            return sequenceId;
        }
    }
}
