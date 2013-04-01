package com.facebook.presto.metadata;

import com.facebook.presto.client.ClientSession;
import com.google.inject.TypeLiteral;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;

import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.json.JsonCodec.jsonCodec;

@ThreadSafe
public class HttpMetadataClient
        implements Closeable
{
    private final AsyncHttpClient httpClient;
    private final URI queryUri;
    private final JsonCodec<Map<String, List<String>>> metadataResponseCodec;

    public HttpMetadataClient(ClientSession session,
            AsyncHttpClient httpClient)
    {
        checkNotNull(session, "session is null");
        this.httpClient = checkNotNull(httpClient, "httpClient is null");

        this.metadataResponseCodec = jsonCodec(new TypeLiteral<Map<String, List<String>>>() {
        });

        this.queryUri = HttpUriBuilder.uriBuilderFrom(session.getServer()).appendPath("/v1/metadata").build();
    }

    public List<String> getSchemaNames(String catalogName)
    {
        Request.Builder requestBuilder = prepareGet()
                .setUri(HttpUriBuilder.uriBuilderFrom(queryUri).appendPath(catalogName).build());

        Request request = requestBuilder.build();

        try {
            JsonResponse<Map<String, List<String>>> response = httpClient.execute(request, createFullJsonResponseHandler(metadataResponseCodec));
            return response.getStatusCode() == 200
                    ? firstNonNull(response.getValue().get("schemaNames"), Collections.<String>emptyList())
                    : Collections.<String>emptyList();
        }
        catch (Exception e) {
            return Collections.emptyList();
        }
    }

    public List<String> getTableNames(String catalogName, String schemaName)
    {
        Request.Builder requestBuilder = prepareGet()
                .setUri(HttpUriBuilder.uriBuilderFrom(queryUri).appendPath(catalogName).appendPath(schemaName).build());

        Request request = requestBuilder.build();

        try {
            JsonResponse<Map<String, List<String>>> response = httpClient.execute(request, createFullJsonResponseHandler(metadataResponseCodec));

            return response.getStatusCode() == 200
                    ? firstNonNull(response.getValue().get("tableNames"), Collections.<String>emptyList())
                    : Collections.<String>emptyList();
        }
        catch (Exception e) {
            return Collections.emptyList();
        }
    }

    @Override
    public void close()
    {
        httpClient.close();
    }
}
