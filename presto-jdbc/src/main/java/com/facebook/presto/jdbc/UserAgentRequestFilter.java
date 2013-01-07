package com.facebook.presto.jdbc;

import com.google.common.net.HttpHeaders;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.Request;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.http.client.Request.Builder.fromRequest;

public class UserAgentRequestFilter
        implements HttpRequestFilter
{
    private final String userAgent;

    public UserAgentRequestFilter(String userAgent)
    {
        this.userAgent = checkNotNull(userAgent, "userAgent is null");
    }

    @Override
    public Request filterRequest(Request request)
    {
        return fromRequest(request)
                .addHeader(HttpHeaders.USER_AGENT, userAgent)
                .build();
    }
}
