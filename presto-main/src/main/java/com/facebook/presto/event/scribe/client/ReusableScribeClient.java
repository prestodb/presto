package com.facebook.presto.event.scribe.client;

import com.facebook.swift.service.RuntimeTTransportException;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Provider;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * The standard Thrift clients need to be recreated after encountering TTransportExceptions.
 * This wrapper client allows the same client to be reused despite transport errors.
 */
@NotThreadSafe
public class ReusableScribeClient
    implements ScribeClient
{
    private final Provider<ScribeClient> clientProvider;
    private ScribeClient client;
    private boolean closed;

    public ReusableScribeClient(Provider<ScribeClient> clientProvider)
    {
        this.clientProvider = checkNotNull(clientProvider, "clientProvider is null");
    }

    public static ReusableScribeClient makeReusableClient(Provider<ScribeClient> clientProvider)
    {
        return new ReusableScribeClient(clientProvider);
    }

    @Override
    public ResultCode log(List<LogEntry> messages)
    {
        checkState(!closed, "client already closed");
        if (client == null) {
            client = clientProvider.get();
        }
        try {
            return client.log(messages);
        }
        catch (RuntimeTTransportException e) {
            client.close();
            client = null;
            throw e;
        }
    }

    @Override
    public void close()
    {
        if (client != null)
        {
            client.close();
            closed = true;
        }
    }
}
