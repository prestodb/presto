package com.facebook.presto.spiller;

import com.facebook.presto.common.Page;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.spi.security.SystemAccessControl;
import com.facebook.presto.spi.security.SystemAccessControlFactory;
import com.facebook.presto.spi.spiller.SingleStreamSpiller;
import com.facebook.presto.spi.spiller.SingleStreamSpillerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SingleStreamSpillerManager
    implements SingleStreamSpiller
{
    private final Map<String, SingleStreamSpillerFactory> singleStreamSpillerFactories = new ConcurrentHashMap<>();

    private final AtomicReference<SingleStreamSpiller> systemAccessControl = new AtomicReference<>(new AccessControlManager.InitializingSystemAccessControl());

    public void addSingleStreamSpillerFactory(SingleStreamSpillerFactory spillerFactory)
    {
        requireNonNull(spillerFactory, "spillerFactory is null");

        if (singleStreamSpillerFactories.putIfAbsent(spillerFactory.getName(), spillerFactory) != null) {
            throw new IllegalArgumentException(format("Single stream spiller factory '%s' is already registered", spillerFactory.getName()));
        }
    }



    @Override
    public CompletableFuture<?> spill(Iterator<Page> page)
    {
        return null;
    }

    @Override
    public Iterator<Page> getSpilledPages()
    {
        return null;
    }

    @Override
    public long getSpilledPagesInMemorySize()
    {
        return 0;
    }

    @Override
    public CompletableFuture<List<Page>> getAllSpilledPages()
    {
        return null;
    }

    @Override
    public void close()
    {

    }
}
