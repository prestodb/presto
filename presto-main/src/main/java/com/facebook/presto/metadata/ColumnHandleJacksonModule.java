package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnHandle;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

public class ColumnHandleJacksonModule
        extends AbstractTypedJacksonModule<ColumnHandle>
{
    @Inject
    public ColumnHandleJacksonModule(HandleResolver handleResolver)
    {
        super(ColumnHandle.class, "type", new ColumnHandleJsonTypeIdResolver(handleResolver));
    }

    private static class ColumnHandleJsonTypeIdResolver
            implements JsonTypeIdResolver<ColumnHandle>
    {
        private final HandleResolver handleResolver;

        private ColumnHandleJsonTypeIdResolver(HandleResolver HandleResolver)
        {
            this.handleResolver = checkNotNull(HandleResolver, "handleIdResolvers is null");
        }

        @Override
        public String getId(ColumnHandle columnHandle)
        {
            return handleResolver.getId(columnHandle);
        }

        @Override
        public Class<? extends ColumnHandle> getType(String id)
        {
            return handleResolver.getColumnHandleClass(id);
        }
    }
}
