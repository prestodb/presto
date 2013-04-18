package com.facebook.presto.metadata;

import com.facebook.presto.spi.TableHandle;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

public class TableHandleJacksonModule
        extends AbstractTypedJacksonModule<TableHandle>
{
    @Inject
    private TableHandleJacksonModule(HandleResolver handleResolver)
    {
        super(TableHandle.class, "type", new TableHandleJsonTypeIdResolver(handleResolver));
    }

    private static class TableHandleJsonTypeIdResolver
            implements JsonTypeIdResolver<TableHandle>
    {
        private final HandleResolver handleResolver;

        private TableHandleJsonTypeIdResolver(HandleResolver HandleResolver)
        {
            this.handleResolver = checkNotNull(HandleResolver, "handleIdResolvers is null");
        }

        @Override
        public String getId(TableHandle tableHandle)
        {
            return handleResolver.getId(tableHandle);
        }

        @Override
        public Class<? extends TableHandle> getType(String id)
        {
            return handleResolver.getTableHandleClass(id);
        }
    }
}
