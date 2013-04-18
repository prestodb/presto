package com.facebook.presto.metadata;

import com.facebook.presto.spi.Split;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

public class SplitJacksonModule
        extends AbstractTypedJacksonModule<Split>
{
    @Inject
    public SplitJacksonModule(HandleResolver handleResolver)
    {
        super(Split.class, "type", new SplitJsonTypeIdResolver(handleResolver));
    }

    private static class SplitJsonTypeIdResolver
            implements JsonTypeIdResolver<Split>
    {
        private final HandleResolver handleResolver;

        private SplitJsonTypeIdResolver(HandleResolver HandleResolver)
        {
            this.handleResolver = checkNotNull(HandleResolver, "handleIdResolvers is null");
        }

        @Override
        public String getId(Split split)
        {
            return handleResolver.getId(split);
        }

        @Override
        public Class<? extends Split> getType(String id)
        {
            return handleResolver.getSplitClass(id);
        }
    }
}
