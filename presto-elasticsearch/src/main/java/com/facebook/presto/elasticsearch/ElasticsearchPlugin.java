
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class ElasticsearchPlugin
        implements Plugin
{
    private TypeManager typeManager;
    private Map<String, String> optionalConfig = ImmutableMap.of();

    @Override
    public synchronized void setOptionalConfig(Map<String, String> optionalConfig)
    {
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
    }

    @Inject
    public synchronized void setTypeManager(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    public synchronized Map<String, String> getOptionalConfig()
    {
        return optionalConfig;
    }

    @Override
    public synchronized <T> List<T> getServices(Class<T> type)
    {
        if (type == ConnectorFactory.class) {
            return ImmutableList.of(type.cast(new ElasticsearchConnectorFactory(typeManager, getOptionalConfig())));
        }
        return ImmutableList.of();
    }
}
