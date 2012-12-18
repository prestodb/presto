package com.facebook.presto.event.scribe.nectar;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.event.client.EventClient;

import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class ScribeNectarEventModule
    implements Module
{
    @Override
    public void configure(Binder binder)
    {
        bindConfig(binder).to(NectarEventMappingConfiguration.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, Object.class);
        binder.bind(EventClient.class).to(ScribeNectarEventClient.class).in(Scopes.SINGLETON);
    }
}
