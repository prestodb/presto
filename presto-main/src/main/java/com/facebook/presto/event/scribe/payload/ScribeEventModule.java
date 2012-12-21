package com.facebook.presto.event.scribe.payload;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.event.client.EventClient;

import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class ScribeEventModule
    implements Module
{
    @Override
    public void configure(Binder binder)
    {
        bindConfig(binder).to(EventMappingConfiguration.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, Object.class);
        binder.bind(EventClient.class).to(ScribeEventClient.class).in(Scopes.SINGLETON);
    }
}
