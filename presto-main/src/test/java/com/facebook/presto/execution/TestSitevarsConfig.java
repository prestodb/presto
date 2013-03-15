package com.facebook.presto.execution;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.Stage;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationModule;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static org.testng.Assert.assertSame;

public class TestSitevarsConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(SitevarsConfig.class)
                .setImportsEnabled(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("sitevar.imports-enabled", "false")
                .build();

        SitevarsConfig expected = new SitevarsConfig()
                .setImportsEnabled(false);

        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test
    public void testSitevarsIsASingleton()
    {
        Injector inj = Guice.createInjector(Stage.PRODUCTION,
                new ConfigurationModule(new ConfigurationFactory(ImmutableMap.<String, String> of())),
                new Module() {
            @Override
            public void configure(final Binder binder) {
                bindConfig(binder).to(SitevarsConfig.class);
                binder.bind(Sitevars.class).in(Scopes.SINGLETON);
            }
        });

        Sitevars s1 = inj.getInstance(Sitevars.class);
        Sitevars s2 = inj.getInstance(Sitevars.class);
        assertSame(s1, s2);
    }
}
