package com.facebook.presto.metadata;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.util.Map;

import static io.airlift.testing.ValidationAssertions.assertFailsValidation;

public class TestDatabaseLocalStorageManagerConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(DatabaseLocalStorageManagerConfig.class)
                .setDataDirectory(new File("var/data")));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("storage-manager.data-directory", "/data")
                .build();

        DatabaseLocalStorageManagerConfig expected = new DatabaseLocalStorageManagerConfig()
                .setDataDirectory(new File("/data"));

        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test
    public void testValidations()
    {
        assertFailsValidation(new DatabaseLocalStorageManagerConfig().setDataDirectory(null), "dataDirectory", "may not be null", NotNull.class);
    }
}
