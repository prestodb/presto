package com.facebook.presto.metadata;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.util.Map;

import static io.airlift.testing.ValidationAssertions.assertFailsValidation;

public class TestStorageManagerConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(StorageManagerConfig.class)
                .setDataDirectory(new File("var/data")));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("storage-manager.data-directory", "/data")
                .build();

        StorageManagerConfig expected = new StorageManagerConfig()
                .setDataDirectory(new File("/data"));

        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test
    public void testValidations()
    {
        assertFailsValidation(new StorageManagerConfig().setDataDirectory(null), "dataDirectory", "may not be null", NotNull.class);
    }
}
