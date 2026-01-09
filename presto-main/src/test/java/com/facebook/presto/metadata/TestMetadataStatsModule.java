/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.metadata;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestMetadataStatsModule
{
    @Test
    public void testModuleCreation()
    {
        MetadataStatsModule module = new MetadataStatsModule();
        assertNotNull(module, "Module should be created");
    }

    @Test
    public void testModuleBindsMetadata()
    {
        MetadataManager testMetadataManager = MetadataManager.createTestMetadataManager();
        Injector injector = Guice.createInjector(binder -> {
            binder.bind(Metadata.class).annotatedWith(ForMetadata.class).toInstance(testMetadataManager);
        }, new MetadataStatsModule());

        MetadataManagerStats stats = injector.getInstance(MetadataManagerStats.class);
        assertNotNull(stats, "MetadataManagerStats should be bound");

        Metadata metadata = injector.getInstance(Metadata.class);
        assertNotNull(metadata, "Metadata should be bound");

        assertTrue(metadata instanceof StatsRecordingMetadataManager,
                "Metadata should be wrapped with StatsRecordingMetadataManager");
    }

    @Test
    public void testStatsAreSingleton()
    {
        MetadataManager testMetadataManager = MetadataManager.createTestMetadataManager();
        Injector injector = Guice.createInjector(binder -> {
            binder.bind(Metadata.class).annotatedWith(ForMetadata.class).toInstance(testMetadataManager);
        }, new MetadataStatsModule());

        MetadataManagerStats stats1 = injector.getInstance(MetadataManagerStats.class);
        MetadataManagerStats stats2 = injector.getInstance(MetadataManagerStats.class);

        assertTrue(stats1 == stats2, "MetadataManagerStats should be singleton");

        Metadata metadata1 = injector.getInstance(Metadata.class);
        Metadata metadata2 = injector.getInstance(Metadata.class);

        assertTrue(metadata1 == metadata2, "Metadata should be singleton");
    }
}
