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
package com.facebook.presto.plugin.opensearch;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Tests for OpenSearchConfig nested field configuration properties.
 */
public class TestOpenSearchConfigNestedFields
{
    @Test
    public void testDefaultValues()
    {
        OpenSearchConfig config = new OpenSearchConfig();

        assertTrue(config.isNestedFieldAccessEnabled());
        assertEquals(config.getNestedMaxDepth(), 5);
        assertTrue(config.isNestedOptimizeQueries());
        assertFalse(config.isNestedDiscoverDynamicFields());
        assertFalse(config.isNestedLogMissingFields());
    }

    @Test
    public void testEnableNestedFieldAccess()
    {
        OpenSearchConfig config = new OpenSearchConfig();
        config.setNestedFieldAccessEnabled(true);

        assertTrue(config.isNestedFieldAccessEnabled());
    }

    @Test
    public void testDisableNestedFieldAccess()
    {
        OpenSearchConfig config = new OpenSearchConfig();
        config.setNestedFieldAccessEnabled(false);

        assertFalse(config.isNestedFieldAccessEnabled());
    }

    @Test
    public void testSetMaxDepth()
    {
        OpenSearchConfig config = new OpenSearchConfig();
        config.setNestedMaxDepth(10);

        assertEquals(config.getNestedMaxDepth(), 10);
    }

    @Test
    public void testSetMaxDepthMinimum()
    {
        OpenSearchConfig config = new OpenSearchConfig();
        config.setNestedMaxDepth(1);

        assertEquals(config.getNestedMaxDepth(), 1);
    }

    @Test
    public void testSetMaxDepthLarge()
    {
        OpenSearchConfig config = new OpenSearchConfig();
        config.setNestedMaxDepth(20);

        assertEquals(config.getNestedMaxDepth(), 20);
    }

    @Test
    public void testEnableQueryOptimization()
    {
        OpenSearchConfig config = new OpenSearchConfig();
        config.setNestedOptimizeQueries(true);

        assertTrue(config.isNestedOptimizeQueries());
    }

    @Test
    public void testDisableQueryOptimization()
    {
        OpenSearchConfig config = new OpenSearchConfig();
        config.setNestedOptimizeQueries(false);

        assertFalse(config.isNestedOptimizeQueries());
    }

    @Test
    public void testEnableDynamicFieldDiscovery()
    {
        OpenSearchConfig config = new OpenSearchConfig();
        config.setNestedDiscoverDynamicFields(true);

        assertTrue(config.isNestedDiscoverDynamicFields());
    }

    @Test
    public void testDisableDynamicFieldDiscovery()
    {
        OpenSearchConfig config = new OpenSearchConfig();
        config.setNestedDiscoverDynamicFields(false);

        assertFalse(config.isNestedDiscoverDynamicFields());
    }

    @Test
    public void testEnableMissingFieldLogging()
    {
        OpenSearchConfig config = new OpenSearchConfig();
        config.setNestedLogMissingFields(true);

        assertTrue(config.isNestedLogMissingFields());
    }

    @Test
    public void testDisableMissingFieldLogging()
    {
        OpenSearchConfig config = new OpenSearchConfig();
        config.setNestedLogMissingFields(false);

        assertFalse(config.isNestedLogMissingFields());
    }

    @Test
    public void testProductionConfiguration()
    {
        // Production: enabled, moderate depth, optimized, no dynamic discovery, no logging
        OpenSearchConfig config = new OpenSearchConfig();
        config.setNestedFieldAccessEnabled(true);
        config.setNestedMaxDepth(5);
        config.setNestedOptimizeQueries(true);
        config.setNestedDiscoverDynamicFields(false);
        config.setNestedLogMissingFields(false);

        assertTrue(config.isNestedFieldAccessEnabled());
        assertEquals(config.getNestedMaxDepth(), 5);
        assertTrue(config.isNestedOptimizeQueries());
        assertFalse(config.isNestedDiscoverDynamicFields());
        assertFalse(config.isNestedLogMissingFields());
    }

    @Test
    public void testDevelopmentConfiguration()
    {
        // Development: enabled, higher depth, optimized, dynamic discovery, logging enabled
        OpenSearchConfig config = new OpenSearchConfig();
        config.setNestedFieldAccessEnabled(true);
        config.setNestedMaxDepth(10);
        config.setNestedOptimizeQueries(true);
        config.setNestedDiscoverDynamicFields(true);
        config.setNestedLogMissingFields(true);

        assertTrue(config.isNestedFieldAccessEnabled());
        assertEquals(config.getNestedMaxDepth(), 10);
        assertTrue(config.isNestedOptimizeQueries());
        assertTrue(config.isNestedDiscoverDynamicFields());
        assertTrue(config.isNestedLogMissingFields());
    }

    @Test
    public void testPerformanceOptimizedConfiguration()
    {
        // Performance: enabled, shallow depth, optimized, no extras
        OpenSearchConfig config = new OpenSearchConfig();
        config.setNestedFieldAccessEnabled(true);
        config.setNestedMaxDepth(3);
        config.setNestedOptimizeQueries(true);
        config.setNestedDiscoverDynamicFields(false);
        config.setNestedLogMissingFields(false);

        assertTrue(config.isNestedFieldAccessEnabled());
        assertEquals(config.getNestedMaxDepth(), 3);
        assertTrue(config.isNestedOptimizeQueries());
        assertFalse(config.isNestedDiscoverDynamicFields());
        assertFalse(config.isNestedLogMissingFields());
    }

    @Test
    public void testDebuggingConfiguration()
    {
        // Debugging: enabled, deep depth, not optimized, dynamic discovery, logging
        OpenSearchConfig config = new OpenSearchConfig();
        config.setNestedFieldAccessEnabled(true);
        config.setNestedMaxDepth(15);
        config.setNestedOptimizeQueries(false);
        config.setNestedDiscoverDynamicFields(true);
        config.setNestedLogMissingFields(true);

        assertTrue(config.isNestedFieldAccessEnabled());
        assertEquals(config.getNestedMaxDepth(), 15);
        assertFalse(config.isNestedOptimizeQueries());
        assertTrue(config.isNestedDiscoverDynamicFields());
        assertTrue(config.isNestedLogMissingFields());
    }

    @Test
    public void testIndependentSettings()
    {
        // Verify settings are independent
        OpenSearchConfig config = new OpenSearchConfig();

        config.setNestedFieldAccessEnabled(false);
        assertFalse(config.isNestedFieldAccessEnabled());
        assertTrue(config.isNestedOptimizeQueries()); // Should still be default

        config.setNestedOptimizeQueries(false);
        assertFalse(config.isNestedOptimizeQueries());
        assertEquals(config.getNestedMaxDepth(), 5); // Should still be default

        config.setNestedMaxDepth(8);
        assertEquals(config.getNestedMaxDepth(), 8);
        assertFalse(config.isNestedDiscoverDynamicFields()); // Should still be default
    }

    @Test
    public void testMultipleConfigurationChanges()
    {
        OpenSearchConfig config = new OpenSearchConfig();

        // Change multiple times
        config.setNestedMaxDepth(3);
        assertEquals(config.getNestedMaxDepth(), 3);

        config.setNestedMaxDepth(7);
        assertEquals(config.getNestedMaxDepth(), 7);

        config.setNestedMaxDepth(5);
        assertEquals(config.getNestedMaxDepth(), 5);

        // Toggle boolean settings
        config.setNestedFieldAccessEnabled(false);
        assertFalse(config.isNestedFieldAccessEnabled());

        config.setNestedFieldAccessEnabled(true);
        assertTrue(config.isNestedFieldAccessEnabled());
    }
}
