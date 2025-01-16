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
package com.facebook.presto.execution.resourceGroups;

import com.facebook.presto.execution.resourceGroups.LegacyResourceGroupConfigurationManager.VoidContext;
import com.facebook.presto.spi.resourceGroups.ResourceGroup;
import com.facebook.presto.spi.resourceGroups.ResourceGroupConfigurationManager;
import com.facebook.presto.spi.resourceGroups.ResourceGroupConfigurationManagerContext;
import com.facebook.presto.spi.resourceGroups.ResourceGroupConfigurationManagerFactory;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.facebook.presto.spi.resourceGroups.SelectionCriteria;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class LegacyResourceGroupConfigurationManager
        implements ResourceGroupConfigurationManager<VoidContext>
{
    enum VoidContext
    {
        NONE
    }

    public static final String NAME = "legacy";
    public static final String HARD_CONCURRENCY_LIMIT = "hard_concurrency_limit";
    public static final String MAX_QUEUED_QUERIES = "max_queued_queries";

    private static final ResourceGroupId GLOBAL = new ResourceGroupId("global");

    private final int hardConcurrencyLimit;
    private final int maxQueued;

    public LegacyResourceGroupConfigurationManager(int hardConcurrencyLimit, int maxQueued)
    {
        checkArgument(hardConcurrencyLimit > 0, "hardConcurrencyLimit must be greater than 0");
        checkArgument(maxQueued > 0, "maxQueued must be greater than 0");
        this.hardConcurrencyLimit = hardConcurrencyLimit;
        this.maxQueued = maxQueued;
    }

    public static class Factory
            implements ResourceGroupConfigurationManagerFactory
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public ResourceGroupConfigurationManager<?> create(Map<String, String> config, ResourceGroupConfigurationManagerContext context)
        {
            String hardConcurrencyLimitString = requireNonNull(config.get(HARD_CONCURRENCY_LIMIT), "LegacyResourceGroupConfigurationManager must have config hard_concurrency_liimt");
            int hardConcurrencyLimit = Integer.parseInt(hardConcurrencyLimitString);
            String maxQueuedString = requireNonNull(config.get(MAX_QUEUED_QUERIES), "LegacyResourceGroupConfigurationManager must have config max_queued_queries");
            int maxQueued = Integer.parseInt(maxQueuedString);
            return new LegacyResourceGroupConfigurationManager(hardConcurrencyLimit, maxQueued);
        }
    }

    @Override
    public void configure(ResourceGroup group, SelectionContext<VoidContext> criteria)
    {
        checkArgument(group.getId().equals(GLOBAL), "Unexpected resource group: %s", group.getId());
        group.setMaxQueuedQueries(maxQueued);
        group.setHardConcurrencyLimit(hardConcurrencyLimit);
    }

    @Override
    public Optional<SelectionContext<VoidContext>> match(SelectionCriteria criteria)
    {
        return Optional.of(new SelectionContext<>(GLOBAL, VoidContext.NONE));
    }
}
