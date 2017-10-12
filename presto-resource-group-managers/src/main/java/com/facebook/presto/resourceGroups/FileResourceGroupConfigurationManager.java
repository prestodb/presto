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
package com.facebook.presto.resourceGroups;

import com.facebook.presto.spi.memory.ClusterMemoryPoolManager;
import com.facebook.presto.spi.resourceGroups.ResourceGroup;
import com.facebook.presto.spi.resourceGroups.ResourceGroupSelector;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.base.Throwables;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FileResourceGroupConfigurationManager
        extends AbstractResourceConfigurationManager
{
    private static final JsonCodec<ManagerSpec> CODEC = new JsonCodecFactory(
            () -> new ObjectMapperProvider().get().enable(FAIL_ON_UNKNOWN_PROPERTIES))
            .jsonCodec(ManagerSpec.class);

    private final List<ResourceGroupSpec> rootGroups;
    private final List<ResourceGroupSelector> selectors;
    private final Optional<Duration> cpuQuotaPeriod;

    @Inject
    public FileResourceGroupConfigurationManager(ClusterMemoryPoolManager memoryPoolManager, FileResourceGroupConfig config)
    {
        super(memoryPoolManager);
        requireNonNull(config, "config is null");

        ManagerSpec managerSpec;
        try {
            managerSpec = CODEC.fromJson(Files.readAllBytes(Paths.get(config.getConfigFile())));
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        catch (IllegalArgumentException e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnrecognizedPropertyException) {
                UnrecognizedPropertyException ex = (UnrecognizedPropertyException) cause;
                String message = format("Unknown property at line %s:%s: %s",
                        ex.getLocation().getLineNr(),
                        ex.getLocation().getColumnNr(),
                        ex.getPropertyName());
                throw new IllegalArgumentException(message, e);
            }
            if (cause instanceof JsonMappingException) {
                // remove the extra "through reference chain" message
                if (cause.getCause() != null) {
                    cause = cause.getCause();
                }
                throw new IllegalArgumentException(cause.getMessage(), e);
            }
            throw e;
        }

        this.rootGroups = managerSpec.getRootGroups();
        this.cpuQuotaPeriod = managerSpec.getCpuQuotaPeriod();
        validateRootGroups(managerSpec);
        this.selectors = buildSelectors(managerSpec);
    }

    @Override
    protected Optional<Duration> getCpuQuotaPeriod()
    {
        return cpuQuotaPeriod;
    }

    @Override
    protected List<ResourceGroupSpec> getRootGroups()
    {
        return rootGroups;
    }

    @Override
    public void configure(ResourceGroup group, SelectionContext context)
    {
        Map.Entry<ResourceGroupIdTemplate, ResourceGroupSpec> entry = getMatchingSpec(group, context);
        configureGroup(group, entry.getValue());
    }

    @Override
    public List<ResourceGroupSelector> getSelectors()
    {
        return selectors;
    }
}
