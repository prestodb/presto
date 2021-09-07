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
package com.facebook.presto.execution.scheduler.nodeselection;

import com.facebook.airlift.json.JsonCodec;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static java.util.Objects.requireNonNull;

public class FileBasedNodeSelectionConfigurationManager
        extends AbstractNodeSelectionConfigurationManager
{
    private final NodeSelectionConfigurationSpec nodeSelectionConfigurationSpecs;

    @Inject
    public FileBasedNodeSelectionConfigurationManager(JsonCodec<NodeSelectionConfigurationSpec> nodeFilterConfigurationJsonCodec, NodeSelectionConfig nodeSelectionConfig)
    {
        try {
            this.nodeSelectionConfigurationSpecs = requireNonNull(nodeFilterConfigurationJsonCodec.fromJson(Files.readAllBytes(Paths.get(nodeSelectionConfig.getConfigFile()))), "nodeFilterConfigurationSpecs is null");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected NodeSelectionConfigurationSpec loadNodeSelectionConfigurationSpec()
    {
        return nodeSelectionConfigurationSpecs;
    }
}
