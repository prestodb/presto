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
package com.facebook.presto.spark.classloader_interface;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.broadcast.Broadcast;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PrestoSparkNativeTaskInputs
        implements PrestoSparkTaskInputs
{
    // fragmentId -> PrestoSparkShuffleReadDescriptor for native execution
    private final Map<String, PrestoSparkShuffleReadDescriptor> shuffleReadDescriptors;
    private final Optional<PrestoSparkShuffleWriteDescriptor> shuffleWriteDescriptor;

    private final Map<String, Broadcast<?>> broadcastInputs;

    public PrestoSparkNativeTaskInputs(
            Map<String, PrestoSparkShuffleReadDescriptor> shuffleReadDescriptors,
            Optional<PrestoSparkShuffleWriteDescriptor> shuffleWriteDescriptor,
            Map<String, Broadcast<?>> broadcastInputs)
    {
        this.shuffleReadDescriptors = ImmutableMap.copyOf(requireNonNull(shuffleReadDescriptors, "shuffleReadDescriptors is null"));
        this.shuffleWriteDescriptor = requireNonNull(shuffleWriteDescriptor, "shuffleWriteDescriptor is null");
        this.broadcastInputs = ImmutableMap.copyOf(requireNonNull(broadcastInputs, "broadcastInputs is null"));
    }

    public Map<String, PrestoSparkShuffleReadDescriptor> getShuffleReadDescriptors()
    {
        return shuffleReadDescriptors;
    }

    public Optional<PrestoSparkShuffleWriteDescriptor> getShuffleWriteDescriptor()
    {
        return shuffleWriteDescriptor;
    }

    public Map<String, Broadcast<?>> getBroadcastInputs()
    {
        return broadcastInputs;
    }
}
