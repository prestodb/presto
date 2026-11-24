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
package com.facebook.presto.iceberg;

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.LocationProvider;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.TableProperties.WRITE_DATA_LOCATION;

/** The output path and physical partition layout for one Iceberg write. */
public final class IcebergWriteLocation
{
    private final String location;
    private final Map<String, String> storageProperties;
    private final boolean includePartitionPaths;

    public IcebergWriteLocation(String location, Map<String, String> storageProperties, boolean includePartitionPaths)
    {
        this.location = requireNonNull(location, "location is null");
        this.storageProperties = ImmutableMap.copyOf(requireNonNull(storageProperties, "storageProperties is null"));
        this.includePartitionPaths = includePartitionPaths;
    }

    /** The pre-existing contract: the table location, its properties and partition subdirectories. */
    public static IcebergWriteLocation forTable(Table table)
    {
        requireNonNull(table, "table is null");
        return new IcebergWriteLocation(table.location(), table.properties(), true);
    }

    /** The caller must supply a provider with a fixed directory for each filename. */
    public static IcebergWriteLocation forDirectoryProvider(Table table, LocationProvider provider, boolean includePartitionPaths)
    {
        String fileLocation = provider.newDataLocation("presto-write-location");
        int separator = fileLocation.lastIndexOf('/');
        checkState(separator > 0, "Location provider returned a path without a directory: %s", fileLocation);
        String directory = fileLocation.substring(0, separator);
        Map<String, String> properties = new HashMap<>(table.properties());
        properties.put(WRITE_DATA_LOCATION, directory);
        return new IcebergWriteLocation(directory, properties, includePartitionPaths);
    }

    public String getLocation()
    {
        return location;
    }

    public Map<String, String> getStorageProperties()
    {
        return storageProperties;
    }

    public boolean isIncludePartitionPaths()
    {
        return includePartitionPaths;
    }
}
