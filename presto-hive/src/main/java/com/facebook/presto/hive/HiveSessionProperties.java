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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import io.airlift.units.DataSize;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;

public final class HiveSessionProperties
{
    public static final String STORAGE_FORMAT_PROPERTY = "storage_format";
    private static final String OPTIMIZED_READER_ENABLED = "optimized_reader_enabled";
    private static final String ORC_MAX_MERGE_DISTANCE = "orc_max_merge_distance";
    private static final String ORC_MAX_READ_SIZE = "orc_max_read_size";

    private HiveSessionProperties()
    {
    }

    public static HiveStorageFormat getHiveStorageFormat(ConnectorSession session, HiveStorageFormat defaultValue)
    {
        String storageFormatString = session.getProperties().get(STORAGE_FORMAT_PROPERTY);
        if (storageFormatString == null) {
            return defaultValue;
        }

        try {
            return HiveStorageFormat.valueOf(storageFormatString.toUpperCase());
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, "Hive storage-format is invalid: " + storageFormatString);
        }
    }

    public static boolean isOptimizedReaderEnabled(ConnectorSession session, boolean defaultValue)
    {
        return isEnabled(OPTIMIZED_READER_ENABLED, session, defaultValue);
    }

    public static DataSize getOrcMaxMergeDistance(ConnectorSession session, DataSize defaultValue)
    {
        String maxMergeDistanceString = session.getProperties().get(ORC_MAX_MERGE_DISTANCE);
        if (maxMergeDistanceString == null) {
            return defaultValue;
        }

        try {
            return DataSize.valueOf(maxMergeDistanceString);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, ORC_MAX_MERGE_DISTANCE + " is invalid: " + maxMergeDistanceString);
        }
    }

    public static DataSize getOrcMaxReadSize(ConnectorSession session, DataSize defaultValue)
    {
        String maxReadSizeString = session.getProperties().get(ORC_MAX_READ_SIZE);
        if (maxReadSizeString == null) {
            return defaultValue;
        }

        try {
            return DataSize.valueOf(maxReadSizeString);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, ORC_MAX_READ_SIZE + " is invalid: " + maxReadSizeString);
        }
    }

    private static boolean isEnabled(String propertyName, ConnectorSession session, boolean defaultValue)
    {
        String enabled = session.getProperties().get(propertyName);
        if (enabled == null) {
            return defaultValue;
        }

        return Boolean.valueOf(enabled);
    }
}
