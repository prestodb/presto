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
package com.facebook.presto.delta;

import io.delta.kernel.types.FieldMetadata;

public class DeltaColumnMetadataUtil
{
    private static final String COLUMN_MAPPING_ID = "delta.columnMapping.id";
    private static final String COLUMN_MAPPING_PHYSICAL_NAME = "delta.columnMapping.physicalName";

    private DeltaColumnMetadataUtil()
    {
        // Empty constructor to prevent instantiation
    }

    /**
     * Obtains the column id from the metadata.
     * Requires column mapping to be enabled in the table to be enabled
     * @param metadata the delta kernel field metadata
     * @return a long representing the column id, null if column mapping is not enabled
     */
    public static Long getColumnIdFromMetadata(FieldMetadata metadata)
    {
        return (Long) metadata.get(COLUMN_MAPPING_ID);
    }

    /**
     * Obtains the column physical name from the metadata.
     * Requires column mapping to be enabled in the table to be enabled
     * @param metadata the delta kernel field metadata
     * @return a string representing the physical column name, null if column mapping is not enabled
     */
    public static String getPhysicalNameFromMetadata(FieldMetadata metadata)
    {
        return (String) metadata.get(COLUMN_MAPPING_PHYSICAL_NAME);
    }
}
