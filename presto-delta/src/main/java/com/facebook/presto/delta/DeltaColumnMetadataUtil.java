/*
 * Copyright (c) 2026. DENODO Technologies.
 * http://www.denodo.com
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of DENODO
 * Technologies ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with DENODO.
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
