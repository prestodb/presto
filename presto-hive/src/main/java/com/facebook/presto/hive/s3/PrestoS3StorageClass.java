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
package com.facebook.presto.hive.s3;

import software.amazon.awssdk.services.s3.model.StorageClass;

import static java.util.Objects.requireNonNull;

public enum PrestoS3StorageClass
{
    STANDARD(StorageClass.STANDARD),
    INTELLIGENT_TIERING(StorageClass.INTELLIGENT_TIERING);

    private final StorageClass s3StorageClass;

    PrestoS3StorageClass(StorageClass s3StorageClass)
    {
        this.s3StorageClass = requireNonNull(s3StorageClass, "s3StorageClass is null");
    }

    public StorageClass getS3StorageClass()
    {
        return s3StorageClass;
    }

    /**
     * Convert from string name to StorageClass, with fallback to STANDARD
     */
    public static StorageClass fromString(String storageClassName)
    {
        if (storageClassName == null) {
            return StorageClass.STANDARD;
        }

        try {
            return PrestoS3StorageClass.valueOf(storageClassName.toUpperCase()).getS3StorageClass();
        }
        catch (IllegalArgumentException e) {
            // Fallback for unknown storage classes
            return StorageClass.STANDARD;
        }
    }

    /**
     * Helper method to convert PrestoS3StorageClass to SDK v2 StorageClass
     */
    public static StorageClass convertToStorageClass(PrestoS3StorageClass prestoStorageClass)
    {
        if (prestoStorageClass == null) {
            return StorageClass.STANDARD;
        }
        return prestoStorageClass.getS3StorageClass();
    }
}
