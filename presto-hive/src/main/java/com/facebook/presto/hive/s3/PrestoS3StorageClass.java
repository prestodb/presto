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

import com.amazonaws.services.s3.model.StorageClass;

import static java.util.Objects.requireNonNull;

public enum PrestoS3StorageClass {
    STANDARD(StorageClass.Standard),
    INTELLIGENT_TIERING(StorageClass.IntelligentTiering);

    private StorageClass s3StorageClass;

    PrestoS3StorageClass(StorageClass s3StorageClass)
    {
        this.s3StorageClass = requireNonNull(s3StorageClass, "s3StorageClass is null");
    }

    public StorageClass getS3StorageClass()
    {
        return s3StorageClass;
    }
}
