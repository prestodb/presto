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

import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

import static java.util.Objects.requireNonNull;

/**
 * LOG_DELIVERY_WRITE is not available in AWS SDK v2, see docs: https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/ObjectCannedACL.html
 */

public enum PrestoS3AclType
{
    AUTHENTICATED_READ(ObjectCannedACL.AUTHENTICATED_READ),
    AWS_EXEC_READ(ObjectCannedACL.AWS_EXEC_READ),
    BUCKET_OWNER_FULL_CONTROL(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL),
    BUCKET_OWNER_READ(ObjectCannedACL.BUCKET_OWNER_READ),
    PRIVATE(ObjectCannedACL.PRIVATE),
    PUBLIC_READ(ObjectCannedACL.PUBLIC_READ),
    PUBLIC_READ_WRITE(ObjectCannedACL.PUBLIC_READ_WRITE);

    private final ObjectCannedACL cannedACL;

    PrestoS3AclType(ObjectCannedACL cannedACL)
    {
        this.cannedACL = requireNonNull(cannedACL, "cannedACL is null");
    }

    public ObjectCannedACL getCannedACL()
    {
        return cannedACL;
    }
}
