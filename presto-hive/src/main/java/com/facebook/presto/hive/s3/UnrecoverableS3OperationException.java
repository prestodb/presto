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

import org.apache.hadoop.fs.Path;

import static java.lang.String.format;

/**
 * This exception is for stopping retries for S3 calls that shouldn't be retried.
 * For example, "Caused by: com.amazonaws.services.s3.model.AmazonS3Exception: Forbidden (Service: Amazon S3; Status Code: 403 ..."
 */
public class UnrecoverableS3OperationException
        extends RuntimeException
{
    UnrecoverableS3OperationException(Path path, Throwable cause)
    {
        // append the path info to the message
        super(format("%s (Path: %s)", cause, path), cause);
    }

    public UnrecoverableS3OperationException(String bucket, String key, Throwable cause)
    {
        // append bucket and key to the message
        super(format("%s (Bucket: %s, Key: %s)", cause, bucket, key));
    }
}
