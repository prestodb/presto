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
package com.facebook.presto.iceberg.s3;

import com.amazonaws.AmazonClientException;
import com.facebook.presto.hive.s3.PrestoS3FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class PrestoIcebergNativeS3FileSystem
        extends PrestoS3FileSystem
{
    @Override
    public boolean mkdirs(Path f, FsPermission permission)
    {
        try {
            s3.putObject(getBucketName(uri), keyFromPath(f) + PATH_SEPARATOR, "");
            return true;
        }
        catch (AmazonClientException e) {
            return false;
        }
    }
}
