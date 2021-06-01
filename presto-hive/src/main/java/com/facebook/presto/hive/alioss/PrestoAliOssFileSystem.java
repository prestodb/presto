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
package com.facebook.presto.hive.alioss;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;

import java.io.FileNotFoundException;
import java.io.IOException;

public class PrestoAliOssFileSystem
        extends AliyunOSSFileSystem
{
    @Override
    public boolean rename(Path srcPath, Path dstPath)
            throws IOException
    {
        try {
            getFileStatus(srcPath);
        }
        catch (FileNotFoundException e) {
            // Presto prefers return false rather than throwing exception
            // if srcPath not exists
            return false;
        }

        try {
            if (!directory(dstPath)) {
                // cannot copy a file to an existing file
                return false;
            }
            // move source under destination directory
            dstPath = new Path(dstPath, srcPath.getName());
        }
        catch (FileNotFoundException e) {
            // destination does not exist
        }

        return super.rename(srcPath, dstPath);
    }

    private boolean directory(Path path)
            throws IOException
    {
        return getFileStatus(path).isDirectory();
    }
}
