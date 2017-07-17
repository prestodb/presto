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
package com.facebook.presto.hive.filesystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

public class MockRawLocalFileSystem extends RawLocalFileSystem
{
    @Override
    public AclStatus getAclStatus(Path path) throws IOException
    {
        AclStatus.Builder builder = new AclStatus.Builder();
        return builder
                .owner("presto")
                .group("presto")
                .setPermission(FsPermission.getDefault())
                .build();
    }

    @Override
    public boolean mkdirs(Path f) throws IOException
    {
        try {
            return super.mkdirs(f);
        }
        catch (IOException ex) {
            return true;
        }
    }

    @Override
    public void access(Path path, FsAction mode) throws AccessControlException, FileNotFoundException, IOException
    {
    }

    @Override
    public void setOwner(Path p, String username, String groupname) throws IOException
    {
    }

    @Override
    public void modifyAclEntries(Path path, List<AclEntry> aclSpec) throws IOException
    {
    }
}
