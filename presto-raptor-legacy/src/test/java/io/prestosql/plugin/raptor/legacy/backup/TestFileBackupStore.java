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
package io.prestosql.plugin.raptor.legacy.backup;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.UUID;

import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestFileBackupStore
        extends AbstractTestBackupStore<FileBackupStore>
{
    @BeforeClass
    public void setup()
    {
        temporary = createTempDir();
        store = new FileBackupStore(new File(temporary, "backup"));
        store.start();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        deleteRecursively(temporary.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testFilePaths()
    {
        UUID uuid = UUID.fromString("701e1a79-74f7-4f56-b438-b41e8e7d019d");
        File expected = new File(temporary, format("backup/70/1e/%s.orc", uuid));
        assertEquals(store.getBackupFile(uuid), expected);
    }
}
