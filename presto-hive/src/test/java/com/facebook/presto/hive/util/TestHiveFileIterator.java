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
package com.facebook.presto.hive.util;

import com.facebook.presto.hive.HadoopDirectoryLister.HadoopFileInfoIterator;
import com.facebook.presto.hive.NamenodeStats;
import com.facebook.presto.hive.util.HiveFileIterator.ListDirectoryOperation;
import com.google.common.collect.Iterators;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.hive.NestedDirectoryPolicy.IGNORED;
import static com.facebook.presto.hive.NestedDirectoryPolicy.RECURSE;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.testng.Assert.assertEquals;

public class TestHiveFileIterator
{
    private static final String PATH_FILTER_MATCHED_PREFIX = "path_filter_test_file_";
    private static final String PATH_FILTER_NOT_MATCHED_PREFIX = "path_filter_not_matched_";

    private Configuration hadoopConf;
    private ListDirectoryOperation listDirectoryOperation;

    @BeforeClass
    private void setup()
    {
        hadoopConf = new Configuration();
        hadoopConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        listDirectoryOperation = path ->
        {
            FileSystem fs = path.getFileSystem(hadoopConf);
            return new HadoopFileInfoIterator(fs.listLocatedStatus(path));
        };
    }

    @AfterClass(alwaysRun = true)
    private void tearDown()
    {
        hadoopConf = null;
        listDirectoryOperation = null;
    }

    @Test
    public void testDefaultPathFilterNoRecursion()
            throws IOException
    {
        // set up
        File rootDir = createTempDir();
        String basePath = rootDir.getAbsolutePath();
        // create 8 files in root directory - 3 pathFilter matched and 5 non matched files.
        createFiles(basePath, 3, true);
        createFiles(basePath, 5, false);
        Path rootPath = new Path("file://" + basePath + File.separator);
        PathFilter pathFilter = path -> true;
        HiveFileIterator hiveFileIterator = new HiveFileIterator(rootPath, listDirectoryOperation, new NamenodeStats(), IGNORED, pathFilter);

        int actualCount = Iterators.size(hiveFileIterator);
        assertEquals(actualCount, 8);

        // cleanup
        deleteTestDir(rootDir);
    }

    @Test
    public void testDefaultPathFilterWithRecursion()
            throws IOException
    {
        // set up
        File rootDir = createTempDir();
        String basePath = rootDir.getAbsolutePath();
        // create 8 files in root directory - 3 pathFilter matched and 5 non matched files.
        createFiles(basePath, 3, true);
        createFiles(basePath, 5, false);
        // create two directories
        List<File> subDirs = createDirs(basePath, 2);
        // create 5 files in dir1 - 3 pathFilter matched and 2 non matched files.
        String dir1 = subDirs.get(0).getAbsolutePath();
        createFiles(dir1, 3, true);
        createFiles(dir1, 2, false);
        // create 7 files in dir2 - 3 pathFilter matched and 4 non matched files.
        String dir2 = subDirs.get(1).getAbsolutePath();
        createFiles(dir2, 3, true);
        createFiles(dir2, 4, false);
        Path rootPath = new Path("file://" + basePath + File.separator);
        PathFilter pathFilter = path -> true;
        HiveFileIterator hiveFileIterator = new HiveFileIterator(rootPath, listDirectoryOperation, new NamenodeStats(), RECURSE, pathFilter);

        int actualCount = Iterators.size(hiveFileIterator);
        assertEquals(actualCount, 20);

        // cleanup
        deleteTestDir(rootDir);
    }

    @Test
    public void testPathFilterWithNoRecursion()
            throws IOException
    {
        // set up
        File rootDir = createTempDir();
        String basePath = rootDir.getAbsolutePath();
        // create 8 files in root directory - 3 pathFilter matched and 5 non matched files.
        createFiles(basePath, 3, true);
        createFiles(basePath, 5, false);
        Path rootPath = new Path("file://" + basePath + File.separator);
        PathFilter pathFilter = path -> path.getName().contains(PATH_FILTER_MATCHED_PREFIX);
        HiveFileIterator hiveFileIterator = new HiveFileIterator(rootPath, listDirectoryOperation, new NamenodeStats(), IGNORED, pathFilter);

        int actualCount = Iterators.size(hiveFileIterator);
        assertEquals(actualCount, 3);

        // cleanup
        deleteTestDir(rootDir);
    }

    @Test
    public void testPathFilterWithRecursion()
            throws IOException
    {
        // set up
        File rootDir = createTempDir();
        String basePath = rootDir.getAbsolutePath();
        // create 8 files in root directory - 3 pathFilter matched and 5 non matched files.
        createFiles(basePath, 3, true);
        createFiles(basePath, 5, false);
        // create two directories
        List<File> subDirs = createDirs(basePath, 2);
        // create 5 files in dir1 - 3 pathFilter matched and 2 non matched files.
        String dir1 = subDirs.get(0).getAbsolutePath();
        createFiles(dir1, 3, true);
        createFiles(dir1, 2, false);
        // create 7 files in dir2 - 3 pathFilter matched and 4 non matched files.
        String dir2 = subDirs.get(1).getAbsolutePath();
        createFiles(dir2, 3, true);
        createFiles(dir2, 4, false);
        Path rootPath = new Path("file://" + basePath + File.separator);
        PathFilter pathFilter = path -> path.getName().contains(PATH_FILTER_MATCHED_PREFIX);
        HiveFileIterator hiveFileIterator = new HiveFileIterator(rootPath, listDirectoryOperation, new NamenodeStats(), RECURSE, pathFilter);

        int actualCount = Iterators.size(hiveFileIterator);
        assertEquals(actualCount, 9);

        // cleanup
        deleteTestDir(rootDir);
    }

    private void deleteTestDir(File rootDir)
            throws IOException
    {
        if (rootDir.exists()) {
            deleteRecursively(rootDir.toPath(), ALLOW_INSECURE);
        }
    }

    private void createFiles(String basePath, int numFiles, boolean matchPathFilter)
            throws IOException
    {
        new File(basePath).mkdirs();
        for (int i = 0; i < numFiles; i++) {
            String fileName;
            if (matchPathFilter) {
                fileName = PATH_FILTER_MATCHED_PREFIX + i;
            }
            else {
                fileName = PATH_FILTER_NOT_MATCHED_PREFIX + i;
            }
            new File(basePath + File.separator + fileName).createNewFile();
        }
    }

    private List<File> createDirs(String basePath, int numDirectories)
    {
        List<File> directories = new ArrayList<>();
        for (int i = 0; i < numDirectories; i++) {
            String dirName = basePath + File.separator + PATH_FILTER_MATCHED_PREFIX + "dir_" + i;
            File file = new File(dirName);
            file.mkdirs();
            directories.add(file);
        }
        return directories;
    }
}
