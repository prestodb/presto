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

import com.facebook.presto.hive.NamenodeStats;
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
import java.util.Optional;
import java.util.Random;

import static com.facebook.presto.hive.HadoopDirectoryLister.HadoopFileInfoIterator;
import static com.facebook.presto.hive.util.HiveFileIterator.NestedDirectoryPolicy.IGNORED;
import static com.facebook.presto.hive.util.HiveFileIterator.NestedDirectoryPolicy.RECURSE;
import static com.google.common.io.Files.createTempDir;
import static org.testng.Assert.assertEquals;

public class TestHiveFileIterator
{
    private NamenodeStats namenodeStats;
    private PathFilter pathFilter;
    private Path rootPath;
    private File rootDir;
    private HiveFileIterator.ListDirectoryOperation listDirectoryOperation;
    private HiveFileIterator hiveFileIterator;
    private String pathFilterString = "path_filter_test_file_";
    private int fileCountForIgnoredPolicyPathFilterOn;
    private int fileCountForRecursePolicyPathFilterOn;
    private int fileCountForIgnoredPolicyPathFilterOff;
    private int fileCountForRecursePolicyPathFilterOff;
    private Configuration hadoopConf;
    private Random random;
    private static final String RANDOM_FILE_NAME_SALT_STRING = "abcdefghijklmnopqrstuvwxyz";

    @AfterClass
    private void cleanup()
    {
        if (rootDir != null && rootDir.exists()) {
            delete(rootDir);
        }
    }

    private void delete(File file)
    {
        if (!file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                delete(child);
            }
        }
        file.delete();
    }

    @BeforeClass
    private void setup() throws IOException
    {
        namenodeStats = new NamenodeStats();
        pathFilter = path -> path.getName().contains(pathFilterString);
        rootDir = createTempDir();
        String basePath = rootDir.getAbsolutePath();
        rootPath = new Path("file://" + basePath + File.separator);
        random = new Random();

        // create few files in rootDir to match path filter criteria
        createFiles(basePath, 5, false, pathFilterString);
        updateExpectedStats(IGNORED, true, 5);
        updateExpectedStats(IGNORED, false, 5);
        updateExpectedStats(RECURSE, true, 5);
        updateExpectedStats(RECURSE, false, 5);

        // create more files that will fail path filter criteria
        createFiles(basePath, 5, true, null);
        updateExpectedStats(IGNORED, false, 5);
        updateExpectedStats(RECURSE, false, 5);

        // create sub directories
        List<File> dirs = createDirs(basePath, 2);

        // create files in each subdir along with couple files and one nested directory
        for (int i = 0; i < dirs.size(); i++) {
            if (i % 2 == 0) {
                createFiles(dirs.get(i).getAbsolutePath(), 5, true, null);
                updateExpectedStats(RECURSE, false, 5);
                List<File> subDirs = createDirs(dirs.get(i).getAbsolutePath(), 1);
                createFiles(subDirs.get(0).getAbsolutePath(), 5, false, pathFilterString);
                updateExpectedStats(RECURSE, true, 5);
                updateExpectedStats(RECURSE, false, 5);
            }
            else {
                createFiles(dirs.get(i).getAbsolutePath(), 5, false, pathFilterString);
                updateExpectedStats(RECURSE, true, 5);
                updateExpectedStats(RECURSE, false, 5);
                List<File> subDirs = createDirs(dirs.get(i).getAbsolutePath(), 1);
                createFiles(subDirs.get(0).getAbsolutePath(), 5, true, null);
                updateExpectedStats(RECURSE, false, 5);
            }
        }
        hadoopConf = new Configuration();
        hadoopConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        listDirectoryOperation = path ->
        {
            FileSystem fs = path.getFileSystem(hadoopConf);
            return new HadoopFileInfoIterator(fs.listLocatedStatus(path));
        };
    }

    private void createFiles(String basePath, int numFiles, boolean randomName, String fileNamePrefix) throws IOException
    {
        for (int i = 0; i < numFiles; i++) {
            new File(basePath).mkdirs();
            String fileName;
            if (randomName) {
                fileName = getRandomStringOfLen(10);
            }
            else {
                fileName = fileNamePrefix + "_" + i;
            }
            new File(basePath + File.separator + fileName).createNewFile();
        }
    }

    private List<File> createDirs(String basePath, int numDirectories)
    {
        List<File> directories = new ArrayList<>();
        for (int i = 0; i < numDirectories; i++) {
            String dirName = basePath + File.separator + pathFilterString + "_" + getRandomStringOfLen(5);
            File file = new File(dirName);
            file.mkdirs();
            directories.add(file);
        }
        return directories;
    }

    private String getRandomStringOfLen(int len)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append(RANDOM_FILE_NAME_SALT_STRING.charAt(random.nextInt(RANDOM_FILE_NAME_SALT_STRING.length())));
        }
        return sb.toString();
    }

    private void updateExpectedStats(HiveFileIterator.NestedDirectoryPolicy policy, boolean hasPathFilter, int expectedCount)
    {
        switch (policy) {
            case IGNORED:
                if (hasPathFilter) {
                    fileCountForIgnoredPolicyPathFilterOn += expectedCount;
                }
                else {
                    fileCountForIgnoredPolicyPathFilterOff += expectedCount;
                }
                break;
            case RECURSE:
                if (hasPathFilter) {
                    fileCountForRecursePolicyPathFilterOn += expectedCount;
                }
                else {
                    fileCountForRecursePolicyPathFilterOff += expectedCount;
                }
                break;
        }
    }

    public int getFileCount(HiveFileIterator hiveFileIterator)
    {
        int actualCount = 0;
        while (hiveFileIterator.hasNext()) {
            hiveFileIterator.next();
            actualCount++;
        }
        return actualCount;
    }

    @Test
    public void testNoPathFilterNoRecursion()
    {
        hiveFileIterator = new HiveFileIterator(rootPath, listDirectoryOperation, namenodeStats, IGNORED, Optional.empty());
        int actualCount = getFileCount(hiveFileIterator);
        assertEquals(actualCount, fileCountForIgnoredPolicyPathFilterOff);
    }

    @Test
    public void testNoPathFilterWithRecursion()
    {
        hiveFileIterator = new HiveFileIterator(rootPath, listDirectoryOperation, namenodeStats, RECURSE, Optional.empty());
        int actualCount = getFileCount(hiveFileIterator);
        assertEquals(actualCount, fileCountForRecursePolicyPathFilterOff);
    }

    @Test
    public void testPathFilterWithNoRecursion()
    {
        hiveFileIterator = new HiveFileIterator(rootPath, listDirectoryOperation, namenodeStats, IGNORED, Optional.of(pathFilter));
        int actualCount = getFileCount(hiveFileIterator);
        assertEquals(actualCount, fileCountForIgnoredPolicyPathFilterOn);
    }

    @Test
    public void testPathFilterWithRecursion()
    {
        hiveFileIterator = new HiveFileIterator(rootPath, listDirectoryOperation, namenodeStats, RECURSE, Optional.of(pathFilter));
        int actualCount = getFileCount(hiveFileIterator);
        assertEquals(actualCount, fileCountForRecursePolicyPathFilterOn);
    }
}
