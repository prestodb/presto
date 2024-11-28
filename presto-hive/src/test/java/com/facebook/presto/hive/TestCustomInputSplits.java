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
package com.facebook.presto.hive;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HadoopExtendedFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test(singleThreaded = true)
public class TestCustomInputSplits
{
    private static final String SPLIT_MINSIZE = "mapreduce.input.fileinputformat.split.minsize";
    private static final String LOCAL_BLOCK_SIZE = "fs.local.block.size";
    private static final String DFS_BLOCK_SIZE = "dfs.blocksize";
    private Path filePath;
    private JobConf jobConf1;
    private FileSystem fs;
    @BeforeClass
    public void setUp()
            throws Exception
    {
        long splitSize = 1000;
        long blockSize = 500;
        String pathStr = Resources.getResource("addressbook.parquet").getPath();
        filePath = new Path(pathStr);

        jobConf1 = new JobConf();
        jobConf1.set("fs.defaultFS", "file:///");
        jobConf1.setClass("fs.file.impl", TestingFileSystem.class, FileSystem.class);
        jobConf1.setClass("fs.hdfs.impl", TestingFileSystem.class, FileSystem.class);
        jobConf1.setBoolean("fs.file.impl.disable.cache", true);
        jobConf1.setLong(LOCAL_BLOCK_SIZE, blockSize);
        jobConf1.setLong(DFS_BLOCK_SIZE, blockSize);
        jobConf1.setLong(SPLIT_MINSIZE, splitSize);

        FileSystem localFileSystem = new LocalFileSystem();
        localFileSystem.initialize(URI.create("file:///"), jobConf1);

        fs = new TestingFileSystem();
        fs.initialize(URI.create("file:///"), jobConf1);

        System.out.println("### Before setting input paths, FileSystem.get(jobConf1) = " + fs.getClass().getName());
        System.out.println("### fs is now initialized with: " + fs.getClass().getName());
    }

    @Test
    public void testCustomSplitSize() throws Exception
    {
        System.out.println("### Before setting input paths, FileSystem.get(jobConf1) = " + FileSystem.get(jobConf1).getClass().getName());
        FileInputFormat.setInputPaths(jobConf1, filePath);
        TextInputFormat targetInputFormat = new DummyTextInputFormat();
        targetInputFormat.configure(jobConf1);
        System.out.println("##### Before calling getSplits, FileSystem class = " + fs.getClass().getName());
        jobConf1.set("fs.hdfs.impl", TestingFileSystem.class.getName());
        jobConf1.set("fs.file.impl", TestingFileSystem.class.getName());
        jobConf1.setClass("fs.file.impl", TestingFileSystem.class, FileSystem.class);

        System.out.println("### Just before getSplits, FileSystem.get(jobConf1) = " + FileSystem.get(jobConf1).getClass().getName());

        InputSplit[] targetSplits = targetInputFormat.getSplits(jobConf1, 0);

        System.out.println("##### In testCustomSplitSize FileSystem1 = " + FileSystem.get(jobConf1).getClass().getName());
        System.out.println("##### In testCustomSplitSize FileSystem1 = " + fs.get(jobConf1));
        System.out.println("##### After getSplits, FileSystem class = " + fs.getClass().getName());
        System.out.println("##### Number of splits generated: " + targetSplits.length);

        assertNotNull(targetSplits);
        assertEquals(targetSplits.length, 4);
    }
    public class DummyTextInputFormat
            extends TextInputFormat
    {
        @Override
        public void configure(JobConf conf)
        {
            super.configure(conf);
            try {
                System.out.println("### fs.file.impl = " + conf.get("fs.file.impl"));
                System.out.println("### fs.hdfs.impl = " + conf.get("fs.hdfs.impl"));
                System.out.println("### fs.defaultFS = " + conf.get("fs.defaultFS"));
                Class<?> fileSystemClass = FileSystem.getFileSystemClass("file", conf);
                System.out.println("### FileSystem class resolved for 'file': " + fileSystemClass.getName());
                FileSystem fs = FileSystem.get(conf);
                System.out.println("### configure() FileSystem class: " + fs.getClass().getName());
                if (!(fs instanceof TestingFileSystem)) {
                    throw new IllegalStateException("Expected TestingFileSystem but found: " + fs.getClass().getName());
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        @Override
        protected boolean isSplitable(FileSystem fs, Path file)
        {
            System.out.println("### isSplitable() received FileSystem: " + fs.getClass().getName());
            if (!(fs instanceof TestingFileSystem)) {
                throw new IllegalStateException("Expected TestingFileSystem in isSplitable, but got: " + fs.getClass().getName());
            }
            return true;
        }
    }

    public static class TestingFileSystem
            extends HadoopExtendedFileSystem
    {
        public TestingFileSystem()
        {
            super(new LocalFileSystem());
            System.out.println("### TestingFileSystem no-argument constructor called");
        }

        @Override
        public void initialize(URI uri, Configuration conf)
                throws IOException
        {
            super.initialize(uri, conf);
            setConf(conf);
            System.out.println("### TestingFileSystem initialized with URI: " + uri + " and configuration: " + conf);
        }

        @Override
        public Configuration getConf()
        {
            System.out.println("### getConf called in TestingFileSystem");
            return super.getConf();
        }

        @Override
        public long getDefaultBlockSize()
        {
            System.out.println("### getDefaultBlockSize() called");
            return 500L; // Custom block size
        }

        @Override
        public long getDefaultBlockSize(Path path)
        {
            System.out.println("### getDefaultBlockSize(Path) called for: " + path);
            return 500L; // Custom block size for the specific path
        }
    }
}
