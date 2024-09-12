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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HadoopExtendedFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test(singleThreaded = true)
public class TestCustomInputSplits
{
    private static final String SPLIT_MINSIZE = "mapreduce.input.fileinputformat.split.minsize";
    private static final String LOCAL_BLOCK_SIZE = "fs.local.block.size";
    private static final String DFS_BLOCK_SIZE = "dfs.blocksize";

    @Test
    public void testCustomSplitSize() throws Exception
    {
        long splitSize = 1000;
        long blockSize = 500;
        String filePath = Resources.getResource("addressbook.parquet").getFile();
        Path path = new Path(filePath);

        TextInputFormat targetInputFormat = new DummyTextInputFormat();
        JobConf jobConf = new JobConf();
        FileInputFormat.setInputPaths(jobConf, path);

        jobConf.set("fs.defaultFS", "file:///");
        jobConf.setClass("fs.file.impl", TestingFileSystem.class, FileSystem.class);
        jobConf.setClass("fs.hdfs.impl", TestingFileSystem.class, FileSystem.class);
        jobConf.setBoolean("fs.file.impl.disable.cache", true);
        jobConf.setLong(LOCAL_BLOCK_SIZE, blockSize);
        jobConf.setLong(DFS_BLOCK_SIZE, blockSize);
        jobConf.set(SPLIT_MINSIZE, Long.toString(splitSize));

        InputSplit[] targetSplits = targetInputFormat.getSplits(jobConf, 0);

        assertNotNull(targetSplits);
        assertEquals(targetSplits.length, 4);
    }

    public class DummyTextInputFormat
            extends TextInputFormat
    {
        protected boolean isSplitable(FileSystem fs, Path file)
        {
            return true;
        }
    }

    public static class TestingFileSystem
            extends HadoopExtendedFileSystem
    {
        public TestingFileSystem()
        {
            super(new LocalFileSystem());
        }
    }
}
