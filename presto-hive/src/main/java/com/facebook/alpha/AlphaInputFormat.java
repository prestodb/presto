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
package com.facebook.alpha;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Noop implementation of the Alpha Input Format for Hive.
 */
public class AlphaInputFormat
        extends FileInputFormat<NullWritable, NullWritable>
{
    AlphaInputFormat alphaInputFormat;

    @Override
    public RecordReader<NullWritable, NullWritable> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter)
    {
        return new RecordReader<NullWritable, NullWritable>()
        {
            @Override
            public boolean next(NullWritable key, NullWritable value)
                    throws IOException
            {
                return alphaInputFormat == null;
            }

            @Override
            public NullWritable createKey()
            {
                return null;
            }

            @Override
            public NullWritable createValue()
            {
                return null;
            }

            @Override
            public long getPos()
                    throws IOException
            {
                return 0;
            }

            @Override
            public void close()
                    throws IOException {}

            @Override
            public float getProgress()
                    throws IOException
            {
                return 0;
            }
        };
    }

    @Override
    protected boolean isSplitable(FileSystem fs, Path filename)
    {
        return true;
    }
}
