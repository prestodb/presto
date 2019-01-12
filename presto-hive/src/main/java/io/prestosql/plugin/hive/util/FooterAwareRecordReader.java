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
package io.prestosql.plugin.hive.util;

import org.apache.hadoop.hive.ql.exec.FooterBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class FooterAwareRecordReader<K extends WritableComparable, V extends Writable>
        implements RecordReader<K, V>
{
    private final RecordReader<K, V> delegate;
    private final JobConf job;
    private final FooterBuffer footerBuffer = new FooterBuffer();

    public FooterAwareRecordReader(RecordReader<K, V> delegate, int footerCount, JobConf job)
            throws IOException
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.job = requireNonNull(job, "job is null");

        checkArgument(footerCount > 0, "footerCount is expected to be positive");

        footerBuffer.initializeBuffer(job, delegate, footerCount, delegate.createKey(), delegate.createValue());
    }

    @Override
    public boolean next(K key, V value)
            throws IOException
    {
        return footerBuffer.updateBuffer(job, delegate, key, value);
    }

    @Override
    public K createKey()
    {
        return delegate.createKey();
    }

    @Override
    public V createValue()
    {
        return delegate.createValue();
    }

    @Override
    public long getPos()
            throws IOException
    {
        return delegate.getPos();
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }

    @Override
    public float getProgress()
            throws IOException
    {
        return delegate.getProgress();
    }
}
