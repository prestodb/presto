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
package com.facebook.presto.orc.metadata;

import com.facebook.presto.orc.DwrfEncryptionProvider;
import com.facebook.presto.orc.DwrfKeyProvider;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.OrcDecompressor;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;
import com.facebook.presto.orc.protobuf.InvalidProtocolBufferException;
import com.google.common.collect.ImmutableList;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.orc.metadata.PostScript.HiveWriterVersion.ORIGINAL;
import static com.google.common.base.Preconditions.checkState;
import static org.testng.Assert.expectThrows;

public class TestExceptionWrappingMetadataReader
{
    private static final OrcDataSourceId ORC_DATA_SOURCE_ID = new OrcDataSourceId("test");

    @Test
    public void testReadPostScriptExceptionHandling()
    {
        byte[] data = new byte[10];
        TestMetadataReader delegate = new TestMetadataReader();
        ExceptionWrappingMetadataReader metadataReader = new ExceptionWrappingMetadataReader(ORC_DATA_SOURCE_ID, delegate);
        assertExceptions(() -> metadataReader.readPostScript(data, 0, 1));
        delegate.assertAllExceptionsThrown();
    }

    @Test
    public void testReadMetadataExceptionHandling()
    {
        TestMetadataReader delegate = new TestMetadataReader();
        ExceptionWrappingMetadataReader metadataReader = new ExceptionWrappingMetadataReader(ORC_DATA_SOURCE_ID, delegate);
        ThrowingRunnable lambda = () -> metadataReader.readMetadata(ORIGINAL, null);
        assertExceptions(lambda);
        delegate.assertAllExceptionsThrown();
    }

    @Test
    public void testReadFooterExceptionHandling()
    {
        TestMetadataReader delegate = new TestMetadataReader();
        ExceptionWrappingMetadataReader metadataReader = new ExceptionWrappingMetadataReader(ORC_DATA_SOURCE_ID, delegate);
        ThrowingRunnable lambda = () -> metadataReader.readFooter(
                ORIGINAL,
                null,
                null,
                null,
                null,
                null);
        assertExceptions(lambda);
        delegate.assertAllExceptionsThrown();
    }

    @Test
    public void testReadStripeFooterExceptionHandling()
    {
        TestMetadataReader delegate = new TestMetadataReader();
        ExceptionWrappingMetadataReader metadataReader = new ExceptionWrappingMetadataReader(ORC_DATA_SOURCE_ID, delegate);
        ThrowingRunnable lambda = () -> metadataReader.readStripeFooter(ORC_DATA_SOURCE_ID, null, null);
        assertExceptions(lambda);
        delegate.assertAllExceptionsThrown();
    }

    @Test
    public void testReadRowIndexesExceptionHandling()
    {
        TestMetadataReader delegate = new TestMetadataReader();
        ExceptionWrappingMetadataReader metadataReader = new ExceptionWrappingMetadataReader(ORC_DATA_SOURCE_ID, delegate);
        ThrowingRunnable lambda = () -> metadataReader.readRowIndexes(ORIGINAL, null, null);
        assertExceptions(lambda);
        delegate.assertAllExceptionsThrown();
    }

    @Test
    public void testReadBloomFilterIndexesExceptionHandling()
    {
        TestMetadataReader delegate = new TestMetadataReader();
        ExceptionWrappingMetadataReader metadataReader = new ExceptionWrappingMetadataReader(ORC_DATA_SOURCE_ID, delegate);
        ThrowingRunnable lambda = () -> metadataReader.readBloomFilterIndexes(null);
        assertExceptions(lambda);
        delegate.assertAllExceptionsThrown();
    }

    private static void assertExceptions(ThrowingRunnable lambda)
    {
        expectThrows(RuntimeException.class, lambda);
        expectThrows(IOException.class, lambda);
        expectThrows(OrcCorruptionException.class, lambda);
    }

    private static class TestMetadataReader
            implements MetadataReader
    {
        private static final List<Exception> EXCEPTIONS = ImmutableList.of(
                new RuntimeException("test runtime exception"),
                new IOException("test io exception"),
                new InvalidProtocolBufferException("test protobuf exception"));
        private int currentExceptionIndex;

        private void throwNextException()
                throws IOException
        {
            checkState(currentExceptionIndex < EXCEPTIONS.size());
            Exception ex = EXCEPTIONS.get(currentExceptionIndex++);
            if (ex instanceof IOException) {
                throw (IOException) ex;
            }
            throw (RuntimeException) ex;
        }

        public void assertAllExceptionsThrown()
        {
            checkState(currentExceptionIndex == EXCEPTIONS.size());
        }

        @Override
        public PostScript readPostScript(byte[] data, int offset, int length)
                throws IOException
        {
            throwNextException();
            return null;
        }

        @Override
        public Metadata readMetadata(PostScript.HiveWriterVersion hiveWriterVersion, InputStream inputStream)
                throws IOException
        {
            throwNextException();
            return null;
        }

        @Override
        public Footer readFooter(
                PostScript.HiveWriterVersion hiveWriterVersion,
                InputStream inputStream,
                DwrfEncryptionProvider dwrfEncryptionProvider,
                DwrfKeyProvider dwrfKeyProvider,
                OrcDataSource orcDataSource,
                Optional<OrcDecompressor> decompressor)
                throws IOException
        {
            throwNextException();
            return null;
        }

        @Override
        public StripeFooter readStripeFooter(OrcDataSourceId orcDataSourceId, List<OrcType> types, InputStream inputStream)
                throws IOException
        {
            throwNextException();
            return null;
        }

        @Override
        public List<RowGroupIndex> readRowIndexes(PostScript.HiveWriterVersion hiveWriterVersion, InputStream inputStream, List<HiveBloomFilter> bloomFilters)
                throws IOException
        {
            throwNextException();
            return null;
        }

        @Override
        public List<HiveBloomFilter> readBloomFilterIndexes(InputStream inputStream)
                throws IOException
        {
            throwNextException();
            return null;
        }
    }
}
