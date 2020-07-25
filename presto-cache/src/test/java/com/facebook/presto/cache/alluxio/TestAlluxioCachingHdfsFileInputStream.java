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
package com.facebook.presto.cache.alluxio;

import alluxio.client.file.FileInStream;
import com.google.common.base.VerifyException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Optional;

import static com.facebook.presto.cache.TestingCacheUtils.validateBuffer;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestAlluxioCachingHdfsFileInputStream
{
    @Test
    public void testConstructor()
    {
        FSDataInputStream dataTierInputStream = new TestFSDataInputStream(new byte[] {});
        FileInStream fileInStream = new TestFileInStream(new byte[] {});
        new AlluxioCachingHdfsFileInputStream(fileInStream, Optional.empty(), false);
        new AlluxioCachingHdfsFileInputStream(fileInStream, Optional.of(dataTierInputStream), false);
        new AlluxioCachingHdfsFileInputStream(fileInStream, Optional.of(dataTierInputStream), true);
        try {
            new AlluxioCachingHdfsFileInputStream(fileInStream, Optional.empty(), true);
            fail();
        }
        catch (VerifyException ex) {
            assertEquals(ex.getMessage(), "data tier input need to be non-null for data validation.");
        }
    }

    @Test
    public void testValidateDataEnabledWithDataMismatch()
            throws IOException
    {
        byte[] inputData = new byte[] {1, 2, 3};
        byte[] corruptedData = new byte[] {1, 3, 3};
        FSDataInputStream dataTierInputStream = new TestFSDataInputStream(inputData);
        FileInStream fileInStream = new TestFileInStream(corruptedData);
        AlluxioCachingHdfsFileInputStream fileInputStream = new AlluxioCachingHdfsFileInputStream(fileInStream, Optional.of(dataTierInputStream), true);
        byte[] buffer = new byte[3];
        try {
            fileInputStream.readFully(0, buffer, 0, buffer.length);
            fail("Data validation didn't work for mismatched data.");
        }
        catch (VerifyException ex) {
            assertEquals(ex.getMessage(), "corrupted buffer at position 1");
        }
    }

    @Test
    public void testValidateDataEnabledWithDataMatched()
            throws IOException
    {
        byte[] inputData = new byte[] {1, 2, 3};
        FSDataInputStream dataTierInputStream = new TestFSDataInputStream(inputData);
        FileInStream fileInStream = new TestFileInStream(inputData);
        AlluxioCachingHdfsFileInputStream fileInputStream = new AlluxioCachingHdfsFileInputStream(fileInStream, Optional.of(dataTierInputStream), true);
        byte[] buffer = new byte[3];
        fileInputStream.readFully(0, buffer, 0, buffer.length);
        validateBuffer(inputData, 0, buffer, 0, inputData.length);
    }

    @Test
    public void testValidateDataDisabledWithDataMismatch()
            throws IOException
    {
        byte[] inputData = new byte[] {1, 2, 3};
        byte[] corruptedData = new byte[] {1, 3, 3};
        FSDataInputStream dataTierInputStream = new TestFSDataInputStream(inputData);
        FileInStream fileInStream = new TestFileInStream(corruptedData);
        AlluxioCachingHdfsFileInputStream fileInputStream = new AlluxioCachingHdfsFileInputStream(fileInStream, Optional.of(dataTierInputStream), false);
        byte[] buffer = new byte[3];
        fileInputStream.readFully(0, buffer, 0, buffer.length);
        validateBuffer(corruptedData, 0, buffer, 0, corruptedData.length);
    }

    @Test
    public void testInteractionWithDataTierInputStream()
            throws IOException
    {
        byte[] inputData = new byte[] {1, 2, 3};
        FSDataInputStream dataTierInputStream = new TestFSDataInputStream(inputData);
        FileInStream fileInStream = new TestFileInStream(inputData);
        AlluxioCachingHdfsFileInputStream validationEnabledInputStream = new AlluxioCachingHdfsFileInputStream(fileInStream, Optional.of(dataTierInputStream), true);
        AlluxioCachingHdfsFileInputStream validationDisabledInputStream = new AlluxioCachingHdfsFileInputStream(fileInStream, Optional.of(dataTierInputStream), false);

        // Seek on multiple positions on validationDisabledInputStream, it should not affect data tier
        validationDisabledInputStream.seek(2L);
        assertEquals(dataTierInputStream.getPos(), 0L);
        assertEquals(fileInStream.getPos(), 2L);
        validationDisabledInputStream.seek(1L);
        assertEquals(dataTierInputStream.getPos(), 0L);
        assertEquals(fileInStream.getPos(), 1L);

        // Seek on multiple positions on validationEnabledInputStream, it should also seek for same position in data tier
        validationEnabledInputStream.seek(2L);
        assertEquals(dataTierInputStream.getPos(), 2L);
        assertEquals(fileInStream.getPos(), 2L);
        validationEnabledInputStream.seek(1L);
        assertEquals(dataTierInputStream.getPos(), 1L);
        assertEquals(fileInStream.getPos(), 1L);
    }

    private static class TestFileInStream
            extends FileInStream
    {
        private final byte[] data;
        private long position;

        private TestFileInStream(byte[] data)
        {
            this.data = data;
            position = 0;
        }

        @Override
        public void seek(long position)
        {
            this.position = position;
        }

        @Override
        public long getPos()
        {
            return position;
        }

        @Override
        public long remaining()
        {
            return data.length - position;
        }

        @Override
        public int positionedRead(long position, byte[] buffer, int offset, int length)
        {
            if (length == 0) {
                return 0;
            }
            else if (position >= 0L && position < data.length) {
                int lengthToRead = Math.min(length, data.length - (int) position);
                System.arraycopy(data, (int) position, buffer, offset, lengthToRead);
                return lengthToRead;
            }
            return -1;
        }

        @Override
        public int read()
        {
            if (position >= data.length) {
                return -1;
            }
            return data[(int) position++];
        }
    }

    private static class TestFSDataInputStream
            extends FSDataInputStream
    {
        TestFSDataInputStream(byte[] data)
        {
            super(new ByteArraySeekableStream(data));
        }
    }
}
