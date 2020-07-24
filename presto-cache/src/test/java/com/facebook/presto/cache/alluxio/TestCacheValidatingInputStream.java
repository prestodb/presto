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

import com.google.common.base.VerifyException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.cache.TestingCacheUtils.validateBuffer;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestCacheValidatingInputStream
{
    @Test
    public void testValidateDataEnabledWithDataMismatch()
            throws IOException
    {
        byte[] inputData = new byte[] {1, 2, 3};
        byte[] corruptedData = new byte[] {1, 3, 3};
        FSDataInputStream dataTierInputStream = new TestFSDataInputStream(inputData);
        FSDataInputStream fileInStream = new TestFSDataInputStream(corruptedData);
        CacheValidatingInputStream fileInputStream =
                new CacheValidatingInputStream(fileInStream, dataTierInputStream);
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
        FSDataInputStream fileInStream = new TestFSDataInputStream(inputData);
        CacheValidatingInputStream fileInputStream = new CacheValidatingInputStream(fileInStream, dataTierInputStream);
        byte[] buffer = new byte[3];
        fileInputStream.readFully(0, buffer, 0, buffer.length);
        validateBuffer(inputData, 0, buffer, 0, inputData.length);
    }

    @Test
    public void testInteractionWithDataTierInputStream()
            throws IOException
    {
        byte[] inputData = new byte[] {1, 2, 3};
        FSDataInputStream dataTierInputStream = new TestFSDataInputStream(inputData);
        FSDataInputStream fileInStream = new TestFSDataInputStream(inputData);
        CacheValidatingInputStream validationEnabledInputStream = new CacheValidatingInputStream(fileInStream, dataTierInputStream);

        // Seek on multiple positions on validationEnabledInputStream, it should also seek for same position in data tier
        validationEnabledInputStream.seek(2L);
        assertEquals(dataTierInputStream.getPos(), 2L);
        assertEquals(fileInStream.getPos(), 2L);
        validationEnabledInputStream.seek(1L);
        assertEquals(dataTierInputStream.getPos(), 1L);
        assertEquals(fileInStream.getPos(), 1L);
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
