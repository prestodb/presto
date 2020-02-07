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
package com.facebook.presto.druid.segment;

import com.facebook.presto.spi.PrestoException;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_SEGMENT_LOAD_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SmooshedColumnSource
        implements SegmentColumnSource
{
    private static final String FILE_EXTENSION = "smoosh";
    private static final String SMOOSH_METADATA_FILE_NAME = makeMetaFileName();
    private static final String VERSION_FILE_NAME = "version.bin";

    private final IndexFileSource indexFileSource;
    private final Map<String, SmooshFileMetadata> columnSmoosh = new HashMap<>();

    public SmooshedColumnSource(IndexFileSource indexFileSource)
    {
        this.indexFileSource = requireNonNull(indexFileSource, "indexFileSource is null");
        loadSmooshFileMetadata();
    }

    @Override
    public int getVersion()
    {
        try {
            return ByteBuffer.wrap(indexFileSource.readFile(VERSION_FILE_NAME)).getInt();
        }
        catch (IOException e) {
            throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, e);
        }
    }

    @Override
    public byte[] getColumnData(String name)
    {
        SmooshFileMetadata metadata = columnSmoosh.get(name);
        if (metadata == null) {
            throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, format("Internal file %s doesn't exist", name));
        }
        String fileName = makeChunkFileName(metadata.getFileCount());
        int fileStart = metadata.getStartOffset();
        int fileSize = metadata.getEndOffset() - fileStart;

        byte[] buffer = new byte[fileSize];
        try {
            indexFileSource.readFile(fileName, fileStart, buffer);
        }
        catch (IOException e) {
            throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, e);
        }
        return buffer;
    }

    private void loadSmooshFileMetadata()
    {
        try {
            byte[] metadata = indexFileSource.readFile(SMOOSH_METADATA_FILE_NAME);
            BufferedReader in = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(metadata)));
            String line = in.readLine();
            if (line == null) {
                throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, format("Malformed metadata file: first line should be version,maxChunkSize,numChunks, got null."));
            }

            String[] splits = line.split(",");
            if (!"v1".equals(splits[0])) {
                throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, format("Malformed metadata file: unknown version[%s], v1 is all I know.", splits[0]));
            }
            if (splits.length != 3) {
                throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, format("Malformed metadata file: wrong number of splits[%d] in line[%s]", splits.length, line));
            }

            while (true) {
                line = in.readLine();
                if (line == null) {
                    break;
                }
                splits = line.split(",");

                if (splits.length != 4) {
                    throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, format("Malformed metadata file: wrong number of splits[%d] in line[%s]", splits.length, line));
                }
                columnSmoosh.put(splits[0], new SmooshFileMetadata(Integer.parseInt(splits[1]), Integer.parseInt(splits[2]), Integer.parseInt(splits[3])));
            }
        }
        catch (IOException e) {
            throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, e);
        }
    }

    private static String makeMetaFileName()
    {
        return format("meta.%s", FILE_EXTENSION);
    }

    private static String makeChunkFileName(int i)
    {
        return format("%05d.%s", i, FILE_EXTENSION);
    }
}
