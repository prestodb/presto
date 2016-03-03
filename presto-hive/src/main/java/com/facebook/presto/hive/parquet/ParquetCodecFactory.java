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
package com.facebook.presto.hive.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.ReflectionUtils;
import parquet.bytes.BytesInput;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class ParquetCodecFactory
{
    private final Map<CompressionCodecName, BytesCompressor> compressors = new HashMap<>();
    private final Map<CompressionCodecName, BytesDecompressor> decompressors = new HashMap<>();
    private final Map<String, CompressionCodec> codecByName = new HashMap<>();
    private final Configuration configuration;

    public ParquetCodecFactory(Configuration configuration)
    {
        this.configuration = configuration;
    }

    private CompressionCodec getCodec(CompressionCodecName codecName)
    {
        String codecClassName = codecName.getHadoopCompressionCodecClassName();
        if (codecClassName == null) {
            return null;
        }
        CompressionCodec codec = codecByName.get(codecClassName);
        if (codec != null) {
            return codec;
        }

        try {
            Class<?> codecClass = Class.forName(codecClassName);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, configuration);
            codecByName.put(codecClassName, codec);
            return codec;
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Class " + codecClassName + " was not found", e);
        }
    }

    public BytesCompressor getCompressor(CompressionCodecName codecName, int pageSize)
    {
        BytesCompressor compressor = compressors.get(codecName);
        if (compressor == null) {
            CompressionCodec codec = getCodec(codecName);
            compressor = new BytesCompressor(codecName, codec, pageSize);
            compressors.put(codecName, compressor);
        }
        return compressor;
    }

    public BytesDecompressor getDecompressor(CompressionCodecName codecName)
    {
        BytesDecompressor decompressor = decompressors.get(codecName);
        if (decompressor == null) {
            CompressionCodec codec = getCodec(codecName);
            decompressor = new BytesDecompressor(codec);
            decompressors.put(codecName, decompressor);
        }
        return decompressor;
    }

    public void release()
    {
        for (BytesCompressor compressor : compressors.values()) {
            compressor.release();
        }
        compressors.clear();
        for (BytesDecompressor decompressor : decompressors.values()) {
            decompressor.release();
        }
        decompressors.clear();
    }

    public class BytesDecompressor
    {
        private final CompressionCodec codec;
        private final Decompressor decompressor;

        public BytesDecompressor(CompressionCodec codec)
        {
            this.codec = codec;
            if (codec != null) {
                decompressor = CodecPool.getDecompressor(codec);
            }
            else {
                decompressor = null;
            }
        }

        public BytesInput decompress(BytesInput bytes, int uncompressedSize)
                throws IOException
        {
            BytesInput decompressed;
            if (codec != null) {
                decompressor.reset();
                InputStream inputStream = codec.createInputStream(new ByteArrayInputStream(bytes.toByteArray()), decompressor);
                decompressed = BytesInput.from(inputStream, uncompressedSize);
            }
            else {
                decompressed = bytes;
            }
            return decompressed;
        }

        private void release()
        {
            if (decompressor != null) {
                CodecPool.returnDecompressor(decompressor);
            }
        }
    }

    public static class BytesCompressor
    {
        private final CompressionCodec codec;
        private final Compressor compressor;
        private final ByteArrayOutputStream compressedOutBuffer;
        private final CompressionCodecName codecName;

        public BytesCompressor(CompressionCodecName codecName, CompressionCodec codec, int pageSize)
        {
            this.codecName = codecName;
            this.codec = codec;
            if (codec != null) {
                compressor = CodecPool.getCompressor(codec);
                compressedOutBuffer = new ByteArrayOutputStream(pageSize);
            }
            else {
                compressor = null;
                compressedOutBuffer = null;
            }
        }

        public BytesInput compress(BytesInput bytes)
                throws IOException
        {
            final BytesInput compressedBytes;
            if (codec == null) {
                compressedBytes = bytes;
            }
            else {
                compressedOutBuffer.reset();
                if (compressor != null) {
                    compressor.reset();
                }
                CompressionOutputStream outputStream = codec.createOutputStream(compressedOutBuffer, compressor);
                bytes.writeAllTo(outputStream);
                outputStream.finish();
                outputStream.close();
                compressedBytes = BytesInput.from(compressedOutBuffer);
            }
            return compressedBytes;
        }

        private void release()
        {
            if (compressor != null) {
                CodecPool.returnCompressor(compressor);
            }
        }

        public CompressionCodecName getCodecName()
        {
            return codecName;
        }
    }
}
