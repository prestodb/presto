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
package com.facebook.presto.server.security.oauth2;

import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import io.jsonwebtoken.CompressionCodec;
import io.jsonwebtoken.CompressionException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import static java.lang.Math.toIntExact;
import static java.util.Arrays.copyOfRange;

public class ZstdCodec
        implements CompressionCodec
{
    public static final String CODEC_NAME = "ZSTD";

    @Override
    public String getId()
    {
        return CODEC_NAME;
    }

    @Override
    public String getAlgorithmName()
    {
        return CODEC_NAME;
    }

    @Override
    public byte[] compress(byte[] bytes)
            throws CompressionException
    {
        ZstdCompressor compressor = new ZstdCompressor();
        byte[] compressed = new byte[compressor.maxCompressedLength(bytes.length)];
        int outputSize = compressor.compress(bytes, 0, bytes.length, compressed, 0, compressed.length);
        return copyOfRange(compressed, 0, outputSize);
    }

    @Override
    public byte[] decompress(byte[] bytes)
            throws CompressionException
    {
        byte[] output = new byte[toIntExact(ZstdDecompressor.getDecompressedSize(bytes, 0, bytes.length))];
        new ZstdDecompressor().decompress(bytes, 0, bytes.length, output, 0, output.length);
        return output;
    }

    @Override
    public OutputStream compress(OutputStream out)
            throws CompressionException
    {
        return new ByteArrayOutputStream()
        {
            @Override
            public void close()
            {
                try {
                    byte[] bytes = toByteArray();
                    out.write(compress(bytes));
                    out.close();
                }
                catch (Exception e) {
                    throw new CompressionException("Unable to compress", e);
                }
            }
        };
    }

    @Override
    public InputStream decompress(InputStream input)
            throws CompressionException
    {
        try {
            byte[] bytes = input.readAllBytes();
            return new ByteArrayInputStream(decompress(bytes));
        }
        catch (Exception e) {
            throw new CompressionException("Unable to decompress", e);
        }
    }
}
