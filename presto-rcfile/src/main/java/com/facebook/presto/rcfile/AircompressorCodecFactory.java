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
package com.facebook.presto.rcfile;

import io.airlift.compress.lz4.Lz4Codec;
import io.airlift.compress.lzo.LzoCodec;
import io.airlift.compress.snappy.SnappyCodec;

import static java.util.Objects.requireNonNull;

public class AircompressorCodecFactory
        implements RcFileCodecFactory
{
    private static final String SNAPPY_CODEC_NAME = "org.apache.hadoop.io.compress.SnappyCodec";
    private static final String LZO_CODEC_NAME = "com.hadoop.compression.lzo.LzoCodec";
    private static final String LZO_CODEC_NAME_DEPRECATED = "org.apache.hadoop.io.compress.LzoCodec";
    private static final String LZ4_CODEC_NAME = "org.apache.hadoop.io.compress.Lz4Codec";
    private static final String LZ4_HC_CODEC_NAME = "org.apache.hadoop.io.compress.Lz4Codec";

    private final RcFileCodecFactory delegate;

    public AircompressorCodecFactory(RcFileCodecFactory delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public RcFileCompressor createCompressor(String codecName)
    {
        if (SNAPPY_CODEC_NAME.equals(codecName)) {
            return new AircompressorCompressor(new SnappyCodec());
        }
        if (LZO_CODEC_NAME.equals(codecName) || LZO_CODEC_NAME_DEPRECATED.equals(codecName)) {
            return new AircompressorCompressor(new LzoCodec());
        }
        if (LZ4_CODEC_NAME.equals(codecName)) {
            return new AircompressorCompressor(new Lz4Codec());
        }
        return delegate.createCompressor(codecName);
    }

    @Override
    public RcFileDecompressor createDecompressor(String codecName)
    {
        if (SNAPPY_CODEC_NAME.equals(codecName)) {
            return new AircompressorDecompressor(new SnappyCodec());
        }
        if (LZO_CODEC_NAME.equals(codecName) || LZO_CODEC_NAME_DEPRECATED.equals(codecName)) {
            return new AircompressorDecompressor(new LzoCodec());
        }
        if (LZ4_CODEC_NAME.equals(codecName) || LZ4_HC_CODEC_NAME.equals(codecName)) {
            return new AircompressorDecompressor(new Lz4Codec());
        }
        return delegate.createDecompressor(codecName);
    }
}
