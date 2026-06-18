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

import com.facebook.presto.orc.metadata.CompressionKind;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.util.Optional;
import java.util.function.Predicate;

import static com.facebook.presto.hive.HiveStorageFormat.DWRF;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveStorageFormat.PAGEFILE;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static java.util.Objects.requireNonNull;

public enum HiveCompressionCodec
{
    NONE(null, CompressionKind.NONE, CompressionCodecName.UNCOMPRESSED, f -> true),
    SNAPPY(SnappyCodec.class, CompressionKind.SNAPPY, CompressionCodecName.SNAPPY, f -> true),
    GZIP(GzipCodec.class, CompressionKind.ZLIB, CompressionCodecName.GZIP, f -> true),
    LZ4(Lz4Codec.class, CompressionKind.LZ4, CompressionCodecName.UNCOMPRESSED, f -> f == PAGEFILE || f == ORC),
    ZSTD(null, CompressionKind.ZSTD, CompressionCodecName.ZSTD, f -> f == ORC || f == DWRF || f == PAGEFILE || f == PARQUET);

    private final Optional<Class<? extends CompressionCodec>> codec;
    private final CompressionKind orcCompressionKind;
    private final CompressionCodecName parquetCompressionCodec;
    private final Predicate<HiveStorageFormat> supportedStorageFormats;

    HiveCompressionCodec(
            Class<? extends CompressionCodec> codec,
            CompressionKind orcCompressionKind,
            CompressionCodecName parquetCompressionCodec,
            Predicate<HiveStorageFormat> supportedStorageFormats)
    {
        this.codec = Optional.ofNullable(codec);
        this.orcCompressionKind = requireNonNull(orcCompressionKind, "orcCompressionKind is null");
        this.parquetCompressionCodec = requireNonNull(parquetCompressionCodec, "parquetCompressionCodec is null");
        this.supportedStorageFormats = requireNonNull(supportedStorageFormats, "supportedStorageFormats is null");
    }

    public Optional<Class<? extends CompressionCodec>> getCodec()
    {
        return codec;
    }

    public CompressionKind getOrcCompressionKind()
    {
        return orcCompressionKind;
    }

    public CompressionCodecName getParquetCompressionCodec()
    {
        return parquetCompressionCodec;
    }

    public boolean isSupportedStorageFormat(HiveStorageFormat hiveStorageFormat)
    {
        return supportedStorageFormats.test(hiveStorageFormat);
    }
}
