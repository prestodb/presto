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
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.util.Optional;
import java.util.function.Predicate;

import static com.facebook.presto.hive.HiveStorageFormat.DWRF;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveStorageFormat.PAGEFILE;
import static java.util.Objects.requireNonNull;

public enum HiveCompressionCodec
{
    NONE(null, CompressionKind.NONE, CompressionCodecName.UNCOMPRESSED, f -> true),
    SNAPPY(SnappyCodec.class, CompressionKind.SNAPPY, CompressionCodecName.SNAPPY, f -> true),
    GZIP(GzipCodec.class, CompressionKind.ZLIB, CompressionCodecName.GZIP, f -> true),
    LZ4(null, CompressionKind.NONE, null, f -> f == PAGEFILE),
    ZSTD(null, CompressionKind.ZSTD, null, f -> f == ORC || f == DWRF || f == PAGEFILE);

    private final Optional<Class<? extends CompressionCodec>> codec;
    private final CompressionKind orcCompressionKind;
    private final Optional<CompressionCodecName> parquetCompressionCodec;
    private final Predicate<HiveStorageFormat> supportedStorageFormats;

    HiveCompressionCodec(
            Class<? extends CompressionCodec> codec,
            CompressionKind orcCompressionKind,
            CompressionCodecName parquetCompressionCodec,
            Predicate<HiveStorageFormat> supportedStorageFormats)
    {
        this.codec = Optional.ofNullable(codec);
        this.orcCompressionKind = requireNonNull(orcCompressionKind, "orcCompressionKind is null");
        this.parquetCompressionCodec = Optional.ofNullable(parquetCompressionCodec);
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

    public Optional<CompressionCodecName> getParquetCompressionCodec()
    {
        return parquetCompressionCodec;
    }

    public boolean isSupportedStorageFormat(HiveStorageFormat hiveStorageFormat)
    {
        return supportedStorageFormats.test(hiveStorageFormat);
    }
}
