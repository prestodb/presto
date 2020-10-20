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
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcDecompressor;
import com.facebook.presto.orc.metadata.PostScript.HiveWriterVersion;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

public interface MetadataReader
{
    PostScript readPostScript(byte[] data, int offset, int length)
            throws IOException;

    Metadata readMetadata(HiveWriterVersion hiveWriterVersion, InputStream inputStream)
            throws IOException;

    Footer readFooter(HiveWriterVersion hiveWriterVersion,
            InputStream inputStream,
            DwrfEncryptionProvider dwrfEncryptionProvider,
            DwrfKeyProvider dwrfKeyProvider,
            OrcDataSource orcDataSource,
            Optional<OrcDecompressor> decompressor)
            throws IOException;

    StripeFooter readStripeFooter(List<OrcType> types, InputStream inputStream)
            throws IOException;

    List<RowGroupIndex> readRowIndexes(HiveWriterVersion hiveWriterVersion, InputStream inputStream)
            throws IOException;

    List<HiveBloomFilter> readBloomFilterIndexes(InputStream inputStream)
            throws IOException;
}
