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
package io.prestosql.orc.metadata;

import io.airlift.slice.SliceOutput;

import java.io.IOException;
import java.util.List;

public interface MetadataWriter
{
    List<Integer> getOrcMetadataVersion();

    int writePostscript(SliceOutput output, int footerLength, int metadataLength, CompressionKind compression, int compressionBlockSize)
            throws IOException;

    int writeMetadata(SliceOutput output, Metadata metadata)
            throws IOException;

    int writeFooter(SliceOutput output, Footer footer)
            throws IOException;

    int writeStripeFooter(SliceOutput output, StripeFooter footer)
            throws IOException;

    int writeRowIndexes(SliceOutput output, List<RowGroupIndex> rowGroupIndexes)
            throws IOException;
}
