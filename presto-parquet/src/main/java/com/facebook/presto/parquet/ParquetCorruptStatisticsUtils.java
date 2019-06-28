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
package com.facebook.presto.parquet;

import com.google.common.base.Strings;
import org.apache.parquet.SemanticVersion;
import org.apache.parquet.VersionParser;
import org.apache.parquet.hadoop.metadata.FileMetaData;

import java.util.Optional;

public class ParquetCorruptStatisticsUtils
{
    private static final SemanticVersion PARQUET_1025_CHANGED_VERSION = new SemanticVersion(1, 10, 0);

    private ParquetCorruptStatisticsUtils()
    {
    }

    public static boolean shouldIgnoreBinaryStatisticsForVarchars(FileMetaData fileMetaData)
    {
        Optional<SemanticVersion> semanticVersion = parseParquetVersion(fileMetaData.getCreatedBy());
        if (semanticVersion.isPresent() && semanticVersion.get().compareTo(PARQUET_1025_CHANGED_VERSION) < 0) {
            return true;
        }

        return false;
    }

    private static Optional<SemanticVersion> parseParquetVersion(String createdBy)
    {
        if (Strings.isNullOrEmpty(createdBy)) {
            return Optional.empty();
        }
        try {
            VersionParser.ParsedVersion version = VersionParser.parse(createdBy);
            if (!"parquet-mr".equals(version.application)) {
                return Optional.empty();
            }

            if (Strings.isNullOrEmpty(version.version)) {
                return Optional.empty();
            }

            return Optional.of(SemanticVersion.parse(version.version));
        }
        catch (SemanticVersion.SemanticVersionParseException | VersionParser.VersionParseException ex) {
            return Optional.empty();
        }
    }
}
