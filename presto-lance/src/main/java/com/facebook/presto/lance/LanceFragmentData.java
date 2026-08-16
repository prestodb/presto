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
package com.facebook.presto.lance;

import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.lance.FragmentMetadata;
import org.lance.fragment.DataFile;
import org.lance.fragment.DeletionFile;
import org.lance.fragment.DeletionFileType;
import org.lance.fragment.RowIdMeta;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * JSON-serializable representation of Lance FragmentMetadata.
 * Replaces Java ObjectOutputStream serialization for cross-node commit data.
 */
public class LanceFragmentData
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final int id;
    private final List<LanceDataFile> files;
    private final long physicalRows;
    private final LanceDeletionFile deletionFile;
    private final String rowIdMetadata;

    @JsonCreator
    public LanceFragmentData(
            @JsonProperty("id") int id,
            @JsonProperty("files") List<LanceDataFile> files,
            @JsonProperty("physicalRows") long physicalRows,
            @JsonProperty("deletionFile") LanceDeletionFile deletionFile,
            @JsonProperty("rowIdMetadata") String rowIdMetadata)
    {
        this.id = id;
        this.files = requireNonNull(files, "files is null");
        this.physicalRows = physicalRows;
        this.deletionFile = deletionFile;
        this.rowIdMetadata = rowIdMetadata;
    }

    public static LanceFragmentData fromFragmentMetadata(FragmentMetadata fragment)
    {
        List<LanceDataFile> files = fragment.getFiles().stream()
                .map(LanceDataFile::fromDataFile)
                .collect(toImmutableList());

        LanceDeletionFile deletionFile = null;
        if (fragment.getDeletionFile() != null) {
            deletionFile = LanceDeletionFile.fromDeletionFile(fragment.getDeletionFile());
        }

        String rowIdMetadata = null;
        if (fragment.getRowIdMeta() != null) {
            rowIdMetadata = fragment.getRowIdMeta().getMetadata();
        }

        return new LanceFragmentData(
                fragment.getId(),
                files,
                fragment.getPhysicalRows(),
                deletionFile,
                rowIdMetadata);
    }

    public FragmentMetadata toFragmentMetadata()
    {
        List<DataFile> dataFiles = files.stream()
                .map(LanceDataFile::toDataFile)
                .collect(toImmutableList());

        DeletionFile delFile = deletionFile != null ? deletionFile.toDeletionFile() : null;
        RowIdMeta rowIdMeta = rowIdMetadata != null ? new RowIdMeta(rowIdMetadata) : null;

        return new FragmentMetadata(id, dataFiles, physicalRows, delFile, rowIdMeta);
    }

    @JsonProperty
    public int getId()
    {
        return id;
    }

    @JsonProperty
    public List<LanceDataFile> getFiles()
    {
        return files;
    }

    @JsonProperty
    public long getPhysicalRows()
    {
        return physicalRows;
    }

    @JsonProperty
    public LanceDeletionFile getDeletionFile()
    {
        return deletionFile;
    }

    @JsonProperty
    public String getRowIdMetadata()
    {
        return rowIdMetadata;
    }

    public static String serializeFragments(List<FragmentMetadata> fragments)
    {
        try {
            List<LanceFragmentData> data = fragments.stream()
                    .map(LanceFragmentData::fromFragmentMetadata)
                    .collect(toImmutableList());
            return MAPPER.writeValueAsString(data);
        }
        catch (JsonProcessingException e) {
            throw new PrestoException(LanceErrorCode.LANCE_ERROR, "Failed to serialize fragment metadata", e);
        }
    }

    public static List<FragmentMetadata> deserializeFragments(String json)
    {
        try {
            List<LanceFragmentData> data = MAPPER.readValue(json, new TypeReference<List<LanceFragmentData>>() {});
            return data.stream()
                    .map(LanceFragmentData::toFragmentMetadata)
                    .collect(toImmutableList());
        }
        catch (JsonProcessingException e) {
            throw new PrestoException(LanceErrorCode.LANCE_ERROR, "Failed to deserialize fragment metadata", e);
        }
    }

    public static class LanceDataFile
    {
        private final String path;
        private final int[] fields;
        private final int[] columnIndices;
        private final int fileMajorVersion;
        private final int fileMinorVersion;
        private final Long fileSizeBytes;
        private final Integer baseId;

        @JsonCreator
        public LanceDataFile(
                @JsonProperty("path") String path,
                @JsonProperty("fields") int[] fields,
                @JsonProperty("columnIndices") int[] columnIndices,
                @JsonProperty("fileMajorVersion") int fileMajorVersion,
                @JsonProperty("fileMinorVersion") int fileMinorVersion,
                @JsonProperty("fileSizeBytes") Long fileSizeBytes,
                @JsonProperty("baseId") Integer baseId)
        {
            this.path = requireNonNull(path, "path is null");
            this.fields = requireNonNull(fields, "fields is null");
            this.columnIndices = requireNonNull(columnIndices, "columnIndices is null");
            this.fileMajorVersion = fileMajorVersion;
            this.fileMinorVersion = fileMinorVersion;
            this.fileSizeBytes = fileSizeBytes;
            this.baseId = baseId;
        }

        public static LanceDataFile fromDataFile(DataFile dataFile)
        {
            return new LanceDataFile(
                    dataFile.getPath(),
                    dataFile.getFields(),
                    dataFile.getColumnIndices(),
                    dataFile.getFileMajorVersion(),
                    dataFile.getFileMinorVersion(),
                    dataFile.getFileSizeBytes(),
                    dataFile.getBaseId().orElse(null));
        }

        public DataFile toDataFile()
        {
            return new DataFile(path, fields, columnIndices, fileMajorVersion, fileMinorVersion, fileSizeBytes, baseId);
        }

        @JsonProperty
        public String getPath()
        {
            return path;
        }

        @JsonProperty
        public int[] getFields()
        {
            return fields;
        }

        @JsonProperty
        public int[] getColumnIndices()
        {
            return columnIndices;
        }

        @JsonProperty
        public int getFileMajorVersion()
        {
            return fileMajorVersion;
        }

        @JsonProperty
        public int getFileMinorVersion()
        {
            return fileMinorVersion;
        }

        @JsonProperty
        public Long getFileSizeBytes()
        {
            return fileSizeBytes;
        }

        @JsonProperty
        public Integer getBaseId()
        {
            return baseId;
        }
    }

    public static class LanceDeletionFile
    {
        private final long id;
        private final long readVersion;
        private final Long numDeletedRows;
        private final String fileType;
        private final Integer baseId;

        @JsonCreator
        public LanceDeletionFile(
                @JsonProperty("id") long id,
                @JsonProperty("readVersion") long readVersion,
                @JsonProperty("numDeletedRows") Long numDeletedRows,
                @JsonProperty("fileType") String fileType,
                @JsonProperty("baseId") Integer baseId)
        {
            this.id = id;
            this.readVersion = readVersion;
            this.numDeletedRows = numDeletedRows;
            this.fileType = fileType;
            this.baseId = baseId;
        }

        public static LanceDeletionFile fromDeletionFile(DeletionFile deletionFile)
        {
            return new LanceDeletionFile(
                    deletionFile.getId(),
                    deletionFile.getReadVersion(),
                    deletionFile.getNumDeletedRows(),
                    deletionFile.getFileType() != null ? deletionFile.getFileType().name() : null,
                    deletionFile.getBaseId().orElse(null));
        }

        public DeletionFile toDeletionFile()
        {
            return new DeletionFile(
                    id,
                    readVersion,
                    numDeletedRows,
                    fileType != null ? DeletionFileType.valueOf(fileType) : null,
                    baseId);
        }

        @JsonProperty
        public long getId()
        {
            return id;
        }

        @JsonProperty
        public long getReadVersion()
        {
            return readVersion;
        }

        @JsonProperty
        public Long getNumDeletedRows()
        {
            return numDeletedRows;
        }

        @JsonProperty
        public String getFileType()
        {
            return fileType;
        }

        @JsonProperty
        public Integer getBaseId()
        {
            return baseId;
        }
    }
}
