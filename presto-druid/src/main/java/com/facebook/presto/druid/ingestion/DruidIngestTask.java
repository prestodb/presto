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
package com.facebook.presto.druid.ingestion;

import com.facebook.airlift.json.JsonCodec;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.fs.Path;

import java.util.List;

public class DruidIngestTask
{
    public static final String TASK_TYPE_INDEX_PARALLEL = "index_parallel";
    public static final String INPUT_FORMAT_JSON = "json";
    public static final String DEFAULT_INPUT_FILE_FILTER = "*.json.gz";

    private final String type;
    private final DruidIngestSpec spec;

    private DruidIngestTask(String type, DruidIngestSpec spec)
    {
        this.type = type;
        this.spec = spec;
    }

    public static class Builder
    {
        private String dataSource;
        private String timestampColumn;
        private List<DruidIngestDimension> dimentions;
        private DruidIngestInputSource inputSource;
        private boolean appendToExisting;

        public Builder withDataSource(String dataSource)
        {
            this.dataSource = dataSource;
            return this;
        }

        public Builder withTimestampColumn(String timestampColumn)
        {
            this.timestampColumn = timestampColumn;
            return this;
        }

        public Builder withDimensions(List<DruidIngestDimension> dimensions)
        {
            this.dimentions = dimensions;
            return this;
        }

        public Builder withInputSource(Path baseDir, List<String> dataFileList)
        {
            switch (baseDir.toUri().getScheme()) {
                case "file":
                    inputSource = new DruidIngestLocalInput("local", baseDir.toString(), DEFAULT_INPUT_FILE_FILTER);
                    break;
                case "hdfs":
                    inputSource = new DruidIngestHDFSInput("hdfs", dataFileList);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported ingestion input source:" + baseDir.toUri().getScheme());
            }

            return this;
        }

        public Builder withAppendToExisting(boolean appendToExisting)
        {
            this.appendToExisting = appendToExisting;
            return this;
        }

        public DruidIngestTask build()
        {
            DruidIngestDataSchema dataSchema = new DruidIngestDataSchema(
                    dataSource,
                    new DruidIngestTimestampSpec(timestampColumn),
                    new DruidIngestDimensionsSpec(dimentions));
            DruidIngestIOConfig ioConfig = new DruidIngestIOConfig(
                    TASK_TYPE_INDEX_PARALLEL,
                    inputSource,
                    new DruidIngestInputFormat(INPUT_FORMAT_JSON),
                    appendToExisting);
            DruidIngestSpec spec = new DruidIngestSpec(dataSchema, ioConfig);
            return new DruidIngestTask(TASK_TYPE_INDEX_PARALLEL, spec);
        }
    }

    @JsonProperty("type")
    public String getType()
    {
        return type;
    }

    @JsonProperty("spec")
    public DruidIngestSpec getSpec()
    {
        return spec;
    }

    public String toJson()
    {
        return JsonCodec.jsonCodec(DruidIngestTask.class).toJson(this);
    }

    public static class DruidIngestSpec
    {
        private final DruidIngestDataSchema dataSchema;
        private final DruidIngestIOConfig ioConfig;

        public DruidIngestSpec(DruidIngestDataSchema dataSchema, DruidIngestIOConfig ioConfig)
        {
            this.dataSchema = dataSchema;
            this.ioConfig = ioConfig;
        }

        @JsonProperty("dataSchema")
        public DruidIngestDataSchema getDataSchema()
        {
            return dataSchema;
        }

        @JsonProperty("ioConfig")
        public DruidIngestIOConfig getIoConfig()
        {
            return ioConfig;
        }
    }

    public static class DruidIngestDataSchema
    {
        private final String dataSource;
        private final DruidIngestTimestampSpec timestampSpec;
        private final DruidIngestDimensionsSpec dimensionsSpec;

        public DruidIngestDataSchema(String dataSource, DruidIngestTimestampSpec timestampSpec, DruidIngestDimensionsSpec dimensionsSpec)
        {
            this.dataSource = dataSource;
            this.timestampSpec = timestampSpec;
            this.dimensionsSpec = dimensionsSpec;
        }

        @JsonProperty("dataSource")
        public String getDataSource()
        {
            return dataSource;
        }

        @JsonProperty("timestampSpec")
        public DruidIngestTimestampSpec getTimestampSpec()
        {
            return timestampSpec;
        }

        @JsonProperty("dimensionsSpec")
        public DruidIngestDimensionsSpec getDimensionsSpec()
        {
            return dimensionsSpec;
        }
    }

    public static class DruidIngestTimestampSpec
    {
        private final String column;

        public DruidIngestTimestampSpec(String column)
        {
            this.column = column;
        }

        @JsonProperty("column")
        public String getColumn()
        {
            return column;
        }
    }

    public static class DruidIngestDimensionsSpec
    {
        private final List<DruidIngestDimension> dimensions;

        public DruidIngestDimensionsSpec(List<DruidIngestDimension> dimensions)
        {
            this.dimensions = dimensions;
        }

        @JsonProperty("dimensions")
        public List<DruidIngestDimension> getDimensions()
        {
            return dimensions;
        }
    }

    public static class DruidIngestDimension
    {
        private final String type;
        private final String name;

        public DruidIngestDimension(String type, String name)
        {
            this.type = type;
            this.name = name;
        }

        @JsonProperty("type")
        public String getType()
        {
            return type;
        }

        @JsonProperty("name")
        public String getName()
        {
            return name;
        }
    }

    public static class DruidIngestIOConfig
    {
        private final String type;
        private final DruidIngestInputSource inputSource;
        private final DruidIngestInputFormat inputFormat;
        private final boolean appendToExisting;

        public DruidIngestIOConfig(
                String type,
                DruidIngestInputSource inputSource,
                DruidIngestInputFormat inputFormat,
                boolean appendToExisting)
        {
            this.type = type;
            this.inputSource = inputSource;
            this.inputFormat = inputFormat;
            this.appendToExisting = appendToExisting;
        }

        @JsonProperty("type")
        public String getType()
        {
            return type;
        }

        @JsonProperty("inputSource")
        public DruidIngestInputSource getInputSource()
        {
            return inputSource;
        }

        @JsonProperty("inputFormat")
        public DruidIngestInputFormat getInputFormat()
        {
            return inputFormat;
        }

        @JsonProperty("appendToExisting")
        public boolean isAppendToExisting()
        {
            return appendToExisting;
        }
    }

    public interface DruidIngestInputSource
    {
    }

    public static class DruidIngestLocalInput
            implements DruidIngestInputSource
    {
        private final String type;
        private final String baseDir;
        private final String filter;

        public DruidIngestLocalInput(String type, String baseDir, String filter)
        {
            this.type = type;
            this.baseDir = baseDir;
            this.filter = filter;
        }

        @JsonProperty("type")
        public String getType()
        {
            return type;
        }

        @JsonProperty("baseDir")
        public String getBaseDir()
        {
            return baseDir;
        }

        @JsonProperty("filter")
        public String getFilter()
        {
            return filter;
        }
    }

    public static class DruidIngestHDFSInput
            implements DruidIngestInputSource
    {
        private final String type;
        private final List<String> paths;

        public DruidIngestHDFSInput(String type, List<String> paths)
        {
            this.type = type;
            this.paths = paths;
        }

        @JsonProperty("type")
        public String getType()
        {
            return type;
        }

        @JsonProperty("paths")
        public List<String> getBaseDir()
        {
            return paths;
        }
    }

    public static class DruidIngestInputFormat
    {
        private final String type;

        public DruidIngestInputFormat(String type)
        {
            this.type = type;
        }

        @JsonProperty("type")
        public String getType()
        {
            return type;
        }
    }
}
