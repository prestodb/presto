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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.hive.HiveStorageFormat;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

@Immutable
public class StorageFormat
{
    public static final StorageFormat VIEW_STORAGE_FORMAT = StorageFormat.createNullable(null, null, null);

    private final String serDe;
    private final String inputFormat;
    private final String outputFormat;

    private StorageFormat(String serDe, String inputFormat, String outputFormat)
    {
        this.serDe = serDe;
        this.inputFormat = inputFormat;
        this.outputFormat = outputFormat;
    }

    public String getSerDe()
    {
        if (serDe == null) {
            throw new IllegalStateException("serDe should not be accessed from a null StorageFormat");
        }
        return serDe;
    }

    public String getInputFormat()
    {
        if (inputFormat == null) {
            throw new IllegalStateException("inputFormat should not be accessed from a null StorageFormat");
        }
        return inputFormat;
    }

    public String getOutputFormat()
    {
        if (outputFormat == null) {
            throw new IllegalStateException("outputFormat should not be accessed from a null StorageFormat");
        }
        return outputFormat;
    }

    @JsonProperty("serDe")
    public String getSerDeNullable()
    {
        return serDe;
    }

    @JsonProperty("inputFormat")
    public String getInputFormatNullable()
    {
        return inputFormat;
    }

    @JsonProperty("outputFormat")
    public String getOutputFormatNullable()
    {
        return outputFormat;
    }

    public static StorageFormat fromHiveStorageFormat(HiveStorageFormat hiveStorageFormat)
    {
        return new StorageFormat(hiveStorageFormat.getSerDe(), hiveStorageFormat.getInputFormat(), hiveStorageFormat.getOutputFormat());
    }

    public static StorageFormat create(String serde, String inputFormat, String outputFormat)
    {
        return new StorageFormat(
                requireNonNull(serde, "serDe is null"),
                requireNonNull(inputFormat, "inputFormat is null"),
                requireNonNull(outputFormat, "outputFormat is null"));
    }

    @JsonCreator
    public static StorageFormat createNullable(
            @JsonProperty("serDe") String serDe,
            @JsonProperty("inputFormat") String inputFormat,
            @JsonProperty("outputFormat") String outputFormat)
    {
        return new StorageFormat(serDe, inputFormat, outputFormat);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StorageFormat that = (StorageFormat) o;
        return Objects.equals(serDe, that.serDe) &&
                Objects.equals(inputFormat, that.inputFormat) &&
                Objects.equals(outputFormat, that.outputFormat);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(serDe, inputFormat, outputFormat);
    }
}
