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
package io.prestosql.plugin.phoenix;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;

import java.io.IOException;

public class WrappedPhoenixInputSplit
{
    private final PhoenixInputSplit phoenixInputSplit;

    public WrappedPhoenixInputSplit(PhoenixInputSplit phoenixInputSplit)
    {
        this.phoenixInputSplit = phoenixInputSplit;
    }

    public PhoenixInputSplit getPhoenixInputSplit()
    {
        return phoenixInputSplit;
    }

    @JsonValue
    public byte[] toBytes()
    {
        return WritableUtils.toByteArray(phoenixInputSplit);
    }

    @JsonCreator
    public static WrappedPhoenixInputSplit fromBytes(byte[] bytes)
            throws IOException
    {
        ByteArrayDataInput dataInput = ByteStreams.newDataInput(bytes);
        PhoenixInputSplit materialized = new PhoenixInputSplit();
        materialized.readFields(dataInput);
        return new WrappedPhoenixInputSplit(materialized);
    }

    @Override
    public int hashCode()
    {
        return phoenixInputSplit.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof WrappedPhoenixInputSplit) {
            PhoenixInputSplit otherSplit = ((WrappedPhoenixInputSplit) obj).getPhoenixInputSplit();
            return phoenixInputSplit.equals(otherSplit);
        }
        return false;
    }
}
