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
package com.facebook.presto.operator.aggregation.reservoirsample;

import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public class SingleTypeTraits
{
    private String type = "";

    public SingleTypeTraits() {}

    public SingleTypeTraits(SliceInput sliceInput)
    {
        final int readType = sliceInput.readInt();
        if (readType == 0) {
            type = new String("");
        }
        else if (readType == 1) {
            type = new String("double");
        }
        else if (readType == 2) {
            type = new String("integer");
        }
        else if (readType == 3) {
            type = new String("boolean");
        }
        else if (readType == 4) {
            type = new String("long");
        }
        else {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "Unknown type");
        }
    }

    public void serialize(SliceOutput sliceOutput)
    {
        if (this.type.equals("")) {
            sliceOutput.appendInt(0);
        }
        else if (this.type.equals("double")) {
            sliceOutput.appendInt(1);
        }
        else if (this.type.equals("integer")) {
            sliceOutput.appendInt(2);
        }
        else if (this.type.equals("boolean")) {
            sliceOutput.appendInt(3);
        }
        else if (this.type.equals("long")) {
            sliceOutput.appendInt(4);
        }
        else {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "Unknown type");
        }
    }

    public void serialize(SliceOutput sliceOutput, Object sample)
    {
        if (this.type.equals("")) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "empty type");
        }
        else if (this.type.equals("double")) {
            sliceOutput.appendDouble((Double) sample);
        }
        else if (this.type.equals("integer")) {
            sliceOutput.appendInt((Integer) sample);
        }
        else if (this.type.equals("boolean")) {
            sliceOutput.appendByte((Boolean) sample ? 1 : 0);
        }
        else if (this.type.equals("long")) {
            sliceOutput.appendLong((Long) sample);
        }
        else {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "Unknown type");
        }
    }

    public Object deserialize(SliceInput sliceInput)
    {
        if (this.type.equals("")) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "empty type");
        }
        if (this.type.equals("double")) {
            return sliceInput.readDouble();
        }
        if (this.type.equals("integer")) {
            return sliceInput.readInt();
        }
        if (this.type.equals("boolean")) {
            return new Boolean(sliceInput.readByte() == 1);
        }
        if (this.type.equals("long")) {
            return sliceInput.readLong();
        }
        throw new PrestoException(
                INVALID_FUNCTION_ARGUMENT,
                "Unknown type");
    }

    public void add(Double value)
    {
        internalAdd(new String("double"));
    }

    public void add(Integer value)
    {
        internalAdd(new String("integer"));
    }

    public void add(Boolean value)
    {
        internalAdd(new String("boolean"));
    }

    public void add(Long value)
    {
        internalAdd(new String("long"));
    }

    private void internalAdd(String type)
    {
        if (this.type.equals("")) {
            this.type = type;
        }
        if (!this.type.equals(type)) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "Inconsistent type");
        }
    }

    public int getRequiredBytesForSerialization()
    {
        return SizeOf.SIZE_OF_INT;
    }

    public int getRequiredBytesForSerialization(Object o)
    {
        if (type == null) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "No type type");
        }
        if (type.compareTo("double") == 0) {
            return SizeOf.SIZE_OF_DOUBLE;
        }
        if (type.compareTo("integer") == 0) {
            return SizeOf.SIZE_OF_INT;
        }
        if (type.compareTo("boolean") == 0) {
            return SizeOf.SIZE_OF_BYTE;
        }
        if (type.compareTo("long") == 0) {
            return SizeOf.SIZE_OF_LONG;
        }
        throw new PrestoException(
                INVALID_FUNCTION_ARGUMENT,
                "Unknown type");
    }
}
