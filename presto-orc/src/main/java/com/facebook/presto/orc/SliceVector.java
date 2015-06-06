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
package com.facebook.presto.orc;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SliceVector
        implements Vector
{
    public Slice[] vector;
    public int[] ids;
    public boolean dictionary;
    public boolean[] isNull;

    public SliceVector()
    {
    }

    public SliceVector(int length)
    {
        if (length > MAX_VECTOR_LENGTH) {
            throw new IllegalArgumentException("length greater than max vector length");
        }
        this.vector = new Slice[length];
        this.dictionary = false;
    }

    public void initialize(int length)
    {
        if (length > MAX_VECTOR_LENGTH) {
            throw new IllegalArgumentException("length greater than max vector length");
        }
        this.vector = new Slice[length];
        this.dictionary = false;
    }

    public void setDictionary(Slice[] dictionary, int[] ids, boolean[] isNullVector)
    {
        requireNonNull(dictionary, "dictionary is null");
        requireNonNull(ids, "ids is null");
        requireNonNull(isNullVector, "isNullVector is null");

        checkArgument(ids.length == isNullVector.length);

        this.vector = dictionary;
        this.ids = ids;
        this.isNull = isNullVector;
        this.dictionary = true;
    }

    @Override
    @VisibleForTesting
    public ObjectVector toObjectVector(int size)
    {
        int length = dictionary ? ids.length : vector.length;
        ObjectVector objectVector = new ObjectVector(length);
        for (int i = 0; i < size; i++) {
            Object objectAtPosition = getSliceAtPosition(i);
            objectVector.vector[i] = objectAtPosition;
        }
        return objectVector;
    }

    public Slice getSliceAtPosition(int position)
    {
        if (dictionary) {
            return isNull[position] ? null : vector[ids[position]];
        }
        return vector[position];
    }
}
