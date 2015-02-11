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

public class BooleanVector
        implements Vector
{
    public final boolean[] isNull;
    public final boolean[] vector;

    public BooleanVector(int length)
    {
        if (length > MAX_VECTOR_LENGTH) {
            throw new IllegalArgumentException("length greater than max vector length");
        }
        isNull = new boolean[length];
        vector = new boolean[length];
    }

    @Override
    @VisibleForTesting
    public ObjectVector toObjectVector(int size)
    {
        ObjectVector objectVector = new ObjectVector(vector.length);
        for (int i = 0; i < size; i++) {
            if (!isNull[i]) {
                objectVector.vector[i] = vector[i];
            }
        }
        return objectVector;
    }
}
