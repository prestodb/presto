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

import java.util.Arrays;

public class ErrorSet
{
    private int positionCount;
    private RuntimeException[] errors;

    public boolean isEmpty()
    {
        for (int i = 0; i < positionCount; i++) {
            if (errors[i] != null) {
                return false;
            }
        }
        return true;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public void clear()
    {
        positionCount = 0;
        if (errors != null) {
            // Drop the references, errors may be large.
            Arrays.fill(errors, null);
        }
    }

    public void erase(int end)
    {
        if (positionCount <= end) {
            clear();
            return;
        }
        System.arraycopy(errors, end, errors, 0, positionCount - end);
        positionCount -= end;
    }

    public void addError(int position, int maxPosition, RuntimeException error)
    {
        if (errors == null) {
            errors = new RuntimeException[maxPosition];
        }
        else if (errors.length < maxPosition) {
            errors = Arrays.copyOf(errors, maxPosition);
        }
        errors[position] = error;
        if (position >= positionCount) {
            for (int i = positionCount; i < position; i++) {
                errors[i] = null;
            }
            positionCount = position + 1;
        }
    }

    public RuntimeException[] getErrors()
    {
        return errors;
    }

    public void setErrors(RuntimeException[] errors, int positionCount)
    {
        if (positionCount > errors.length) {
            throw new IllegalArgumentException("positionCount is larger than the errors array");
        }
        this.positionCount = positionCount;
        this.errors = errors;
    }

    public RuntimeException getFirstError(int numPositions)
    {
        int end = Math.min(positionCount, numPositions);
        for (int i = 0; i < end; i++) {
            if (errors[i] != null) {
                return errors[i];
            }
        }
        return null;
    }
}
