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
package com.facebook.presto.parquet.writer.levels;

import com.google.common.collect.AbstractIterator;

public interface RepetitionLevelIterable
{
    RepetitionValueIterator getIterator();

    abstract class RepetitionValueIterator
            extends AbstractIterator<RepetitionLevel>
    {
        /**
         * Base repetition level marks at which level either:
         * 1. A new collection starts
         * 2. A collection is null or empty
         * 3. A primitive column stays
         */
        private int baseRepetitionLevel;

        abstract boolean end();

        void setBase(int base)
        {
            this.baseRepetitionLevel = base;
        }

        int getBase()
        {
            return baseRepetitionLevel;
        }
    }

    class RepetitionLevel
    {
        private final int value;
        private final boolean isNull;

        RepetitionLevel(int value, boolean isNull)
        {
            this.value = value;
            this.isNull = isNull;
        }

        boolean isNull()
        {
            return isNull;
        }

        int value()
        {
            return this.value;
        }
    }
}
