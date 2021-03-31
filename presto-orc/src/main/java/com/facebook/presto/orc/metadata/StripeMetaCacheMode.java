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
package com.facebook.presto.orc.metadata;

/**
 * Describes content of the metadata cache stored in DWRF files.
 */
public enum StripeMetaCacheMode
{
    // no cache
    NONE(0b00),

    // cache contains stripe indexes
    INDEX(0b1),

    // cache contains stripe footers
    FOOTER(0b10),

    // cache contains stripe indexes and footers
    INDEX_AND_FOOTER(0b11);

    private final int value;

    StripeMetaCacheMode(int value)
    {
        this.value = value;
    }

    public boolean has(StripeMetaCacheMode test)
    {
        return (value & test.value) == test.value;
    }
}
