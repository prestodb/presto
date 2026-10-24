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
package com.facebook.presto.orc.metadata.statistics.hll;

enum Format
{
    SPARSE_V1(0),
    DENSE_V1(1),
    SPARSE_V2(2),
    DENSE_V2(3);

    private byte tag;

    Format(int tag)
    {
        this.tag = (byte) tag;
    }

    public byte getTag()
    {
        return tag;
    }
}
