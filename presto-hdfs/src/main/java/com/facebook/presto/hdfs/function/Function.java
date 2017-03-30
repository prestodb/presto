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
package com.facebook.presto.hdfs.function;

/**
 * presto-root
 *
 * @author Jelly
 */
public interface Function
{
    long apply(String v);
    long apply(int v);
    long apply(long v);

    @Override
    int hashCode();

    @Override
    boolean equals(Object obj);

    @Override
    String toString();
}
