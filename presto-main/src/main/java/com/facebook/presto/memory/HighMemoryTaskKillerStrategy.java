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
package com.facebook.presto.memory;

public enum HighMemoryTaskKillerStrategy
{
    FREE_MEMORY_ON_FULL_GC, //Kills high memory tasks if worker is running low memory and full GC is not able to reclaim enough memory
    FREE_MEMORY_ON_FREQUENT_FULL_GC //Kills high memory tasks if worker if frequent full GC not able to reclaim enough memory
}
