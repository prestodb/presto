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
 * Describes content of the DWRF stripe metadata cache.
 */
public enum DwrfStripeCacheMode
{
    // no cache
    NONE,

    // cache contains stripe indexes
    INDEX,

    // cache contains stripe footers
    FOOTER,

    // cache contains stripe indexes and footers
    INDEX_AND_FOOTER;
}
