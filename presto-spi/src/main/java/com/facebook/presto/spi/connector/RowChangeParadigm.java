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
package com.facebook.presto.spi.connector;

/**
 * Different connectors have different ways of representing row updates,
 * imposed by the underlying storage systems.  The  Trino engine classifies
 * these different paradigms as elements of this RowChangeParadigm
 * enumeration, returned by {@link ConnectorMetadata#getRowChangeParadigm}
 */
public enum RowChangeParadigm
{
    /**
     * A storage paradigm in which the connector can update individual columns
     * of rows identified by a rowId.  The corresponding merge processor class is
     * {@code ChangeOnlyUpdatedColumnsMergeProcessor}
     */
    CHANGE_ONLY_UPDATED_COLUMNS,

    /**
     * A paradigm that translates a changed row into a delete by rowId, and an insert of a
     * new record, which will get a new rowId when the connector writes it out.  The
     * corresponding merge processor class is {@code DeleteAndInsertMergeProcessor}.
     */
    DELETE_ROW_AND_INSERT_ROW,
}
