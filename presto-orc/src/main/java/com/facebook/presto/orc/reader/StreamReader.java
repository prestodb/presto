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
package com.facebook.presto.orc.reader;

import com.facebook.presto.orc.Filter;
import com.facebook.presto.orc.QualifyingSet;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;

import java.io.IOException;
import java.util.List;

public interface StreamReader
{
    /* Specifies that only fields in subfields will be accessed by the
    enclosing query. Fields not mentioned should be returned as null
    RLEs so as to preserve record layout without materializing the
    data. 'depth' is the position corresponding to this StreamReader
    in subfields.get(n).getPath(). If subfields is {"a.b.c", "a.b.d"} and this is the StrucStreamReader of b then depth is 1.
    */
    default void setReferencedSubfields(List<Subfield> subfields, int depth) {}

    default void setInputQualifyingSet(QualifyingSet qualifyingSet)
    {
        throw new UnsupportedOperationException();
    }

    default QualifyingSet getInputQualifyingSet()
    {
        return null;
    }

    default QualifyingSet getOutputQualifyingSet()
    {
        return null;
    }

    default void setOutputQualifyingSet(QualifyingSet set)
    {
        throw new UnsupportedOperationException();
    }

    default QualifyingSet getOrCreateOutputQualifyingSet()
    {
        throw new UnsupportedOperationException();
    }

    /* If filter is non-null, sets the output QualifyingSet by
     * applying filter to the rows in the input QualifyingSet. If
     * channel is not -1, appends the values in the post-filter rows
     * to a Block. The Block can be retrieved by getBlock(). */
    default void setFilterAndChannel(Filter filter, int channel, int columnIndex, Type type)
    {
        throw new UnsupportedOperationException("setFilterAndChannel is not supported by " + this.getClass().getSimpleName());
    }

    default int getChannel()
    {
        return -1;
    }

    // Returns the 'numFirstRows first values accumulated into
    // this. If mayReuse is false, this will not keep any reference to
    // the returned memory. Otherwise a subsequent methods of this may
    // alter the Blocks contents.
    default Block getBlock(int numFirstRows, boolean mayReuse)
    {
        throw new UnsupportedOperationException("getBlock is not supported by " + this.getClass().getSimpleName());
    }

    default Filter getFilter()
    {
        return null;
    }

    default int getColumnIndex()
    {
        return -1;
    }

    default int getNumValues()
    {
        throw new UnsupportedOperationException();
    }

    // Sets the number of additional result bytes a scan() is allowed
    // to accumulate.
    default void setResultSizeBudget(long bytes)
    {
        throw new UnsupportedOperationException();
    }

    default void erase(int numFirstRows)
    {
        throw new UnsupportedOperationException("erase is not supported by " + this.getClass().getSimpleName());
    }

    default void compactValues(int[] surviving, int base, int numSurviving)
    {
        throw new UnsupportedOperationException("compactValues is not supported by " + this.getClass().getSimpleName());
    }

    default int getPosition()
    {
        throw new UnsupportedOperationException();
    }

    // Returns an approximation of the size of the Block to be returned from getBlock().
    default int getResultSizeInBytes()
    {
        throw new UnsupportedOperationException();
    }

    default void scan()
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    // Returns the average number of bytes per non-null output value.
    default int getAverageResultSize()
    {
        throw new UnsupportedOperationException();
    }

    // Reconsiders filter order for embedded struct readers
    default void maybeReorderFilters()
    {
    }

    Block readBlock(Type type)
            throws IOException;

    void prepareNextRead(int batchSize);

    void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException;

    void startRowGroup(InputStreamSources dataStreamSources)
            throws IOException;

    void close();

    long getRetainedSizeInBytes();

    /**
     * Returns true if the reader must flush accumulated results
     * before proceeding to a new row group. This is called at the
     * beginning of a stripe/row group, after success return of
     * advanceToNextRowGroup().
     * @param isNewStripe must be true on the first call after
     * advancing to a new stripe and false otherwise.
     */
    default boolean mustExtractValuesBeforeScan(boolean isNewStripe)
    {
        return false;
    }
}
