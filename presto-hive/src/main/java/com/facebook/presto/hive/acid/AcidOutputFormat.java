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
package com.facebook.presto.hive.acid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

/**
 * An extension for OutputFormats that want to implement ACID transactions.
 * @param <V> the row type of the file
 */
@SuppressWarnings("all")
public interface AcidOutputFormat<K extends WritableComparable, V> extends HiveOutputFormat<K, V> {

    /**
     * Options to control how the files are written
     */
    public static class Options implements Cloneable {
        private final Configuration configuration;
        private FileSystem fs;
        private ObjectInspector inspector;
        private boolean writingBase = false;
        private boolean writingDeleteDelta = false;
        private boolean isCompressed = false;
        private Properties properties;
        private Reporter reporter;
        private long minimumTransactionId;
        private long maximumTransactionId;
        private int bucketId;
        /**
         * Based on {@link org.apache.hadoop.hive.ql.metadata.Hive#mvFile(HiveConf, FileSystem, Path, FileSystem, Path, boolean, boolean)}
         * _copy_N starts with 1.
         */
        private int copyNumber = 0;
        private PrintStream dummyStream = null;
        private boolean oldStyle = false;
        private int recIdCol = -1;  // Column the record identifier is in, -1 indicates no record id
        //unique within a transaction
        private int statementId = 0;
        private Path finalDestination;
        /**
         * Create the options object.
         * @param conf Use the given configuration
         */
        public Options(Configuration conf) {
            this.configuration = conf;
        }

        /**
         * shallow clone
         */
        @Override
        public Options clone() {
            try {
                return (Options)super.clone();
            }
            catch(CloneNotSupportedException ex) {
                throw new RuntimeException("clone() not properly implemented: " + ex.getMessage(), ex);
            }
        }
        /**
         * Use the given ObjectInspector for each record written.
         * @param inspector the inspector to use.
         * @return this
         */
        public Options inspector(ObjectInspector inspector) {
            this.inspector = inspector;
            return this;
        }

        /**
         * Is this writing a base directory? Should only be used by the compactor,
         * or when implementing insert overwrite.
         * @param val is this a base file?
         * @return this
         */
        public Options writingBase(boolean val) {
            this.writingBase = val;
            return this;
        }

        /**
         * Is this writing a delete delta directory?
         * @param val is this a delete delta file?
         * @return this
         */
        public Options writingDeleteDelta(boolean val) {
            this.writingDeleteDelta = val;
            return this;
        }

        /**
         * Provide a file system to the writer. Otherwise, the filesystem for the
         * path will be used.
         * @param fs the file system that corresponds to the the path
         * @return this
         */
        public Options filesystem(FileSystem fs) {
            this.fs = fs;
            return this;
        }

        /**
         * Should the output be compressed?
         * @param isCompressed is the output compressed?
         * @return this
         */
        public Options isCompressed(boolean isCompressed) {
            this.isCompressed = isCompressed;
            return this;
        }

        /**
         * Provide the table properties for the table.
         * @param properties the table's properties
         * @return this
         */
        public Options tableProperties(Properties properties) {
            this.properties = properties;
            return this;
        }

        /**
         * Provide the MapReduce reporter.
         * @param reporter the reporter object
         * @return this
         */
        public Options reporter(Reporter reporter) {
            this.reporter = reporter;
            return this;
        }

        /**
         * The minimum transaction id that is included in this file.
         * @param min minimum transaction id
         * @return this
         */
        public Options minimumTransactionId(long min) {
            this.minimumTransactionId = min;
            return this;
        }

        /**
         * The maximum transaction id that is included in this file.
         * @param max maximum transaction id
         * @return this
         */
        public Options maximumTransactionId(long max) {
            this.maximumTransactionId = max;
            return this;
        }

        /**
         * The bucketId that is included in this file.
         * @param bucket the bucketId number
         * @return this
         */
        public Options bucket(int bucket) {
            this.bucketId = bucket;
            return this;
        }

        /**
         * Multiple inserts into legacy (pre-acid) tables can generate multiple copies of each bucket
         * file.
         * @see org.apache.hadoop.hive.ql.exec.Utilities#COPY_KEYWORD
         * @param copyNumber the number of the copy ( > 0)
         * @return this
         */
        public Options copyNumber(int copyNumber) {
            this.copyNumber = copyNumber;
            return this;
        }

        /**
         * Whether it should use the old style (0000000_0) filenames.
         * @param value should use the old style names
         * @return this
         */
        Options setOldStyle(boolean value) {
            oldStyle = value;
            return this;
        }

        /**
         * Which column the row id field is in.
         * @param recIdCol
         * @return this
         */
        public Options recordIdColumn(int recIdCol) {
            this.recIdCol = recIdCol;
            return this;
        }

        /**
         * Temporary switch while we are in development that replaces the
         * implementation with a dummy one that just prints to stream.
         * @param stream the stream to print to
         * @return this
         */
        public Options useDummy(PrintStream stream) {
            this.dummyStream = stream;
            return this;
        }

        /**
         * @since 1.3.0
         * This can be set to -1 to make the system generate old style (delta_xxxx_yyyy) file names.
         * This is primarily needed for testing to make sure 1.3 code can still read files created
         * by older code.  Also used by Comactor.
         */
        public Options statementId(int id) {
            if(id >= AcidUtil.MAX_STATEMENTS_PER_TXN) {
                throw new RuntimeException("Too many statements for transactionId: " + maximumTransactionId);
            }
            if(id < -1) {
                throw new IllegalArgumentException("Illegal statementId value: " + id);
            }
            this.statementId = id;
            return this;
        }
        /**
         * @param p where the data for this operation will eventually end up;
         *          basically table or partition directory in FS
         */
        public Options finalDestination(Path p) {
            this.finalDestination = p;
            return this;
        }

        public Configuration getConfiguration() {
            return configuration;
        }

        public FileSystem getFilesystem() {
            return fs;
        }

        public ObjectInspector getInspector() {
            return inspector;
        }

        public boolean isCompressed() {
            return isCompressed;
        }

        public Properties getTableProperties() {
            return properties;
        }

        public Reporter getReporter() {
            return reporter;
        }

        public long getMinimumTransactionId() {
            return minimumTransactionId;
        }

        public long getMaximumTransactionId() {
            return maximumTransactionId;
        }

        public boolean isWritingBase() {
            return writingBase;
        }

        public boolean isWritingDeleteDelta() {
            return writingDeleteDelta;
        }

        public int getBucketId() {
            return bucketId;
        }

        public int getRecordIdColumn() {
            return recIdCol;
        }

        public PrintStream getDummyStream() {
            return dummyStream;
        }

        boolean getOldStyle() {
            return oldStyle;
        }
        public int getStatementId() {
            return statementId;
        }
        public int getCopyNumber() {
            return copyNumber;
        }
        public Path getFinalDestination() {
            return finalDestination;
        }
    }

    /**
     * Create a RecordUpdater for inserting, updating, or deleting records.
     * @param path the partition directory name
     * @param options the options for the writer
     * @return the RecordUpdater for the output file
     */
    public RecordUpdater getRecordUpdater(Path path,
                                          Options options) throws IOException;

    /**
     * Create a raw writer for ACID events.
     * This is only intended for the compactor.
     * @param path the root directory
     * @param options options for writing the file
     * @return a record writer
     * @throws IOException
     */
    public RecordWriter getRawRecordWriter(Path path,
                                           Options options) throws IOException;
}
