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

import com.facebook.presto.hive.metastore.Table;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

@SuppressWarnings("all")
public class AcidUtil {

    public static final String BASE_PREFIX = "base_";
    public static final PathFilter baseFileFilter = new PathFilter()
    {
        @Override
        public boolean accept(Path path) {
            return path.getName().startsWith(BASE_PREFIX);
        }
    };
    public static final String DELTA_PREFIX = "delta_";
    public static final String DELETE_DELTA_PREFIX = "delete_delta_";
    public static final String DELTA_SIDE_FILE_SUFFIX = "_flush_length";
    public static final PathFilter deltaFileFilter = path -> path.getName().startsWith(DELTA_PREFIX);
    public static final PathFilter deleteEventDeltaDirFilter = path -> path.getName().startsWith(DELETE_DELTA_PREFIX);
    public static final String BUCKET_PREFIX = "bucket_";
    public static final PathFilter bucketFileFilter = path -> path.getName().startsWith(BUCKET_PREFIX) &&
            !path.getName().endsWith(DELTA_SIDE_FILE_SUFFIX);
    public static final String BUCKET_DIGITS = "%05d";
    public static final String LEGACY_FILE_BUCKET_DIGITS = "%06d";
    public static final String DELTA_DIGITS = "%07d";
    /**
     * 10K statements per tx.  Probably overkill ... since that many delta files
     * would not be good for performance
     */
    public static final String STATEMENT_DIGITS = "%04d";
    /**
     * This must be in sync with {@link #STATEMENT_DIGITS}
     */
    public static final int MAX_STATEMENTS_PER_TXN = 10000;
    public static final Pattern BUCKET_DIGIT_PATTERN = Pattern.compile("[0-9]{5}$");
    public static final Pattern LEGACY_BUCKET_DIGIT_PATTERN = Pattern.compile("^[0-9]{6}");
    public static final PathFilter originalBucketFilter = new PathFilter() {
        @Override
        public boolean accept(Path path) {
            return ORIGINAL_PATTERN.matcher(path.getName()).matches();
        }
    };

    private static final Pattern ORIGINAL_PATTERN =
            Pattern.compile("[0-9]+_[0-9]+");

    public static final PathFilter hiddenFileFilter = p -> {
        String name = p.getName();
        return !name.startsWith("_") && !name.startsWith(".");
    };

    private static final HadoopShims SHIMS = ShimLoader.getHadoopShims();

    private AcidUtil() {

    }

    /**
     * Get the transaction id from a base directory name.
     *
     * @param path the base directory name
     * @return the maximum transaction id that is included
     */
    static long parseBase(Path path) {
        String filename = path.getName();
        if (filename.startsWith(BASE_PREFIX)) {
            return Long.parseLong(filename.substring(BASE_PREFIX.length()));
        }
        throw new IllegalArgumentException(filename + " does not start with " +
                BASE_PREFIX);
    }

    /**
     * Parse a bucket filename back into the options that would have created
     * the file.
     *
     * @param bucketFile the path to a bucket file
     * @param conf       the configuration
     * @return the options used to create that filename
     */
    public static AcidOutputFormat.Options parseBaseOrDeltaBucketFilename(
            Path bucketFile, Configuration conf) {
        AcidOutputFormat.Options result = new AcidOutputFormat.Options(conf);
        String filename = bucketFile.getName();
        if (ORIGINAL_PATTERN.matcher(filename).matches()) {
            int bucket = Integer.parseInt(filename.substring(0, filename.indexOf('_')));
            result
                    .setOldStyle(true)
                    .minimumTransactionId(0)
                    .maximumTransactionId(0)
                    .bucket(bucket)
                    .writingBase(true);
        }
        else if (filename.startsWith(BUCKET_PREFIX)) {
            int bucket = Integer.parseInt(filename.substring(filename.indexOf('_') + 1));
            if (bucketFile.getParent().getName().startsWith(BASE_PREFIX)) {
                result
                        .setOldStyle(false)
                        .minimumTransactionId(0)
                        .maximumTransactionId(parseBase(bucketFile.getParent()))
                        .bucket(bucket)
                        .writingBase(true);
            }
            else if (bucketFile.getParent().getName().startsWith(DELTA_PREFIX)) {
                ParsedDelta parsedDelta = parsedDelta(bucketFile.getParent(), DELTA_PREFIX);
                result
                        .setOldStyle(false)
                        .minimumTransactionId(parsedDelta.minTransaction)
                        .maximumTransactionId(parsedDelta.maxTransaction)
                        .bucket(bucket);
            }
            else if (bucketFile.getParent().getName().startsWith(DELETE_DELTA_PREFIX)) {
                ParsedDelta parsedDelta = parsedDelta(bucketFile.getParent(), DELETE_DELTA_PREFIX);
                result
                        .setOldStyle(false)
                        .minimumTransactionId(parsedDelta.minTransaction)
                        .maximumTransactionId(parsedDelta.maxTransaction)
                        .bucket(bucket);
            }
        }
        else {
            result.setOldStyle(true).bucket(-1).minimumTransactionId(0)
                    .maximumTransactionId(0);
        }
        return result;
    }

    public static interface Directory {
        /**
         * Get the base directory.
         *
         * @return the base directory to read
         */
        Path getBaseDirectory();

        /**
         * Get the list of original files.  Not {@code null}.
         *
         * @return the list of original files (eg. 000000_0)
         */
        List<FileStatus> getOriginalFiles();

        /**
         * Get the list of base and delta directories that are valid and not
         * obsolete.  Not {@code null}.  List must be sorted in a specific way.
         * See {@link org.apache.hadoop.hive.ql.io.AcidUtils.ParsedDelta#compareTo(org.apache.hadoop.hive.ql.io.AcidUtils.ParsedDelta)}
         * for details.
         *
         * @return the minimal list of current directories
         */
        List<ParsedDelta> getCurrentDirectories();

        /**
         * Get the list of obsolete directories. After filtering out bases and
         * deltas that are not selected by the valid transaction list, return the
         * list of original files, bases, and deltas that have been replaced by
         * more up to date ones.  Not {@code null}.
         */
        List<FileStatus> getObsolete();
    }

    public static class ParsedDelta implements Comparable<ParsedDelta> {
        private final long minTransaction;
        private final long maxTransaction;
        private final FileStatus path;
        //-1 is for internal (getAcidState()) purposes and means the delta dir
        //had no statement ID
        private final int statementId;
        private final boolean isDeleteDelta; // records whether delta dir is of type 'delete_delta_x_y...'

        /**
         * for pre 1.3.x delta files
         */
        ParsedDelta(long min, long max, FileStatus path, boolean isDeleteDelta) {
            this(min, max, path, -1, isDeleteDelta);
        }

        ParsedDelta(long min, long max, FileStatus path, int statementId, boolean isDeleteDelta) {
            this.minTransaction = min;
            this.maxTransaction = max;
            this.path = path;
            this.statementId = statementId;
            this.isDeleteDelta = isDeleteDelta;
        }

        public long getMinTransaction() {
            return minTransaction;
        }

        public long getMaxTransaction() {
            return maxTransaction;
        }

        public Path getPath() {
            return path.getPath();
        }

        public int getStatementId() {
            return statementId == -1 ? 0 : statementId;
        }

        public boolean isDeleteDelta(){
            return isDeleteDelta;
        }

        /**
         * Compactions (Major/Minor) merge deltas/bases but delete of old files
         * happens in a different process; thus it's possible to have bases/deltas with
         * overlapping txnId boundaries.  The sort order helps figure out the "best" set of files
         * to use to get data.
         * This sorts "wider" delta before "narrower" i.e. delta_5_20 sorts before delta_5_10 (and delta_11_20)
         */
        @Override
        public int compareTo(ParsedDelta parsedDelta) {
            if (minTransaction != parsedDelta.minTransaction) {
                if (minTransaction < parsedDelta.minTransaction) {
                    return -1;
                } else {
                    return 1;
                }
            } else if (maxTransaction != parsedDelta.maxTransaction) {
                if (maxTransaction < parsedDelta.maxTransaction) {
                    return 1;
                } else {
                    return -1;
                }
            } else if (statementId != parsedDelta.statementId) {
                /**
                 * We want deltas after minor compaction (w/o statementId) to sort
                 * earlier so that getAcidState() considers compacted files (into larger ones) obsolete
                 * Before compaction, include deltas with all statementIds for a given txnId
                 * in a {@link org.apache.hadoop.hive.ql.io.AcidUtils.Directory}
                 */
                if (statementId < parsedDelta.statementId) {
                    return -1;
                } else {
                    return 1;
                }
            } else {
                return path.compareTo(parsedDelta.path);
            }
        }
    }

    public static ParsedDelta parsedDelta(Path deltaDir) {
        String deltaDirName = deltaDir.getName();
        if (deltaDirName.startsWith(DELETE_DELTA_PREFIX)) {
            return parsedDelta(deltaDir, DELETE_DELTA_PREFIX);
        }
        return parsedDelta(deltaDir, DELTA_PREFIX); // default prefix is delta_prefix
    }

    private static ParsedDelta parseDelta(FileStatus path, String deltaPrefix) {
        ParsedDelta p = parsedDelta(path.getPath(), deltaPrefix);
        boolean isDeleteDelta = deltaPrefix.equals(DELETE_DELTA_PREFIX);
        return new ParsedDelta(p.getMinTransaction(),
                p.getMaxTransaction(), path, p.statementId, isDeleteDelta);
    }

    public static ParsedDelta parsedDelta(Path deltaDir, String deltaPrefix) {
        String filename = deltaDir.getName();
        boolean isDeleteDelta = deltaPrefix.equals(DELETE_DELTA_PREFIX);
        if (filename.startsWith(deltaPrefix)) {
            String rest = filename.substring(deltaPrefix.length());
            int split = rest.indexOf('_');
            int split2 = rest.indexOf('_', split + 1);//may be -1 if no statementId
            long min = Long.parseLong(rest.substring(0, split));
            long max = split2 == -1 ?
                    Long.parseLong(rest.substring(split + 1)) :
                    Long.parseLong(rest.substring(split + 1, split2));
            if (split2 == -1) {
                return new ParsedDelta(min, max, null, isDeleteDelta);
            }
            int statementId = Integer.parseInt(rest.substring(split2 + 1));
            return new ParsedDelta(min, max, null, statementId, isDeleteDelta);
        }
        throw new IllegalArgumentException(deltaDir + " does not start with " +
                deltaPrefix);
    }

    /**
     * Is the given directory in ACID format?
     *
     * @param directory the partition directory to check
     * @param conf      the query configuration
     * @return true, if it is an ACID directory
     * @throws IOException
     */
    public static boolean isAcid(Path directory,
                                 Configuration conf) throws IOException {
        FileSystem fs = directory.getFileSystem(conf);
        for (FileStatus file : fs.listStatus(directory)) {
            String filename = file.getPath().getName();
            if (filename.startsWith(BASE_PREFIX) ||
                    filename.startsWith(DELTA_PREFIX) ||
                    filename.startsWith(DELETE_DELTA_PREFIX)) {
                if (file.isDir()) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isEmpty(Path directory,
                                  Configuration conf) throws IOException {
        FileSystem fs = directory.getFileSystem(conf);
        return fs.listStatus(directory, hiddenFileFilter).length == 0;
    }

    /**
     * State class for getChildState; cannot modify 2 things in a method.
     */
    private static class TxnBase {
        private FileStatus status;
        private long txn = 0;
        private long oldestBaseTxnId = Long.MAX_VALUE;
        private Path oldestBase = null;
    }

    /**
     * Get the ACID state of the given directory. It finds the minimal set of
     * base and diff directories. Note that because major compactions don't
     * preserve the history, we can't use a base directory that includes a
     * transaction id that we must exclude.
     *
     * @param directory the partition directory to analyze
     * @param conf      the configuration
     * @param txnList   the list of transactions that we are reading
     * @return the state of the directory
     * @throws IOException
     */
    public static Directory getAcidState(Path directory,
                                         Configuration conf,
                                         ValidTxnList txnList,
                                         boolean useFileIds,
                                         boolean ignoreEmptyFiles
    ) throws IOException {
        FileSystem fs = directory.getFileSystem(conf);
        // The following 'deltas' includes all kinds of delta files including insert & delete deltas.
        final List<ParsedDelta> deltas = new ArrayList<ParsedDelta>();
        List<ParsedDelta> working = new ArrayList<ParsedDelta>();
        List<FileStatus> originalDirectories = new ArrayList<FileStatus>();
        final List<FileStatus> obsolete = new ArrayList<FileStatus>();

        TxnBase bestBase = new TxnBase();
        final List<FileStatus> original = new ArrayList<>();

        List<FileStatus> children = SHIMS.listLocatedStatus(fs, directory, hiddenFileFilter);
        for (FileStatus child : children) {
            getChildState(
                    child, txnList, working, originalDirectories, original, obsolete, bestBase, ignoreEmptyFiles);
        }

        // If we have a base, the original files are obsolete.
        if (bestBase.status != null) {
            // Add original files to obsolete list if any
            for (FileStatus fswid : original) {
                obsolete.add(fswid);
            }
            // Add original direcotries to obsolete list if any
            obsolete.addAll(originalDirectories);
            // remove the entries so we don't get confused later and think we should
            // use them.
            original.clear();
            originalDirectories.clear();
        } else {
            // Okay, we're going to need these originals.  Recurse through them and figure out what we
            // really need.
            for (FileStatus origDir : originalDirectories) {
                findOriginals(fs, origDir, original);
            }
        }

        Collections.sort(working);
        //so now, 'working' should be sorted like delta_5_20 delta_5_10 delta_11_20 delta_51_60 for example
        //and we want to end up with the best set containing all relevant data: delta_5_20 delta_51_60,
        //subject to list of 'exceptions' in 'txnList' (not show in above example).
        long current = bestBase.txn;
        int lastStmtId = -1;
        ParsedDelta prev = null;
        for (ParsedDelta next : working) {
            if (next.maxTransaction > current) {
                // are any of the new transactions ones that we care about?
                if (txnList.isTxnRangeValid(current + 1, next.maxTransaction) !=
                        ValidTxnList.RangeResponse.NONE) {
                    deltas.add(next);
                    current = next.maxTransaction;
                    lastStmtId = next.statementId;
                    prev = next;
                }
            } else if (next.maxTransaction == current && lastStmtId >= 0) {
                //make sure to get all deltas within a single transaction;  multi-statement txn
                //generate multiple delta files with the same txnId range
                //of course, if maxTransaction has already been minor compacted, all per statement deltas are obsolete
                deltas.add(next);
                prev = next;
            } else if (prev != null && next.maxTransaction == prev.maxTransaction
                    && next.minTransaction == prev.minTransaction
                    && next.statementId == prev.statementId) {
                // The 'next' parsedDelta may have everything equal to the 'prev' parsedDelta, except
                // the path. This may happen when we have split update and we have two types of delta
                // directories- 'delta_x_y' and 'delete_delta_x_y' for the SAME txn range.

                // Also note that any delete_deltas in between a given delta_x_y range would be made
                // obsolete. For example, a delta_30_50 would make delete_delta_40_40 obsolete.
                // This is valid because minor compaction always compacts the normal deltas and the delete
                // deltas for the same range. That is, if we had 3 directories, delta_30_30,
                // delete_delta_40_40 and delta_50_50, then running minor compaction would produce
                // delta_30_50 and delete_delta_30_50.

                deltas.add(next);
                prev = next;
            } else {
                obsolete.add(next.path);
            }
        }

        if (bestBase.oldestBase != null && bestBase.status == null) {
            /**
             * If here, it means there was a base_x (> 1 perhaps) but none were suitable for given
             * {@link txnList}.  Note that 'original' files are logically a base_Long.MIN_VALUE and thus
             * cannot have any data for an open txn.  We could check {@link deltas} has files to cover
             * [1,n] w/o gaps but this would almost never happen...*/
            long[] exceptions = txnList.getInvalidTransactions();
            String minOpenTxn = exceptions != null && exceptions.length > 0 ?
                    Long.toString(exceptions[0]) : "x";
            throw new IOException("Not enough history available for ({0},{1}).  " +
                    "Oldest available base: {2}" .format(
                            Long.toString(txnList.getHighWatermark()),
                            minOpenTxn, bestBase.oldestBase.toString()));
        }

        final Path base = bestBase.status == null ? null : bestBase.status.getPath();

        return new Directory() {

            @Override
            public Path getBaseDirectory() {
                return base;
            }

            @Override
            public List<FileStatus> getOriginalFiles() {
                return original;
            }

            @Override
            public List<ParsedDelta> getCurrentDirectories() {
                return deltas;
            }

            @Override
            public List<FileStatus> getObsolete() {
                return obsolete;
            }
        };
    }

    /**
     * Convert a list of deltas to a list of delta directories.
     * @param deltas the list of deltas out of a Directory object.
     * @return a list of delta directory paths that need to be read
     */
    public static Path[] getPaths(List<ParsedDelta> deltas) {
        Path[] result = new Path[deltas.size()];
        for(int i=0; i < result.length; ++i) {
            result[i] = deltas.get(i).getPath();
        }
        return result;
    }

    /**
     * We can only use a 'base' if it doesn't have an open txn (from specific reader's point of view)
     * A 'base' with open txn in its range doesn't have 'enough history' info to produce a correct
     * snapshot for this reader.
     * Note that such base is NOT obsolete.  Obsolete files are those that are "covered" by other
     * files within the snapshot.
     */
    private static boolean isValidBase(long baseTxnId, ValidTxnList txnList) {
        if (baseTxnId == Long.MIN_VALUE) {
            //such base is created by 1st compaction in case of non-acid to acid table conversion
            //By definition there are no open txns with id < 1.
            return true;
        }
        return txnList.isValidBase(baseTxnId);
    }

    private static void getChildState(FileStatus child, ValidTxnList txnList, List<ParsedDelta> working,
                                      List<FileStatus> originalDirectories, List<FileStatus> original,
                                      List<FileStatus> obsolete, TxnBase bestBase,
                                      boolean ignoreEmptyFiles) throws IOException {
        Path p = child.getPath();
        String fn = p.getName();
        if (fn.startsWith(BASE_PREFIX) && child.isDir()) {
            long txn = parseBase(p);
            if (bestBase.oldestBaseTxnId > txn) {
                //keep track for error reporting
                bestBase.oldestBase = p;
                bestBase.oldestBaseTxnId = txn;
            }
            if (bestBase.status == null) {
                if (isValidBase(txn, txnList)) {
                    bestBase.status = child;
                    bestBase.txn = txn;
                }
            } else if (bestBase.txn < txn) {
                if (isValidBase(txn, txnList)) {
                    obsolete.add(bestBase.status);
                    bestBase.status = child;
                    bestBase.txn = txn;
                }
            } else {
                obsolete.add(child);
            }
        } else if ((fn.startsWith(DELTA_PREFIX) || fn.startsWith(DELETE_DELTA_PREFIX))
                && child.isDir()) {
            String deltaPrefix =
                    (fn.startsWith(DELTA_PREFIX)) ? DELTA_PREFIX : DELETE_DELTA_PREFIX;
            ParsedDelta delta = parseDelta(child, deltaPrefix);
            if (txnList.isTxnRangeValid(delta.minTransaction,
                    delta.maxTransaction) !=
                    ValidTxnList.RangeResponse.NONE) {
                working.add(delta);
            }
        } else if (child.isDir()) {
            // This is just the directory.  We need to recurse and find the actual files.  But don't
            // do this until we have determined there is no base.  This saves time.  Plus,
            // it is possible that the cleaner is running and removing these original files,
            // in which case recursing through them could cause us to get an error.
            originalDirectories.add(child);
        } else if (!ignoreEmptyFiles || child.getLen() != 0) {
            original.add(child);
        }
    }

    /**
     * Find the original files (non-ACID layout) recursively under the partition directory.
     *
     * @param fs       the file system
     * @param stat     the directory to add
     * @param original the list of original files
     * @throws IOException
     */
    private static void findOriginals(FileSystem fs, FileStatus stat,
                                      List<FileStatus> original) throws IOException {
        assert stat.isDir();
        List<FileStatus> children = SHIMS.listLocatedStatus(fs, stat.getPath(), hiddenFileFilter);
        for (FileStatus child : children) {
            if (child.isDir()) {
                findOriginals(fs, child, original);
            } else {
                original.add(child);
            }
        }
    }

    /**
     * Checks if a table is a valid ACID table.
     * Note, users are responsible for using the correct TxnManager. We do not look at
     * SessionState.get().getTxnMgr().supportsAcid() here
     *
     * @param table table
     * @return true if table is a legit ACID table, false otherwise
     */
    public static boolean isAcidTable(Table table) {
        if (table == null) {
            return false;
        }
        String tableIsTransactional = table.getParameters().get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL);
        if (tableIsTransactional == null) {
            tableIsTransactional = table.getParameters().get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL.toUpperCase());
        }

        return tableIsTransactional != null && tableIsTransactional.equalsIgnoreCase("true");
    }

    /**
     * Checks if path is a valid bucket file
     * @param path
     * @return
     */
    public static boolean isBucketFile(Path path) {
        return path.getName().startsWith(BUCKET_PREFIX) &&
                !path.getName().endsWith(DELTA_SIDE_FILE_SUFFIX);
    }
}
