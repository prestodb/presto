package com.facebook.presto.hudi.query.index;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.SchemaTableName;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.util.Lazy;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public abstract class HudiBaseIndexSupport
        implements HudiIndexSupport
{
    private final Logger log;
    protected final SchemaTableName schemaTableName;
    protected final Lazy<HoodieTableMetaClient> lazyMetaClient;

    public HudiBaseIndexSupport(Logger log, SchemaTableName schemaTableName, Lazy<HoodieTableMetaClient> lazyMetaClient)
    {
        this.log = requireNonNull(log, "log is null");
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.lazyMetaClient = requireNonNull(lazyMetaClient, "metaClient is null");
    }

    public void printDebugMessage(Map<String, List<FileSlice>> candidateFileSlices, Map<String, List<FileSlice>> inputFileSlices, long lookupDurationMs)
    {
        if (log.isDebugEnabled()) {
            int candidateFileSize = candidateFileSlices.values().stream().mapToInt(List::size).sum();
            int totalFiles = inputFileSlices.values().stream().mapToInt(List::size).sum();
            double skippingPercent = totalFiles == 0 ? 0.0d : (totalFiles - candidateFileSize) / (totalFiles * 1.0d);

            log.info("Total files: %s; files after data skipping: %s; skipping percent %s; time taken: %s ms; table name: %s",
                    totalFiles,
                    candidateFileSize,
                    skippingPercent,
                    lookupDurationMs,
                    schemaTableName);
        }
    }

    protected Map<String, HoodieIndexDefinition> getAllIndexDefinitions()
    {
        if (lazyMetaClient.get().getIndexMetadata().isEmpty()) {
            return Map.of();
        }

        return lazyMetaClient.get().getIndexMetadata().get().getIndexDefinitions();
    }
}
