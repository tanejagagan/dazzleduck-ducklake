package io.dazzleduck.sql.ducklake;

import java.util.List;

public class MergeTableOpsUtil {

    /**
     *
     * @param tableId id of the table
     * @param tempTableId temporary  table id which will be used to calculate the metadata
     * @param mdDatabase metadata database
     * @param toAdd Files to be added
     * @param remove Files to be deleted
     *  The function will update metadata table inside a transaction. It will abort if any of the file  in the remove is missing
     *
     */
    public void replace(long tableId,
                        long tempTableId,
                        String mdDatabase, List<String> toAdd, List<String> remove) {

    }

    /**
     *
     * @param tableId id of the table
     * @param mdDatabase metadata database
     * @param input input files. Partitioned or un-partitioned.
     * @param partition
     * @return the list of newly created files. Note this will not update the metadata. It needs to be combined with replace function to make this changes visible to the table.
     */
    public List<String> rewriteWithPartitionNoCommit(long tableId,
                                                     String mdDatabase,
                                                     List<String> input,
                                                     List<String> partition) {
        return null;
    }

    /**
     *
     * @param tableId remove the relevant files
     * @param mdDatabase metadata database
     * @param filter filter for the files which need to preserved.
     * @return List of the files which are removed. It will not delete the files but update the metadata.
     */
    public List<String> drop(long tableId, String mdDatabase, String filter){
        return null;
    }
}
