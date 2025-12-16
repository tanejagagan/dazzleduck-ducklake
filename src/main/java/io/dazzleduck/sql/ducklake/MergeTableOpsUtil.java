package io.dazzleduck.sql.ducklake;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.FileStatus;
import io.dazzleduck.sql.commons.ingestion.CopyResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Collectors;

public class MergeTableOpsUtil {

    private static final String ADD_FILE_TO_TABLE_QUERY = "CALL ducklake_add_data_files('%s', '%s', '%s', schema => 'main', ignore_extra_columns => true, allow_missing => true);";
    private static final String COPY_TO_NEW_FILE_WITH_PARTITION_QUERY = "COPY (SELECT * FROM read_parquet([%s])) TO '%s' (FORMAT PARQUET,%s RETURN_FILES);";
    private static final String INSERT_INTO_SCHEDULE_FILE_DELETION_QUERY = "INSERT INTO %s.ducklake_files_scheduled_for_deletion(data_file_id, path, path_is_relative, schedule_start) SELECT data_file_id, path, path_is_relative, now() FROM %s.ducklake_data_file WHERE data_file_id IN (%s);";
    private static final String DELETE_FILE_COLUMN_STATS_QUERY = "DELETE FROM %s.ducklake_file_column_stats WHERE data_file_id IN (%s);";
    private static final String DELETE_FILE_PARTITION_VALUE_QUERY = "DELETE FROM %s.ducklake_file_partition_value WHERE data_file_id IN (%s);";
    private static final String DELETE_DATA_FILE_QUERY = "DELETE FROM %s.ducklake_data_file WHERE data_file_id IN (%s);";
    private static final String GET_FILE_ID_BY_PATH_QUERY = "SELECT data_file_id FROM %s.ducklake_data_file WHERE table_id = %s AND path IN (%s);";
    private static final String GET_TABLE_NAME_BY_ID =  "SELECT table_name FROM %s.ducklake_table WHERE table_id = '%s';";
    private static final String UPDATE_TABLE_ID =  "UPDATE %s.ducklake_data_file SET table_id = %s WHERE table_id = %s;";
    private static final String SELECT_DUCKLAKE_DATA_FILES_QUERY = "SELECT path, file_size_bytes, end_snapshot FROM %s.ducklake_data_file WHERE file_size_bytes BETWEEN %s AND %s;";

    /**
     * @param database database of the table
     * @param tableId id of the table
     * @param tempTableId temporary  table id which will be used to calculate the metadata
     * @param mdDatabase metadata database
     * @param toAdd Files to be added
     * @param toRemove Files to be deleted
     *  The function will update metadata table inside a transaction. It will abort if any of the file  in the remove is missing
     * Example we have written a file 'c' which  is combination of   file a and b. So this function will be called as
     * replace(1, 10, __test_database, c, List.of(a,  b))
     * add all the files to dummy table
     * begin transaction
     * check all the files in the remove list exist in the tableId
     * change the table id's for the files in the dummy table
     * delete the files in the remove
     * commit the transaction.
     * open conn --> begin transaction --> execute changes with connectionPool.executeBatch --> commit; close conn.
     */
    public static void replace(String database,
                        long tableId,
                        long tempTableId,
                        String mdDatabase,
                        List<String> toAdd,
                        List<String> toRemove) throws SQLException {

        try (Connection conn = ConnectionPool.getConnection()) {
            if (!toAdd.isEmpty()) {
                String tempTableName = ConnectionPool.collectFirst(conn, GET_TABLE_NAME_BY_ID.formatted(mdDatabase, tempTableId), String.class);
                for (String file : toAdd) {
                    ConnectionPool.execute(ADD_FILE_TO_TABLE_QUERY.formatted(database, tempTableName, file));
                }
            }
            if (!toRemove.isEmpty()) {
                ConnectionPool.execute(conn, "BEGIN TRANSACTION;");
                String filePaths = toRemove.stream().map(fp -> "'" + fp + "'").collect(Collectors.joining(", "));
                List<Long> fileIds = (List<Long>) ConnectionPool.collectFirstColumn(conn, GET_FILE_ID_BY_PATH_QUERY.formatted(mdDatabase, tableId, filePaths), Long.class);
                if (fileIds.size() != toRemove.size()) {
                    throw new IllegalStateException("One or more files scheduled for deletion were not found for tableId=" + tableId);
                }
                String fileIdsString = fileIds.stream().map(String::valueOf).collect(Collectors.joining(", "));

                String updateNewFileTableId = UPDATE_TABLE_ID.formatted(mdDatabase, tableId, tempTableId);
                var queries = getQueries(mdDatabase, fileIdsString, updateNewFileTableId);
                for (String query : queries) {
                    ConnectionPool.execute(conn, query);
                }
            }
        }
    }

    private static String[] getQueries(String mdDatabase, String fileIdsString, String updateNewFileTableId) {
        String deleteStatsQuery = DELETE_FILE_COLUMN_STATS_QUERY.formatted(mdDatabase, fileIdsString);
        String deletePartitionQuery = DELETE_FILE_PARTITION_VALUE_QUERY.formatted(mdDatabase, fileIdsString);
        String deleteFileQuery = DELETE_DATA_FILE_QUERY.formatted(mdDatabase, fileIdsString);
        String scheduleDeletesQuery = INSERT_INTO_SCHEDULE_FILE_DELETION_QUERY.formatted(mdDatabase, mdDatabase, fileIdsString);

        return new String[]{
                scheduleDeletesQuery,    // Mark old files for deletion
                updateNewFileTableId,    // Move merged file(s) to main table
                deleteStatsQuery,        // Remove stats of old files
                deletePartitionQuery,    // Remove partition values of old files
                deleteFileQuery,         // Remove old file entries
                "COMMIT"
        };
    }

    /**
     *
     * @param inputFiles input files. Partitioned or un-partitioned.
     * @param partition
     * @return the list of newly created files. Note this will not update the metadata. It needs to be combined with replace function to make this changes visible to the table.
     *
     * input -> /data/log/a, /data/log/b
     * baseLocation -> /data/log
     * partition -> List.Of('date', applicationid).
     */
    public static List<String> rewriteWithPartitionNoCommit(List<String> inputFiles,
                                                     String baseLocation,
                                                     List<String> partition) throws SQLException {
        try (Connection conn = ConnectionPool.getConnection()) {
            String sources = inputFiles.stream().map(s -> "'" + s + "'").collect(Collectors.joining(","));
            String partitionClause = partition.isEmpty() ? "" : "PARTITION_BY (" + String.join(", ", partition) + "),";
            String copyQuery = COPY_TO_NEW_FILE_WITH_PARTITION_QUERY.formatted(sources, baseLocation, partitionClause);

            List<String> files = new ArrayList<>();
            for (CopyResult r : ConnectionPool.collectAll(conn, copyQuery, CopyResult.class)) {
                files.addAll(Arrays.stream(r.files()).map(Object::toString).toList());
            }
            return files;
        }
    }

    /**
     *
     * @param tableId remove the relevant files
     * @param mdDatabase metadata database
     * @param filter filter for the files which need to preserved.
     * @return List of the files which are removed. It will not delete the files but update the metadata.
     */
    public static List<String> drop(long tableId, String mdDatabase, String filter){
        return null;
    }

    public static List<FileStatus> listFiles(String mdDatabase,
                                             String catalog,
                                             long minSize,
                                             long maxSize) throws RuntimeException, SQLException {
        List<FileStatus> filesToCompact = new ArrayList<>();
        String selectQuery = SELECT_DUCKLAKE_DATA_FILES_QUERY.formatted(mdDatabase, minSize, maxSize);
        try (Connection conn = ConnectionPool.getConnection()){
            for (FileStatus file : ConnectionPool.collectAll(conn, selectQuery, FileStatus.class)) {
                filesToCompact.add(file);
            }
        }
        return filesToCompact;
    }
}
