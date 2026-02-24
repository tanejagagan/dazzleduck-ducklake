package io.dazzleduck.sql.ducklake;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.FileStatus;
import io.dazzleduck.sql.commons.ducklake.DucklakePartitionPruning;
import io.dazzleduck.sql.commons.ingestion.CopyResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Collectors;

public class MergeTableOpsUtil {
    private static final Logger logger = LoggerFactory.getLogger(MergeTableOpsUtil.class);

    // Queries use %s for mdDatabase and %s for schema qualifier (via MetadataConfig.q())
    private static String getTableIdSql(String mdDatabase, String tableName) {
        return "SELECT table_id FROM %s%sducklake_table WHERE table_name = '%s'"
                .formatted(mdDatabase, MetadataConfig.q(), tableName);
    }

    private static String getFileIdByPathQuery(String mdDatabase, long tableId, String filePaths) {
        return "SELECT data_file_id FROM %s%sducklake_data_file WHERE table_id = %s AND path IN (%s)"
                .formatted(mdDatabase, MetadataConfig.q(), tableId, filePaths);
    }

    private static String getTableNameById(String mdDatabase, long tableId) {
        return "SELECT table_name FROM %s%sducklake_table WHERE table_id = '%s'"
                .formatted(mdDatabase, MetadataConfig.q(), tableId);
    }

    private static String selectDucklakeDataFilesQuery(String mdDatabase, long tableId, long minSize, long maxSize) {
        return "SELECT path, file_size_bytes, end_snapshot FROM %s%sducklake_data_file WHERE table_id = %s AND file_size_bytes BETWEEN %s AND %s"
                .formatted(mdDatabase, MetadataConfig.q(), tableId, minSize, maxSize);
    }

    private static String createSnapshotQuery(String mdDatabase) {
        return "INSERT INTO %s%sducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) SELECT MAX(snapshot_id) + 1, now(), MAX(schema_version), MAX(next_catalog_id), MAX(next_file_id) FROM %s%sducklake_snapshot"
                .formatted(mdDatabase, MetadataConfig.q(), mdDatabase, MetadataConfig.q());
    }

    private static String getMaxSnapshotIdQuery(String mdDatabase) {
        return "SELECT MAX(snapshot_id) FROM %s%sducklake_snapshot"
                .formatted(mdDatabase, MetadataConfig.q());
    }

    private static String setEndSnapshotQuery(String mdDatabase, long snapshotId, String fileIds) {
        return "UPDATE %s%sducklake_data_file SET end_snapshot = %s WHERE data_file_id IN (%s) AND end_snapshot IS NULL"
                .formatted(mdDatabase, MetadataConfig.q(), snapshotId, fileIds);
    }

    private static String getTableInfoByIdQuery(String mdDatabase, long tableId) {
        return "SELECT s.schema_name, t.table_name FROM %s%sducklake_table t JOIN %s%sducklake_schema s ON t.schema_id = s.schema_id WHERE t.table_id = %s"
                .formatted(mdDatabase, MetadataConfig.q(), mdDatabase, MetadataConfig.q(), tableId);
    }

    private static String getFileIdsByTableAndPathsQuery(String mdDatabase, long tableId, String filePaths) {
        return "SELECT data_file_id FROM %s%sducklake_data_file WHERE table_id = %s AND path IN (%s)"
                .formatted(mdDatabase, MetadataConfig.q(), tableId, filePaths);
    }

    private static String getExistingActiveFilesQuery(String mdDatabase, long tableId) {
        return "SELECT path FROM %s%sducklake_data_file WHERE table_id = %s AND end_snapshot IS NULL"
                .formatted(mdDatabase, MetadataConfig.q(), tableId);
    }

    private static String moveFilesToMainTableQuery(String mdDatabase, long mainTableId, long tempTableId, long snapshotId) {
        return "UPDATE %s%sducklake_data_file SET table_id = %s, begin_snapshot = %s WHERE table_id = %s AND end_snapshot IS NULL"
                .formatted(mdDatabase, MetadataConfig.q(), mainTableId, snapshotId, tempTableId);
    }

    // ADD_FILE_TO_TABLE_QUERY does not need qualifier - it's a CALL statement, not a metadata query
    private static final String ADD_FILE_TO_TABLE_QUERY =
            "CALL ducklake_add_data_files('%s', '%s', '%s', schema => 'main', ignore_extra_columns => true, allow_missing => true);";

    // COPY query does not touch metadata tables, no qualifier needed
    private static final String COPY_TO_NEW_FILE_WITH_PARTITION_QUERY =
            "COPY (SELECT * FROM read_parquet([%s])) TO '%s' (FORMAT PARQUET,%s RETURN_FILES);";

    /**
     * Replaces files in a table using proper DuckLake snapshot mechanism.
     *
     * <p>Since {@code ducklake_add_data_files} auto-commits internally, this method uses a
     * temp-table approach to achieve full transactional atomicity:
     * <ol>
     *   <li>Phase 1 (outside transaction): Add new files to a temp table via
     *       {@code ducklake_add_data_files} (auto-commits to temp table only)</li>
     *   <li>Phase 2 (single transaction): Create snapshot, move file records from temp to main
     *       table, and mark old files with end_snapshot — all atomically committed or rolled back</li>
     * </ol>
     *
     * <p>Supports any combination: toAdd only, toRemove only, or both.
     *
     * <p>Files marked with end_snapshot remain visible in older snapshots until
     * {@code ducklake_expire_snapshots()} is called, which moves them to
     * {@code ducklake_files_scheduled_for_deletion}.
     *
     * @param database    database/catalog name
     * @param tableId     id of the main table
     * @param mdDatabase  metadata database name
     * @param toAdd       files to be added to the table (can be empty)
     * @param toRemove    files to be marked for deletion (can be empty)
     * @return the snapshot ID created for this replace operation, or -1 if both lists are empty
     * @throws SQLException             if a database access error occurs
     * @throws IllegalArgumentException if required parameters are null or blank
     * @throws IllegalStateException    if files to remove are not found
     */
    public static long replace(String database,
                               long tableId,
                               String mdDatabase,
                               List<String> toAdd,
                               List<String> toRemove) throws SQLException {
        if (database == null || database.isBlank()) {
            throw new IllegalArgumentException("database cannot be null or blank");
        }
        if (mdDatabase == null || mdDatabase.isBlank()) {
            throw new IllegalArgumentException("mdDatabase cannot be null or blank");
        }
        if (toAdd == null) {
            throw new IllegalArgumentException("toAdd cannot be null");
        }
        if (toRemove == null) {
            throw new IllegalArgumentException("toRemove cannot be null");
        }

        // Early return if nothing to do
        if (toAdd.isEmpty() && toRemove.isEmpty()) {
            return -1;
        }

        try (Connection conn = ConnectionPool.getConnection()) {
            String tableName = ConnectionPool.collectFirst(conn, getTableNameById(mdDatabase, tableId), String.class);
            if (tableName == null) {
                throw new IllegalStateException("Table not found for tableId=" + tableId);
            }

            String tempTableName = null;
            Long tempTableId = null;

            // Phase 1: Add files to a temp table (ducklake_add_data_files auto-commits,
            // but only affects the temp table — main table is untouched)
            if (!toAdd.isEmpty()) {
                tempTableName = "__temp_" + tableId;

                // Create temp table with same schema as main table
                ConnectionPool.execute(conn, "CREATE OR REPLACE TABLE %s.%s AS SELECT * FROM %s.%s LIMIT 0"
                        .formatted(database, tempTableName, database, tableName));
                tempTableId = ConnectionPool.collectFirst(conn, getTableIdSql(mdDatabase, tempTableName), Long.class);

                // Get existing active file paths to avoid duplicates on retry
                var existingFilesIterator = ConnectionPool.collectFirstColumn(conn,
                        getExistingActiveFilesQuery(mdDatabase, tableId),
                        String.class).iterator();
                List<String> existingFileNames = new ArrayList<>();
                while (existingFilesIterator.hasNext()) {
                    existingFileNames.add(extractFileName(existingFilesIterator.next()));
                }

                for (String file : toAdd) {
                    String fileName = extractFileName(file);
                    if (!existingFileNames.contains(fileName)) {
                        String escapedFile = escapeSql(file);
                        ConnectionPool.execute(conn, ADD_FILE_TO_TABLE_QUERY.formatted(database, tempTableName, escapedFile));
                    }
                }
            }

            // Phase 2: Single transaction — create snapshot, move files, mark deletions
            conn.setAutoCommit(false);
            try {
                long snapshotId = createNewSnapshot(conn, mdDatabase);

                // Mark files for deletion
                if (!toRemove.isEmpty()) {
                    String filePaths = toQuotedSqlList(toRemove);
                    List<Long> fileIds = collectLongList(conn, getFileIdByPathQuery(mdDatabase, tableId, filePaths));

                    if (fileIds.size() != toRemove.size()) {
                        throw new IllegalStateException("One or more files scheduled for deletion were not found for tableId=" + tableId);
                    }

                    markFilesAsDeleted(conn, mdDatabase, snapshotId, fileIds);
                }

                // Move files from temp table to main table in metadata
                if (tempTableId != null) {
                    ConnectionPool.execute(conn, moveFilesToMainTableQuery(mdDatabase, tableId, tempTableId, snapshotId));
                }

                conn.commit();

                // Cleanup: drop temp table (outside transaction, best-effort)
                if (tempTableName != null) {
                    conn.setAutoCommit(true);
                    try {
                        ConnectionPool.execute(conn, "DROP TABLE IF EXISTS %s.%s".formatted(database, tempTableName));
                    } catch (Exception cleanupEx) {
                        logger.warn("Failed to drop temp table {}.{}: {}", database, tempTableName, cleanupEx.getMessage());
                    }
                }
                return snapshotId;
            } catch (Exception e) {
                try {
                    conn.rollback();
                } catch (SQLException rollbackEx) {
                    e.addSuppressed(rollbackEx);
                }
                if (e instanceof SQLException) throw (SQLException) e;
                throw (RuntimeException) e;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    /**
     *
     * @param inputFiles input files. Partitioned or un-partitioned.
     * @param partition
     * @return the list of newly created files. Note this will not update the metadata. It needs to be combined with replace function to make this changes visible to the table.
     * <p>
     * input -> /data/log/a, /data/log/b
     * baseLocation -> /data/log
     * partition -> List.Of('date', applicationid).
     */
    public static List<String> rewriteWithPartitionNoCommit(List<String> inputFiles,
                                                            String baseLocation,
                                                            List<String> partition) throws SQLException {
        if (inputFiles == null || inputFiles.isEmpty()) {
            throw new IllegalArgumentException("inputFiles cannot be null or empty");
        }
        if (baseLocation == null || baseLocation.isBlank()) {
            throw new IllegalArgumentException("baseLocation cannot be null or blank");
        }
        if (partition == null) {
            throw new IllegalArgumentException("partition cannot be null");
        }
        try (Connection conn = ConnectionPool.getConnection()) {
            String sources = inputFiles.stream().map(s -> "'" + s + "'").collect(Collectors.joining(","));
            return getStrings(sources, baseLocation, partition, conn);
        }
    }

    /**
     *
     * @param inputFile input file..
     * @param partition
     * @return the list of newly created files. Note this will not update the metadata. It needs to be combined with replace function to make this changes visible to the table.
     * <p>
     * input -> /data/log/a, /data/log/b
     * baseLocation -> /data/log
     * partition -> List.Of('date', applicationid).
     */
    public static List<String> rewriteWithPartitionNoCommit(String inputFile,
                                                            String baseLocation,
                                                            List<String> partition) throws SQLException {
        if (inputFile == null || inputFile.isBlank()) {
            throw new IllegalArgumentException("inputFile cannot be null or blank");
        }
        if (baseLocation == null || baseLocation.isBlank()) {
            throw new IllegalArgumentException("baseLocation cannot be null or blank");
        }
        if (partition == null) {
            throw new IllegalArgumentException("partition cannot be null");
        }
        try (Connection conn = ConnectionPool.getConnection()) {
            Path p = Paths.get(inputFile);
            Path fileName = p.getFileName();
            var targetPath = baseLocation + fileName.toString();
            return getStrings("'" + inputFile + "'", targetPath, partition, conn);
        }
    }

    private static List<String> getStrings(String inputFile, String baseLocation, List<String> partition, Connection conn) {
        // Remove trailing slashes to prevent double slashes in partition paths
        while (baseLocation.endsWith("/")) {
            baseLocation = baseLocation.substring(0, baseLocation.length() - 1);
        }
        String partitionClause = partition.isEmpty() ? "" : "PARTITION_BY (" + String.join(", ", partition) + "),";
        String copyQuery = COPY_TO_NEW_FILE_WITH_PARTITION_QUERY.formatted(inputFile, baseLocation, partitionClause);

        List<String> files = new ArrayList<>();
        for (CopyResult r : ConnectionPool.collectAll(conn, copyQuery, CopyResult.class)) {
            files.addAll(Arrays.stream(r.files()).map(Object::toString).toList());
        }
        return files;
    }

    /**
     * Marks files matching the filter for deletion by setting their end_snapshot directly in metadata.
     * This follows DuckLake's proper snapshot mechanism - files are not immediately deleted
     * but marked with an end_snapshot. After calling ducklake_expire_snapshots(), the files
     * will be moved to ducklake_files_scheduled_for_deletion.
     *
     * <p>Note: This method uses partition pruning to find files, which only works with files
     * that have partition directory structure in their paths (e.g., "category=sales/data_0.parquet").
     * Files created by normal INSERT statements may not have this structure.
     *
     * @param tableId    the table ID whose files should be marked for deletion
     * @param mdDatabase metadata database name
     * @param filter     SQL SELECT statement with WHERE clause to identify files to delete.
     *                   Files matching this filter (based on partition pruning) will be marked for deletion.
     * @return List of the file paths which are marked for deletion.
     * @throws SQLException             if a database access error occurs
     * @throws JsonProcessingException  if the filter SQL cannot be parsed
     * @throws IllegalArgumentException if filter or mdDatabase is null or blank
     */
    public static List<String> deleteDirectlyFromMetadata(long tableId, String mdDatabase, String filter) throws SQLException, JsonProcessingException {
        if (mdDatabase == null || mdDatabase.isBlank()) {
            throw new IllegalArgumentException("mdDatabase cannot be null or blank");
        }
        if (filter == null || filter.isBlank()) {
            throw new IllegalArgumentException("filter cannot be null or blank");
        }

        try (Connection conn = ConnectionPool.getConnection()) {
            // Get table schema and name for partition pruning
            var tableInfoIterator = ConnectionPool.collectAll(conn,
                    getTableInfoByIdQuery(mdDatabase, tableId), TableInfo.class).iterator();
            if (!tableInfoIterator.hasNext()) {
                throw new IllegalStateException("Table not found for tableId=" + tableId);
            }
            TableInfo tableInfo = tableInfoIterator.next();

            // Use DucklakePartitionPruning to get files matching the filter (files to delete)
            DucklakePartitionPruning pruning = new DucklakePartitionPruning(mdDatabase);
            List<FileStatus> filesToDelete = pruning.pruneFiles(tableInfo.schemaName(), tableInfo.tableName(), filter);
            Set<String> filesToRemove = filesToDelete.stream()
                    .map(FileStatus::fileName)
                    .collect(Collectors.toSet());

            if (filesToRemove.isEmpty()) {
                return List.of();
            }

            // Get file IDs for files to remove
            String filePaths = toQuotedSqlList(new ArrayList<>(filesToRemove));
            List<Long> fileIds = collectLongList(conn,
                    getFileIdsByTableAndPathsQuery(mdDatabase, tableId, filePaths));

            if (fileIds.isEmpty()) {
                return List.of();
            }

            // Create a new snapshot and mark files for deletion
            long newSnapshotId = createNewSnapshot(conn, mdDatabase);
            markFilesAsDeleted(conn, mdDatabase, newSnapshotId, fileIds);

            return new ArrayList<>(filesToRemove);
        }
    }

    public static List<FileStatus> listFiles(String mdDatabase,
                                             long tableId,
                                             long minSize,
                                             long maxSize) throws SQLException {
        if (mdDatabase == null || mdDatabase.isBlank()) {
            throw new IllegalArgumentException("mdDatabase cannot be null or blank");
        }
        if (minSize < 0) {
            throw new IllegalArgumentException("minSize cannot be negative");
        }
        if (maxSize < minSize) {
            throw new IllegalArgumentException("maxSize cannot be less than minSize");
        }
        List<FileStatus> filesToCompact = new ArrayList<>();
        try (Connection conn = ConnectionPool.getConnection()) {
            for (FileStatus file : ConnectionPool.collectAll(conn, selectDucklakeDataFilesQuery(mdDatabase, tableId, minSize, maxSize), FileStatus.class)) {
                filesToCompact.add(file);
            }
        }
        return filesToCompact;
    }

    /**
     * Looks up the table ID for a given table name from DuckLake metadata.
     *
     * @param metadataDatabase The metadata database name (e.g., "__ducklake_metadata_<catalog>")
     * @param tableName        The table name to look up
     * @return The table ID, or null if not found
     */
    public static Long lookupTableId(String metadataDatabase, String tableName) {
        if (metadataDatabase == null || metadataDatabase.isBlank()) {
            logger.warn("Cannot lookup table_id: metadataDatabase is null or blank");
            return null;
        }
        if (tableName == null || tableName.isBlank()) {
            logger.warn("Cannot lookup table_id: tableName is null or blank");
            return null;
        }

        try {
            Long tableId = ConnectionPool.collectFirst(getTableIdSql(metadataDatabase, escapeSql(tableName)), Long.class);
            if (tableId == null) {
                logger.debug("Table '{}' not found in metadata database '{}'", tableName, metadataDatabase);
            }
            return tableId;
        } catch (SQLException e) {
            logger.error("Failed to lookup table_id for table '{}' in '{}': {}",
                    tableName, metadataDatabase, e.getMessage());
            return null;
        }
    }

    /**
     * Looks up the table ID for a given table name, throwing an exception if not found.
     *
     * @param metadataDatabase The metadata database name
     * @param tableName        The table name to look up
     * @return The table ID
     * @throws IllegalStateException if the table is not found
     */
    public static long requireTableId(String metadataDatabase, String tableName) {
        Long tableId = lookupTableId(metadataDatabase, tableName);
        if (tableId == null) {
            throw new IllegalStateException(
                    "Table '%s' not found in metadata database '%s'".formatted(tableName, metadataDatabase));
        }
        return tableId;
    }

    /**
     * Escapes single quotes in SQL strings.
     */
    private static String escapeSql(String value) {
        return value.replace("'", "''");
    }

    /**
     * Converts a list of strings to a quoted SQL list (e.g., "'val1','val2'").
     * Single quotes in values are escaped to prevent SQL injection.
     */
    private static String toQuotedSqlList(List<String> values) {
        return values.stream()
                .map(v -> "'" + v.replace("'", "''") + "'")
                .collect(Collectors.joining(", "));
    }

    /**
     * Extracts the filename from a file path.
     * Handles both paths with '/' separators and plain filenames.
     */
    private static String extractFileName(String path) {
        int lastSlash = path.lastIndexOf('/');
        return lastSlash >= 0 ? path.substring(lastSlash + 1) : path;
    }

    /**
     * Collects a list of Long values from a SQL query.
     */
    private static List<Long> collectLongList(Connection conn, String query) throws SQLException {
        var iterator = ConnectionPool.collectFirstColumn(conn, query, Long.class).iterator();
        List<Long> result = new ArrayList<>();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        return result;
    }

    /**
     * Creates a new DuckLake snapshot and returns its ID.
     */
    private static long createNewSnapshot(Connection conn, String mdDatabase) throws SQLException {
        ConnectionPool.execute(conn, createSnapshotQuery(mdDatabase));
        return ConnectionPool.collectFirst(conn, getMaxSnapshotIdQuery(mdDatabase), Long.class);
    }

    private static void markFilesAsDeleted(Connection conn, String mdDatabase, long snapshotId, List<Long> fileIds) throws SQLException {
        String fileIdsString = fileIds.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(", "));
        ConnectionPool.execute(conn, setEndSnapshotQuery(mdDatabase, snapshotId, fileIdsString));
    }

    /**
     * Record to hold table schema and name information.
     */
    public record TableInfo(String schemaName, String tableName) {
    }
}
