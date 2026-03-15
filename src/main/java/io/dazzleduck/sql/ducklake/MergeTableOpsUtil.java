package io.dazzleduck.sql.ducklake;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.FileStatus;
import io.dazzleduck.sql.commons.ducklake.DucklakePartitionPruning;
import io.dazzleduck.sql.commons.ingestion.CopyResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;

public class MergeTableOpsUtil {
    private static final Logger logger = LoggerFactory.getLogger(MergeTableOpsUtil.class);

    /**
     * Guards concurrent DuckLake catalog writes during Phase 1.
     *
     * <p>Empirical testing reveals two properties of the DuckLake catalog under concurrent
     * access from multiple {@code DuckDBConnection.duplicate()} connections:
     * <ol>
     *   <li><b>Concurrent {@code CREATE TABLE} is broken</b> — even when each thread creates
     *       a completely different table, 3-4 out of 5 threads fail with
     *       "Failed to parse catalog entry — trailing data after quoted value". The DuckLake
     *       catalog writer is not safe for concurrent DDL.</li>
     *   <li><b>Concurrent {@code ducklake_add_data_files()} is safe</b> — multiple threads
     *       can call it simultaneously on pre-existing tables without any errors.</li>
     * </ol>
     *
     * <p>A {@link ReadWriteLock} captures this asymmetry precisely:
     * <ul>
     *   <li>{@code CREATE TABLE} acquires the <em>write</em> lock — exclusive, blocks all
     *       other catalog operations until the DDL is fully committed.</li>
     *   <li>{@code ducklake_add_data_files()} (and the metadata reads preceding it) acquires
     *       the <em>read</em> lock — shared, multiple add-file calls may proceed in parallel,
     *       but none can run while a {@code CREATE TABLE} is in flight.</li>
     * </ul>
     */
    private static final ReadWriteLock DUCKLAKE_CATALOG_LOCK = new ReentrantReadWriteLock();

    // -------------------------------------------------------------------------
    // SQL query builders — DuckDB / generic metadata path
    // All use mdDatabase + MetadataConfig.q() as a schema prefix.
    // -------------------------------------------------------------------------

    private static String tableIdByNameQuery(String mdDatabase, String tableName) {
        return "SELECT table_id FROM %s%sducklake_table WHERE table_name = '%s'"
                .formatted(mdDatabase, MetadataConfig.q(), tableName);
    }

    private static String tableNameByIdQuery(String mdDatabase, long tableId) {
        return "SELECT table_name FROM %s%sducklake_table WHERE table_id = %s"
                .formatted(mdDatabase, MetadataConfig.q(), tableId);
    }

    private static String tableInfoByIdQuery(String mdDatabase, long tableId) {
        return ("SELECT s.schema_name, t.table_name "
                + "FROM %s%sducklake_table t JOIN %s%sducklake_schema s ON t.schema_id = s.schema_id "
                + "WHERE t.table_id = %s")
                .formatted(mdDatabase, MetadataConfig.q(), mdDatabase, MetadataConfig.q(), tableId);
    }

    private static String fileIdsByPathQuery(String mdDatabase, long tableId, String quotedPaths) {
        return "SELECT data_file_id FROM %s%sducklake_data_file WHERE table_id = %s AND path IN (%s)"
                .formatted(mdDatabase, MetadataConfig.q(), tableId, quotedPaths);
    }

    private static String dataFilesInSizeRangeQuery(String mdDatabase, long tableId, long minSize, long maxSize) {
        return ("SELECT path, file_size_bytes, end_snapshot FROM %s%sducklake_data_file "
                + "WHERE table_id = %s AND file_size_bytes BETWEEN %s AND %s")
                .formatted(mdDatabase, MetadataConfig.q(), tableId, minSize, maxSize);
    }

    private static String createSnapshotQuery(String mdDatabase) {
        return ("INSERT INTO %s%sducklake_snapshot "
                + "(snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) "
                + "SELECT MAX(snapshot_id) + 1, now(), MAX(schema_version), MAX(next_catalog_id), MAX(next_file_id) "
                + "FROM %s%sducklake_snapshot")
                .formatted(mdDatabase, MetadataConfig.q(), mdDatabase, MetadataConfig.q());
    }

    private static String maxSnapshotIdQuery(String mdDatabase) {
        return "SELECT MAX(snapshot_id) FROM %s%sducklake_snapshot"
                .formatted(mdDatabase, MetadataConfig.q());
    }

    private static String setEndSnapshotQuery(String mdDatabase, long snapshotId, String fileIds) {
        return ("UPDATE %s%sducklake_data_file SET end_snapshot = %s "
                + "WHERE data_file_id IN (%s) AND end_snapshot IS NULL")
                .formatted(mdDatabase, MetadataConfig.q(), snapshotId, fileIds);
    }

    private static String moveFilesByIdsQuery(String mdDatabase, long mainTableId, long snapshotId, String fileIds) {
        return ("UPDATE %s%sducklake_data_file SET table_id = %s, begin_snapshot = %s "
                + "WHERE data_file_id IN (%s) AND end_snapshot IS NULL")
                .formatted(mdDatabase, MetadataConfig.q(), mainTableId, snapshotId, fileIds);
    }

    // -------------------------------------------------------------------------
    // SQL query builders — direct Postgres JDBC path
    // Used when MetadataConfig.isPostgres() == true so that INSERT...RETURNING
    // works natively without going through DuckDB's Postgres scanner.
    // DuckLake stores metadata in Postgres's "public" schema ("main" is DuckDB-internal).
    // -------------------------------------------------------------------------

    private static final String PG_SCHEMA = "public";

    private static String pgCreateSnapshotQuery() {
        return ("INSERT INTO %s.ducklake_snapshot "
                + "(snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) "
                + "SELECT MAX(snapshot_id) + 1, now(), MAX(schema_version), MAX(next_catalog_id), MAX(next_file_id) "
                + "FROM %s.ducklake_snapshot RETURNING snapshot_id")
                .formatted(PG_SCHEMA, PG_SCHEMA);
    }

    private static String pgFileIdsByPathQuery(long tableId, String quotedPaths) {
        return "SELECT data_file_id FROM %s.ducklake_data_file WHERE table_id = %s AND path IN (%s)"
                .formatted(PG_SCHEMA, tableId, quotedPaths);
    }

    private static String pgSetEndSnapshotQuery(long snapshotId, String fileIds) {
        return ("UPDATE %s.ducklake_data_file SET end_snapshot = %s "
                + "WHERE data_file_id IN (%s) AND end_snapshot IS NULL")
                .formatted(PG_SCHEMA, snapshotId, fileIds);
    }

    private static String pgMoveFilesByIdsQuery(long mainTableId, long snapshotId, String fileIds) {
        return ("UPDATE %s.ducklake_data_file SET table_id = %s, begin_snapshot = %s "
                + "WHERE data_file_id IN (%s) AND end_snapshot IS NULL")
                .formatted(PG_SCHEMA, mainTableId, snapshotId, fileIds);
    }

    // -------------------------------------------------------------------------
    // DuckDB statement templates (no metadata schema prefix needed)
    // -------------------------------------------------------------------------

    private static final String ADD_FILE_TO_TABLE_QUERY =
            "CALL ducklake_add_data_files('%s', '%s', '%s', schema => 'main', ignore_extra_columns => true, allow_missing => true);";

    private static final String COPY_WITH_PARTITION_QUERY =
            "COPY (%s) TO '%s' (FORMAT PARQUET,%s RETURN_FILES);";

    // =========================================================================
    // Public API
    // =========================================================================

    /**
     * Replaces files in a table using the DuckLake snapshot mechanism.
     *
     * <p>Because {@code ducklake_add_data_files} auto-commits internally, a temp-table
     * approach is used for atomicity:
     * <ol>
     *   <li>Phase 1 (outside transaction): register new files under a persistent temp table
     *       ({@code __temp_<tableId>}) via {@code ducklake_add_data_files}, which auto-commits
     *       to the temp table only. The resulting {@code data_file_id}s are captured.</li>
     *   <li>Phase 2 (single atomic transaction): create a snapshot, move the captured file IDs
     *       from the temp table to the main table, and retire old files with {@code end_snapshot}.
     *       Moving by explicit ID (rather than by temp-table ownership) is safe for concurrent
     *       callers sharing the same persistent temp table.</li>
     * </ol>
     *
     * <p>When a direct Postgres connection is configured ({@link MetadataConfig#isPostgres()}),
     * Phase 2 uses {@code INSERT...RETURNING} for race-free snapshot ID generation.
     * Otherwise a two-query approach is used (suitable for SQLite/DuckDB backends).
     *
     * <p>Files retired with {@code end_snapshot} remain visible in older snapshots until
     * {@code ducklake_expire_snapshots()} is called.
     *
     * @param database    catalog name
     * @param tableId     ID of the main table
     * @param mdDatabase  metadata database name
     * @param toAdd       absolute paths of files to register (may be empty)
     * @param toRemove    file names to retire (may be empty)
     * @return the new snapshot ID, or -1 if both lists are empty
     * @throws SQLException             on database access errors
     * @throws IllegalArgumentException if required parameters are null or blank
     * @throws IllegalStateException    if files to remove are not found in metadata
     */
    public static long replace(String database,
                               long tableId,
                               String mdDatabase,
                               List<String> toAdd,
                               List<String> toRemove) throws SQLException {
        if (database == null || database.isBlank()) throw new IllegalArgumentException("database cannot be null or blank");
        if (mdDatabase == null || mdDatabase.isBlank()) throw new IllegalArgumentException("mdDatabase cannot be null or blank");
        if (toAdd == null) throw new IllegalArgumentException("toAdd cannot be null");
        if (toRemove == null) throw new IllegalArgumentException("toRemove cannot be null");
        if (toAdd.isEmpty() && toRemove.isEmpty()) return -1;

        try (Connection conn = ConnectionPool.getConnection()) {
            String tableName = ConnectionPool.collectFirst(conn, tableNameByIdQuery(mdDatabase, tableId), String.class);
            if (tableName == null) {
                throw new IllegalStateException("Table not found for tableId=" + tableId);
            }

            // Phase 1: register new files under the persistent temp table (auto-commits to temp table only).
            // The temp table name is deterministic (__temp_<tableId>) and reused across calls.
            // See DUCKLAKE_CATALOG_LOCK javadoc for the full race analysis.
            List<Long> toAddIds = null;
            if (!toAdd.isEmpty()) {
                String tempTableName = "__temp_" + tableId;
                // CREATE TABLE IF NOT EXISTS is idempotent: a no-op when the table already exists.
                // The write lock is still required because we cannot know whether the table exists
                // without a separate query — and if it does not, this call writes to the DuckLake
                // catalog. Always holding the write lock here is simpler and safe in both cases.
                DUCKLAKE_CATALOG_LOCK.writeLock().lock();
                try {
                    ConnectionPool.execute(conn, "CREATE TABLE IF NOT EXISTS %s.%s AS SELECT * FROM %s.%s LIMIT 0"
                            .formatted(database, tempTableName, database, tableName));
                } finally {
                    DUCKLAKE_CATALOG_LOCK.writeLock().unlock();
                }
                DUCKLAKE_CATALOG_LOCK.readLock().lock();
                try {
                    toAddIds = registerFilesInTempTable(conn, database, mdDatabase, tempTableName, toAdd);
                } finally {
                    DUCKLAKE_CATALOG_LOCK.readLock().unlock();
                }
            }

            // Phase 2: atomically create snapshot, move temp files to main table, retire old files.
            return MetadataConfig.isPostgres()
                    ? commitViaPostgres(tableId, toAddIds, toRemove)
                    : commitViaDuckDb(conn, mdDatabase, tableId, toAddIds, toRemove);
        }
    }

    /**
     * Rewrites a set of Parquet files into a new layout, optionally repartitioning.
     * Does not update DuckLake metadata — pair with {@link #replace} to commit.
     *
     * <p>This overload performs a plain {@code SELECT *} — no internal-column synthesis.
     * Use {@link #rewriteWithPartitionNoCommit(List, String, List, String)} when compacting
     * files that may lack {@code _ducklake_internal_row_id} / {@code _ducklake_internal_snapshot_id}.
     *
     * @param inputFiles   source Parquet file paths
     * @param baseLocation destination directory (or file path when {@code partition} is empty)
     * @param partition    columns to partition by; empty list means no partitioning
     * @return absolute paths of the newly written files
     */
    public static List<String> rewriteWithPartitionNoCommit(List<String> inputFiles,
                                                            String baseLocation,
                                                            List<String> partition) throws SQLException {
        return rewriteWithPartitionNoCommit(inputFiles, baseLocation, partition, null);
    }

    /**
     * Rewrites a set of Parquet files into a new layout, optionally repartitioning,
     * with automatic synthesis of missing DuckLake internal columns.
     *
     * <p>When {@code mdDatabase} is provided the method inspects each file's Parquet schema
     * via {@code parquet_schema()} and classifies files into three groups:
     * <ul>
     *   <li><b>ALL</b> have {@code _ducklake_internal_*}: plain {@code SELECT *} passthrough.</li>
     *   <li><b>NONE</b> have the columns: values are synthesised from {@code ducklake_data_file}
     *       ({@code begin_snapshot} → snapshot ID, {@code row_id_start + file_row_number} → row ID).</li>
     *   <li><b>MIXED</b>: files are unioned with {@code union_by_name=true}; existing values pass
     *       through and missing values are filled via {@code COALESCE} from metadata.</li>
     * </ul>
     *
     * <p>When {@code mdDatabase} is {@code null} the behaviour is identical to
     * {@link #rewriteWithPartitionNoCommit(List, String, List)} (no synthesis).
     *
     * @param inputFiles   source Parquet file paths
     * @param baseLocation destination directory (or file path when {@code partition} is empty)
     * @param partition    columns to partition by; empty list means no partitioning
     * @param mdDatabase   DuckLake metadata database name used to look up internal column values;
     *                     {@code null} disables synthesis
     * @return absolute paths of the newly written files
     */
    public static List<String> rewriteWithPartitionNoCommit(List<String> inputFiles,
                                                            String baseLocation,
                                                            List<String> partition,
                                                            String mdDatabase) throws SQLException {
        if (inputFiles == null || inputFiles.isEmpty()) throw new IllegalArgumentException("inputFiles cannot be null or empty");
        if (baseLocation == null || baseLocation.isBlank()) throw new IllegalArgumentException("baseLocation cannot be null or blank");
        if (partition == null) throw new IllegalArgumentException("partition cannot be null");
        try (Connection conn = ConnectionPool.getConnection()) {
            String sourceQuery;
            if (mdDatabase != null) {
                Set<String> withInternal = findFilesWithInternalColumns(conn, inputFiles);
                List<String> without = inputFiles.stream().filter(f -> !withInternal.contains(f)).toList();
                Map<String, FileInternalMeta> meta = without.isEmpty()
                        ? Map.of()
                        : fetchFileInternalMeta(conn, mdDatabase, without);
                sourceQuery = buildSourceQuery(inputFiles, withInternal, meta);
            } else {
                sourceQuery = "SELECT * FROM read_parquet([%s])".formatted(
                        inputFiles.stream().map(s -> "'" + escapeSql(s) + "'").collect(Collectors.joining(", ")));
            }
            return executeCopyAndCollectPaths(sourceQuery, baseLocation, partition, conn);
        }
    }

    /**
     * Single-file overload of {@link #rewriteWithPartitionNoCommit(List, String, List)}.
     */
    public static List<String> rewriteWithPartitionNoCommit(String inputFile,
                                                            String baseLocation,
                                                            List<String> partition) throws SQLException {
        if (inputFile == null || inputFile.isBlank()) throw new IllegalArgumentException("inputFile cannot be null or blank");
        if (baseLocation == null || baseLocation.isBlank()) throw new IllegalArgumentException("baseLocation cannot be null or blank");
        if (partition == null) throw new IllegalArgumentException("partition cannot be null");
        try (Connection conn = ConnectionPool.getConnection()) {
            String targetPath = baseLocation + Paths.get(inputFile).getFileName();
            String sourceQuery = "SELECT * FROM read_parquet(['" + escapeSql(inputFile) + "'])";
            return executeCopyAndCollectPaths(sourceQuery, targetPath, partition, conn);
        }
    }

    /**
     * Marks files matching a partition filter for deletion by setting {@code end_snapshot}.
     * Files are not physically removed until {@code ducklake_expire_snapshots()} is called.
     *
     * <p>Only files with Hive-style partition directory structure (e.g.
     * {@code category=sales/data_0.parquet}) can be matched by the filter.
     *
     * @param tableId    the table whose files should be retired
     * @param mdDatabase metadata database name
     * @param filter     SQL SELECT with WHERE clause identifying partitions to delete
     * @return file paths that were retired
     * @throws SQLException             on database access errors
     * @throws JsonProcessingException  if the filter SQL cannot be parsed
     * @throws IllegalArgumentException if filter or mdDatabase is null or blank
     */
    public static List<String> deleteDirectlyFromMetadata(long tableId, String mdDatabase, String filter)
            throws SQLException, JsonProcessingException {
        if (mdDatabase == null || mdDatabase.isBlank()) throw new IllegalArgumentException("mdDatabase cannot be null or blank");
        if (filter == null || filter.isBlank()) throw new IllegalArgumentException("filter cannot be null or blank");

        try (Connection conn = ConnectionPool.getConnection()) {
            var tableInfoIterator = ConnectionPool.collectAll(conn, tableInfoByIdQuery(mdDatabase, tableId), TableInfo.class).iterator();
            if (!tableInfoIterator.hasNext()) {
                throw new IllegalStateException("Table not found for tableId=" + tableId);
            }
            TableInfo tableInfo = tableInfoIterator.next();

            DucklakePartitionPruning pruning = new DucklakePartitionPruning(mdDatabase);
            Set<String> filesToRemove = pruning.pruneFiles(tableInfo.schemaName(), tableInfo.tableName(), filter)
                    .stream().map(FileStatus::fileName).collect(Collectors.toSet());

            if (filesToRemove.isEmpty()) {
                return List.of();
            }

            List<Long> fileIds = queryLongList(conn,
                    fileIdsByPathQuery(mdDatabase, tableId, toQuotedSqlList(new ArrayList<>(filesToRemove))));
            if (fileIds.isEmpty()) {
                return List.of();
            }

            long snapshotId = createNewSnapshot(conn, mdDatabase);
            markFilesAsDeleted(conn, mdDatabase, snapshotId, fileIds);
            return new ArrayList<>(filesToRemove);
        }
    }

    /**
     * Lists active data files for a table within a given size range.
     *
     * @param mdDatabase metadata database name
     * @param tableId    table ID
     * @param minSize    minimum file size in bytes (inclusive)
     * @param maxSize    maximum file size in bytes (inclusive)
     * @return matching file statuses
     */
    public static List<FileStatus> listFiles(String mdDatabase,
                                             long tableId,
                                             long minSize,
                                             long maxSize) throws SQLException {
        if (mdDatabase == null || mdDatabase.isBlank()) throw new IllegalArgumentException("mdDatabase cannot be null or blank");
        if (minSize < 0) throw new IllegalArgumentException("minSize cannot be negative");
        if (maxSize < minSize) throw new IllegalArgumentException("maxSize cannot be less than minSize");
        try (Connection conn = ConnectionPool.getConnection()) {
            List<FileStatus> result = new ArrayList<>();
            ConnectionPool.collectAll(conn, dataFilesInSizeRangeQuery(mdDatabase, tableId, minSize, maxSize), FileStatus.class)
                    .forEach(result::add);
            return result;
        }
    }

    /**
     * Returns the table ID for the given table name, or {@code null} if not found.
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
            Long tableId = ConnectionPool.collectFirst(tableIdByNameQuery(metadataDatabase, escapeSql(tableName)), Long.class);
            if (tableId == null) {
                logger.debug("Table '{}' not found in metadata database '{}'", tableName, metadataDatabase);
            }
            return tableId;
        } catch (SQLException e) {
            logger.error("Failed to lookup table_id for table '{}' in '{}': {}", tableName, metadataDatabase, e.getMessage());
            return null;
        }
    }

    /**
     * Returns the table ID for the given table name, throwing if not found.
     *
     * @throws IllegalStateException if the table does not exist
     */
    public static long requireTableId(String metadataDatabase, String tableName) {
        Long tableId = lookupTableId(metadataDatabase, tableName);
        if (tableId == null) {
            throw new IllegalStateException(
                    "Table '%s' not found in metadata database '%s'".formatted(tableName, metadataDatabase));
        }
        return tableId;
    }

    // =========================================================================
    // Phase helpers
    // =========================================================================

    /**
     * Phase 1: registers each file in {@code toAdd} under the persistent temp table via
     * {@code ducklake_add_data_files}, then returns all active {@code data_file_id}s in the
     * temp table. The temp table must exist and its catalog entry must be fully committed
     * before this method is called (caller holds the read lock — see {@link #DUCKLAKE_CATALOG_LOCK}).
     *
     * @return the {@code data_file_id}s of all active files currently in the temp table;
     *         includes IDs from any previous failed Phase 2 that did not complete the move
     */
    private static List<Long> registerFilesInTempTable(Connection conn,
                                                        String database,
                                                        String mdDatabase,
                                                        String tempTableName,
                                                        List<String> toAdd) throws SQLException {
        long tempTableId = ConnectionPool.collectFirst(conn, tableIdByNameQuery(mdDatabase, tempTableName), Long.class);

        for (String file : toAdd) {
            ConnectionPool.execute(conn, ADD_FILE_TO_TABLE_QUERY.formatted(database, tempTableName, escapeSql(file)));
        }

        // Query back exactly the IDs for the files we just added — not all files in the temp
        // table — so Phase 2 moves only what this call registered, nothing more.
        return queryLongList(conn, fileIdsByPathQuery(mdDatabase, tempTableId, toQuotedSqlList(toAdd)));
    }

    /**
     * Phase 2 via direct Postgres JDBC: creates a snapshot with {@code INSERT...RETURNING}
     * (race-free), retires old files, and moves the given file IDs to the main table —
     * all within a single Postgres transaction.
     *
     * <p>Using explicit {@code data_file_id}s (rather than {@code WHERE table_id = tempTableId})
     * makes the move deterministic: concurrent calls on the same temp table cannot accidentally
     * move each other's files.
     */
    private static long commitViaPostgres(long tableId, List<Long> toAddIds, List<String> toRemove) throws SQLException {
        try (Connection pgConn = MetadataConfig.getPostgresConnection()) {
            pgConn.setAutoCommit(false);
            try {
                long snapshotId = createSnapshotViaPostgres(pgConn);

                if (!toRemove.isEmpty()) {
                    List<Long> fileIds = collectLongListJdbc(pgConn, pgFileIdsByPathQuery(tableId, toQuotedSqlList(toRemove)));
                    if (fileIds.size() != toRemove.size()) {
                        throw new IllegalStateException(
                                "One or more files scheduled for deletion were not found for tableId=" + tableId);
                    }
                    executeJdbc(pgConn, pgSetEndSnapshotQuery(snapshotId, joinIds(fileIds)));
                }

                if (toAddIds != null && !toAddIds.isEmpty()) {
                    executeJdbc(pgConn, pgMoveFilesByIdsQuery(tableId, snapshotId, joinIds(toAddIds)));
                }

                pgConn.commit();
                return snapshotId;
            } catch (SQLException e) {
                pgConn.rollback();
                throw e;
            } catch (RuntimeException e) {
                pgConn.rollback();
                throw e;
            } catch (Exception e) {
                pgConn.rollback();
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Phase 2 via DuckDB connection: creates a snapshot using two queries (INSERT then MAX),
     * retires old files, and moves the given file IDs to the main table —
     * all within a single DuckDB transaction.
     *
     * <p>Using explicit {@code data_file_id}s (rather than {@code WHERE table_id = tempTableId})
     * makes the move deterministic: concurrent calls on the same temp table cannot accidentally
     * move each other's files.
     */
    private static long commitViaDuckDb(Connection conn,
                                        String mdDatabase,
                                        long tableId,
                                        List<Long> toAddIds,
                                        List<String> toRemove) throws SQLException {
        conn.setAutoCommit(false);
        try {
            long snapshotId = createNewSnapshot(conn, mdDatabase);

            if (!toRemove.isEmpty()) {
                List<Long> fileIds = queryLongList(conn, fileIdsByPathQuery(mdDatabase, tableId, toQuotedSqlList(toRemove)));
                if (fileIds.size() != toRemove.size()) {
                    throw new IllegalStateException(
                            "One or more files scheduled for deletion were not found for tableId=" + tableId);
                }
                markFilesAsDeleted(conn, mdDatabase, snapshotId, fileIds);
            }

            if (toAddIds != null && !toAddIds.isEmpty()) {
                ConnectionPool.execute(conn, moveFilesByIdsQuery(mdDatabase, tableId, snapshotId, joinIds(toAddIds)));
            }

            conn.commit();
            return snapshotId;
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } catch (RuntimeException e) {
            conn.rollback();
            throw e;
        } catch (Exception e) {
            conn.rollback();
            throw new RuntimeException(e);
        } finally {
            conn.setAutoCommit(true);
        }
    }

    // =========================================================================
    // Snapshot helpers
    // =========================================================================

    /** Creates a snapshot via DuckDB and returns its ID (two queries: INSERT then MAX). */
    private static long createNewSnapshot(Connection conn, String mdDatabase) throws SQLException {
        ConnectionPool.execute(conn, createSnapshotQuery(mdDatabase));
        return ConnectionPool.collectFirst(conn, maxSnapshotIdQuery(mdDatabase), Long.class);
    }

    /**
     * Creates a snapshot via a direct Postgres JDBC connection using {@code INSERT...RETURNING},
     * atomically returning the new snapshot ID without a race condition.
     *
     * <p>A {@code LOCK TABLE} in {@code SHARE ROW EXCLUSIVE MODE} is acquired first so that
     * concurrent transactions cannot both read the same {@code MAX(snapshot_id)} and then collide
     * on the primary-key constraint when inserting the new row.  The lock is released automatically
     * at transaction end.
     */
    private static long createSnapshotViaPostgres(Connection pgConn) throws SQLException {
        try (Statement lock = pgConn.createStatement()) {
            lock.execute("LOCK TABLE " + PG_SCHEMA + ".ducklake_snapshot IN SHARE ROW EXCLUSIVE MODE");
        }
        try (Statement stmt = pgConn.createStatement();
             ResultSet rs = stmt.executeQuery(pgCreateSnapshotQuery())) {
            if (!rs.next()) {
                throw new SQLException("INSERT...RETURNING returned no rows for snapshot creation");
            }
            return rs.getLong(1);
        }
    }

    // =========================================================================
    // JDBC / query utilities
    // =========================================================================

    private static void markFilesAsDeleted(Connection conn, String mdDatabase, long snapshotId, List<Long> fileIds)
            throws SQLException {
        ConnectionPool.execute(conn, setEndSnapshotQuery(mdDatabase, snapshotId, joinIds(fileIds)));
    }

    /** Runs a DuckDB-connection query and collects the first column as {@code Long} values. */
    private static List<Long> queryLongList(Connection conn, String query) throws SQLException {
        List<Long> result = new ArrayList<>();
        ConnectionPool.collectFirstColumn(conn, query, Long.class).forEach(result::add);
        return result;
    }

    /** Executes a SQL statement on a plain JDBC connection (bypassing {@code ConnectionPool}). */
    private static void executeJdbc(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    /** Runs a query on a plain JDBC connection and collects the first column as {@code Long} values. */
    private static List<Long> collectLongListJdbc(Connection conn, String sql) throws SQLException {
        List<Long> result = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                result.add(rs.getLong(1));
            }
        }
        return result;
    }

    /**
     * Runs a DuckDB COPY query and returns the written file paths reported by {@code RETURN_FILES}.
     *
     * @param sourceQuery a complete SQL SELECT expression used as the COPY source
     */
    private static List<String> executeCopyAndCollectPaths(String sourceQuery,
                                                           String baseLocation,
                                                           List<String> partition,
                                                           Connection conn) {
        while (baseLocation.endsWith("/")) {
            baseLocation = baseLocation.substring(0, baseLocation.length() - 1);
        }
        String partitionClause = partition.isEmpty() ? "" : "PARTITION_BY (" + String.join(", ", partition) + "),";
        String copyQuery = COPY_WITH_PARTITION_QUERY.formatted(sourceQuery, baseLocation, partitionClause);

        List<String> files = new ArrayList<>();
        for (CopyResult r : ConnectionPool.collectAll(conn, copyQuery, CopyResult.class)) {
            files.addAll(Arrays.stream(r.files()).map(Object::toString).toList());
        }
        return files;
    }

    /**
     * Returns the set of file paths (from {@code inputFiles}) that contain a
     * {@code _ducklake_internal_row_id} column in their Parquet schema.
     */
    private static Set<String> findFilesWithInternalColumns(Connection conn,
                                                            List<String> inputFiles) throws SQLException {
        String fileList = inputFiles.stream()
                .map(f -> "'" + escapeSql(f) + "'")
                .collect(Collectors.joining(", "));
        String query = "SELECT DISTINCT file_name FROM parquet_schema([%s]) WHERE name = '_ducklake_internal_row_id'"
                .formatted(fileList);
        Set<String> result = new HashSet<>();
        ConnectionPool.collectFirstColumn(conn, query, String.class).forEach(result::add);
        return result;
    }

    /**
     * Fetches {@code begin_snapshot} and {@code row_id_start} from {@code ducklake_data_file}
     * for each file in {@code filesWithout}, matched by filename (last path component).
     *
     * @param filesWithout absolute paths of files that lack internal columns
     * @return map from bare filename to its metadata; never {@code null}
     * @throws IllegalStateException if any file's metadata cannot be found
     */
    private static Map<String, FileInternalMeta> fetchFileInternalMeta(Connection conn,
                                                                        String mdDatabase,
                                                                        List<String> filesWithout) throws SQLException {
        String quotedFilenames = filesWithout.stream()
                .map(f -> "'" + escapeSql(Paths.get(f).getFileName().toString()) + "'")
                .collect(Collectors.joining(", "));
        String query = ("SELECT list_last(string_split(path, '/')), begin_snapshot, row_id_start "
                + "FROM %s%sducklake_data_file "
                + "WHERE list_last(string_split(path, '/')) IN (%s)")
                .formatted(mdDatabase, MetadataConfig.q(), quotedFilenames);
        Map<String, FileInternalMeta> result = new HashMap<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                result.put(rs.getString(1), new FileInternalMeta(rs.getLong(2), rs.getLong(3)));
            }
        }
        return result;
    }

    /**
     * Builds the SQL SELECT expression that will be used as the COPY source, handling
     * three cases based on which files already carry DuckLake internal columns.
     *
     * <ul>
     *   <li>ALL have internal columns → {@code SELECT * FROM read_parquet([...])}.</li>
     *   <li>NONE have internal columns → synthesise from metadata via JOIN.</li>
     *   <li>MIXED → {@code union_by_name=true} + COALESCE + LEFT JOIN.</li>
     * </ul>
     */
    private static String buildSourceQuery(List<String> inputFiles,
                                           Set<String> filesWithInternal,
                                           Map<String, FileInternalMeta> metaForWithout) {
        String quotedPaths = inputFiles.stream()
                .map(f -> "'" + escapeSql(f) + "'")
                .collect(Collectors.joining(", "));

        if (filesWithInternal.size() == inputFiles.size()) {
            // ALL case: every file already has internal columns — plain passthrough.
            return "SELECT * FROM read_parquet([%s])".formatted(quotedPaths);
        }

        List<String> filesWithout = inputFiles.stream()
                .filter(f -> !filesWithInternal.contains(f))
                .toList();

        String valuesClause = filesWithout.stream()
                .map(f -> {
                    String fname = Paths.get(f).getFileName().toString();
                    FileInternalMeta m = metaForWithout.get(fname);
                    if (m == null) {
                        throw new IllegalStateException("No ducklake_data_file entry found for: " + f);
                    }
                    return "('%s', CAST(%d AS BIGINT), CAST(%d AS BIGINT))".formatted(
                            escapeSql(fname), m.beginSnapshot(), m.rowIdStart());
                })
                .collect(Collectors.joining(", "));

        if (filesWithInternal.isEmpty()) {
            // NONE case: synthesise _ducklake_internal_* for every file from metadata.
            return ("SELECT p.* EXCLUDE (filename, file_row_number), "
                    + "m.begin_snapshot AS _ducklake_internal_snapshot_id, "
                    + "(m.row_id_start + p.file_row_number)::BIGINT AS _ducklake_internal_row_id "
                    + "FROM read_parquet([%s], filename=true, file_row_number=true) p "
                    + "JOIN (VALUES %s) AS m(fname, begin_snapshot, row_id_start) "
                    + "ON list_last(string_split(p.filename, '/')) = m.fname")
                    .formatted(quotedPaths, valuesClause);
        } else {
            // MIXED case: pass through existing values, synthesise for files that lack them.
            return ("SELECT p.* EXCLUDE (filename, file_row_number, "
                    + "_ducklake_internal_snapshot_id, _ducklake_internal_row_id), "
                    + "COALESCE(p._ducklake_internal_snapshot_id, m.begin_snapshot) "
                    + "  AS _ducklake_internal_snapshot_id, "
                    + "COALESCE(p._ducklake_internal_row_id, "
                    + "  (m.row_id_start + p.file_row_number)::BIGINT) AS _ducklake_internal_row_id "
                    + "FROM read_parquet([%s], union_by_name=true, filename=true, file_row_number=true) p "
                    + "LEFT JOIN (VALUES %s) AS m(fname, begin_snapshot, row_id_start) "
                    + "ON list_last(string_split(p.filename, '/')) = m.fname")
                    .formatted(quotedPaths, valuesClause);
        }
    }

    // =========================================================================
    // SQL string utilities
    // =========================================================================

    /** Escapes single quotes in a SQL string value. */
    private static String escapeSql(String value) {
        return value.replace("'", "''");
    }

    /** Converts a list of strings to a quoted, comma-separated SQL list (e.g. {@code 'a','b'}). */
    private static String toQuotedSqlList(List<String> values) {
        return values.stream()
                .map(v -> "'" + v.replace("'", "''") + "'")
                .collect(Collectors.joining(", "));
    }

    /** Joins a list of Long IDs into a comma-separated string for use in SQL {@code IN} clauses. */
    private static String joinIds(List<Long> ids) {
        return ids.stream().map(String::valueOf).collect(Collectors.joining(", "));
    }

    // =========================================================================
    // Types
    // =========================================================================

    public record TableInfo(String schemaName, String tableName) {}

    /** Holds the DuckLake metadata needed to synthesise internal columns for a single data file. */
    private record FileInternalMeta(long beginSnapshot, long rowIdStart) {}
}
