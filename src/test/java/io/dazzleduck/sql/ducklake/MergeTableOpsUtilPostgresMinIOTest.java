package io.dazzleduck.sql.ducklake;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.FileStatus;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for MergeTableOpsUtil using PostgreSQL for metadata storage
 * and MinIO (S3-compatible) for data file storage.
 *
 * Key differences from other test classes:
 * - Metadata is stored in PostgreSQL (not local DuckDB)
 * - Data files are stored in MinIO/S3 (not local filesystem)
 * - Uses ATTACH with postgres: connection string and S3 DATA_PATH
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MergeTableOpsUtilPostgresMinIOTest {

    private static final String MINIO_ACCESS_KEY = "minioadmin";
    private static final String MINIO_SECRET_KEY = "minioadmin";
    private static final String BUCKET_NAME = "test-pg-minio-bucket";

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17.5")
            .withDatabaseName("testdb")
            .withUsername("test")
            .withPassword("test");

    @Container
    private static final MinIOContainer minioContainer = new MinIOContainer("minio/minio:latest")
            .withUserName(MINIO_ACCESS_KEY)
            .withPassword(MINIO_SECRET_KEY);

    private static S3Client s3Client;
    private static String s3Endpoint;

    @TempDir
    Path tempDir;

    private String catalog;
    private String metadataDb;

    @BeforeAll
    static void setupMinIO() {
        s3Endpoint = minioContainer.getS3URL();

        s3Client = S3Client.builder()
                .endpointOverride(URI.create(s3Endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)))
                .forcePathStyle(true)
                .build();

        // Create bucket
        s3Client.createBucket(CreateBucketRequest.builder()
                .bucket(BUCKET_NAME)
                .build());

        // Set persistent DuckDB configuration for S3 access
        try (Connection conn = ConnectionPool.getConnection()) {
            String[] globalConfig = {
                    "INSTALL httpfs;",
                    "LOAD httpfs;",
                    "SET GLOBAL s3_endpoint='%s';".formatted(s3Endpoint.replace("http://", "")),
                    "SET GLOBAL s3_access_key_id='%s';".formatted(MINIO_ACCESS_KEY),
                    "SET GLOBAL s3_secret_access_key='%s';".formatted(MINIO_SECRET_KEY),
                    "SET GLOBAL s3_use_ssl=false;",
                    "SET GLOBAL s3_url_style='path';"
            };
            ConnectionPool.executeBatchInTxn(conn, globalConfig);
        } catch (Exception e) {
            throw new RuntimeException("Failed to configure DuckDB for MinIO", e);
        }
    }

    @AfterAll
    static void tearDownMinIO() {
        if (s3Client != null) {
            s3Client.close();
        }
    }

    @BeforeEach
    void setup() throws Exception {
        catalog = "pg_minio_ducklake";
        metadataDb = "__ducklake_metadata_" + catalog;

        // Attach ducklake catalog with PostgreSQL metadata + S3 data storage
        String attach = "ATTACH 'ducklake:postgres:dbname=%s user=%s password=%s host=%s port=%d' AS %s (DATA_PATH 's3://%s/data');"
                .formatted(
                        postgres.getDatabaseName(),
                        postgres.getUsername(),
                        postgres.getPassword(),
                        postgres.getHost(),
                        postgres.getFirstMappedPort(),
                        catalog,
                        BUCKET_NAME
                );
        ConnectionPool.execute(attach);
    }

    @AfterEach
    void tearDown() {
        try {
            ConnectionPool.execute("DETACH " + catalog);
        } catch (Exception ignored) {
        }
        cleanupS3Bucket();
    }

    private void cleanupS3Bucket() {
        try {
            ListObjectsV2Response listResponse = s3Client.listObjectsV2(
                    ListObjectsV2Request.builder().bucket(BUCKET_NAME).build());

            if (!listResponse.contents().isEmpty()) {
                List<ObjectIdentifier> toDelete = listResponse.contents().stream()
                        .map(obj -> ObjectIdentifier.builder().key(obj.key()).build())
                        .collect(Collectors.toList());

                s3Client.deleteObjects(DeleteObjectsRequest.builder()
                        .bucket(BUCKET_NAME)
                        .delete(Delete.builder().objects(toDelete).build())
                        .build());
            }
        } catch (Exception e) {
            System.err.println("Error cleaning up S3 bucket: " + e.getMessage());
        }
    }

    @Test
    @Order(1)
    void testReplaceHappyPath() throws Exception {
        String tableName = "pg_s3_products";

        try (Connection conn = ConnectionPool.getConnection()) {
            String[] setup = {
                    "USE " + catalog,
                    "CREATE TABLE %s (id INT, name VARCHAR)".formatted(tableName),
                    "INSERT INTO %s VALUES (1,'A')".formatted(tableName),
                    "INSERT INTO %s VALUES (2,'B')".formatted(tableName)
            };
            ConnectionPool.executeBatchInTxn(conn, setup);

            String GET_TABLE_ID = "SELECT table_id FROM %s.ducklake_table WHERE table_name='%s'";
            Long tableId = ConnectionPool.collectFirst(conn, GET_TABLE_ID.formatted(metadataDb, tableName), Long.class);
            assertNotNull(tableId, "Table ID should not be null");

            // Get original file paths from PostgreSQL metadata
            var filePathsIt = ConnectionPool.collectFirstColumn(conn,
                    "SELECT path FROM %s.ducklake_data_file WHERE table_id = %s"
                            .formatted(metadataDb, tableId), String.class).iterator();

            List<String> originalFilePaths = new ArrayList<>();
            while (filePathsIt.hasNext()) {
                originalFilePaths.add(filePathsIt.next());
            }
            assertFalse(originalFilePaths.isEmpty(), "Should have at least 1 file from INSERTs");

            // Create merged file on S3
            String s3MergedPath = "s3://%s/data/main/%s/merged.parquet".formatted(BUCKET_NAME, tableName);
            ConnectionPool.execute(conn, "COPY (SELECT * FROM %s.%s) TO '%s' (FORMAT PARQUET)"
                    .formatted(catalog, tableName, s3MergedPath));

            // Extract just filenames for removal
            List<String> fileNamesToRemove = originalFilePaths.stream()
                    .map(p -> {
                        int lastSlash = p.lastIndexOf('/');
                        return lastSlash >= 0 ? p.substring(lastSlash + 1) : p;
                    }).toList();

            // Execute replace operation
            long snapshotId = MergeTableOpsUtil.replace(
                    catalog,
                    tableId,
                    metadataDb,
                    List.of(s3MergedPath),
                    fileNamesToRemove
            );

            assertTrue(snapshotId > 0, "Should return valid snapshot ID");

            // Verify merged file is registered as active in PostgreSQL metadata
            Long newFileCount = ConnectionPool.collectFirst(conn,
                    "SELECT COUNT(*) FROM %s.ducklake_data_file WHERE path LIKE '%%merged.parquet%%' AND end_snapshot IS NULL"
                            .formatted(metadataDb), Long.class);
            assertEquals(1L, newFileCount, "Merged file should be registered as active");

            // Verify old files have end_snapshot set
            Long oldFilesWithEndSnapshot = ConnectionPool.collectFirst(conn,
                    "SELECT COUNT(*) FROM %s.ducklake_data_file WHERE table_id = %s AND end_snapshot = %s"
                            .formatted(metadataDb, tableId, snapshotId), Long.class);
            assertEquals((long) originalFilePaths.size(), oldFilesWithEndSnapshot, "Old files should have end_snapshot set");

            // Files should NOT be scheduled for deletion yet
            Long scheduledCount = ConnectionPool.collectFirst(conn,
                    "SELECT COUNT(*) FROM %s.ducklake_files_scheduled_for_deletion".formatted(metadataDb), Long.class);
            assertEquals(0L, scheduledCount, "Files should NOT be scheduled for deletion until expire_snapshots is called");

            // Verify table data integrity
            Long rowCount = ConnectionPool.collectFirst(conn,
                    "SELECT COUNT(*) FROM %s.%s".formatted(catalog, tableName), Long.class);
            assertEquals(2L, rowCount, "Table should contain all rows");
        }
    }

    @Test
    @Order(2)
    void testReplaceWithMissingFile() throws Exception {
        String tableName = "pg_s3_missing";

        try (Connection conn = ConnectionPool.getConnection()) {
            String[] setup = {
                    "USE " + catalog,
                    "CREATE TABLE %s (id INT, name VARCHAR)".formatted(tableName),
                    "INSERT INTO %s VALUES (1,'A')".formatted(tableName)
            };
            ConnectionPool.executeBatchInTxn(conn, setup);

            String GET_TABLE_ID = "SELECT table_id FROM %s.ducklake_table WHERE table_name='%s'";
            Long tableId = ConnectionPool.collectFirst(conn, GET_TABLE_ID.formatted(metadataDb, tableName), Long.class);

            var fileIt = ConnectionPool.collectFirstColumn(conn,
                    "SELECT path FROM %s.ducklake_data_file WHERE table_id = %s"
                            .formatted(metadataDb, tableId), String.class).iterator();

            List<String> existingFiles = new ArrayList<>();
            while (fileIt.hasNext()) {
                existingFiles.add(fileIt.next());
            }
            assertFalse(existingFiles.isEmpty(), "Should have at least one file");

            String s3MergedPath = "s3://%s/data/main/%s/merged.parquet".formatted(BUCKET_NAME, tableName);
            ConnectionPool.execute(conn, "COPY (SELECT * FROM %s.%s) TO '%s' (FORMAT PARQUET)"
                    .formatted(catalog, tableName, s3MergedPath));

            String existingFileName = existingFiles.getFirst();
            int lastSlash = existingFileName.lastIndexOf('/');
            if (lastSlash >= 0) {
                existingFileName = existingFileName.substring(lastSlash + 1);
            }

            String finalExistingFileName = existingFileName;

            Exception ex = assertThrows(
                    Exception.class,
                    () -> MergeTableOpsUtil.replace(
                            catalog,
                            tableId,
                            metadataDb,
                            List.of(s3MergedPath),
                            List.of(finalExistingFileName, "does_not_exist.parquet")
                    )
            );

            String errorMsg = ex.getMessage();
            if (ex.getCause() != null && ex.getCause().getMessage() != null) {
                errorMsg = ex.getCause().getMessage();
            }
            assertTrue(errorMsg.contains("One or more files scheduled for deletion were not found"),
                    "Should throw error for missing file");
        }
    }

    @Test
    @Order(3)
    void testRewriteWithPartition() throws Exception {
        String tableName = "pg_s3_partitioned";

        try (Connection conn = ConnectionPool.getConnection()) {
            String[] setup = {
                    "USE " + catalog,
                    "CREATE TABLE %s (id INT, category VARCHAR, date DATE)".formatted(tableName),
                    "INSERT INTO %s VALUES (1,'A','2025-01-01')".formatted(tableName),
                    "INSERT INTO %s VALUES (2,'A','2025-01-01')".formatted(tableName),
                    "INSERT INTO %s VALUES (3,'B','2025-01-02')".formatted(tableName)
            };
            ConnectionPool.executeBatchInTxn(conn, setup);

            // Export table to S3 parquet file
            String tempExportPath = "s3://%s/data/main/%s/export.parquet".formatted(BUCKET_NAME, tableName);
            ConnectionPool.execute(conn, "COPY (SELECT * FROM %s.%s) TO '%s' (FORMAT PARQUET)"
                    .formatted(catalog, tableName, tempExportPath));

            // Rewrite with partitioning on S3
            String baseLocation = "s3://%s/data/main/%s/partitioned/".formatted(BUCKET_NAME, tableName);

            List<String> files = MergeTableOpsUtil.rewriteWithPartitionNoCommit(
                    List.of(tempExportPath),
                    baseLocation,
                    List.of("date", "category")
            );

            assertFalse(files.isEmpty(), "Should create partitioned files");

            for (String f : files) {
                assertTrue(f.contains("date="), "File should contain date partition");
                assertTrue(f.contains("category="), "File should contain category partition");
            }

            // Verify row count preserved
            String fileList = files.stream().map(f -> "'" + f + "'").collect(Collectors.joining(","));
            Long rowCount = ConnectionPool.collectFirst(conn,
                    "SELECT COUNT(*) FROM read_parquet([" + fileList + "])", Long.class);
            assertEquals(3L, rowCount, "All rows should be preserved");
        }
    }

    @Test
    @Order(4)
    void testRewriteWithoutPartition() throws Exception {
        String tableName = "pg_s3_unpartitioned";

        try (Connection conn = ConnectionPool.getConnection()) {
            String[] setup = {
                    "USE " + catalog,
                    "CREATE TABLE %s (id INT)".formatted(tableName),
                    "INSERT INTO %s VALUES (1)".formatted(tableName),
                    "INSERT INTO %s VALUES (2)".formatted(tableName)
            };
            ConnectionPool.executeBatchInTxn(conn, setup);

            String exportPath = "s3://%s/data/main/%s/export.parquet".formatted(BUCKET_NAME, tableName);
            ConnectionPool.execute(conn, "COPY (SELECT * FROM %s.%s) TO '%s' (FORMAT PARQUET)"
                    .formatted(catalog, tableName, exportPath));

            String outputPath = "s3://%s/data/main/%s/merged.parquet".formatted(BUCKET_NAME, tableName);

            List<String> files = MergeTableOpsUtil.rewriteWithPartitionNoCommit(
                    List.of(exportPath),
                    outputPath,
                    List.of()
            );

            assertFalse(files.isEmpty(), "Should create output file");

            for (String f : files) {
                assertFalse(f.contains("="), "Unpartitioned rewrite must not create partitions");
            }

            String fileList = files.stream().map(f -> "'" + f + "'").collect(Collectors.joining(","));
            Long rowCount = ConnectionPool.collectFirst(conn,
                    "SELECT COUNT(*) FROM read_parquet([" + fileList + "])", Long.class);
            assertEquals(2L, rowCount, "All rows should be preserved");
        }
    }

    @Test
    @Order(5)
    void testListFiles() throws Exception {
        String tableName = "pg_s3_file_listing";

        try (Connection conn = ConnectionPool.getConnection()) {
            String[] setup = {
                    "USE " + catalog,
                    "CREATE TABLE %s (id INT, data VARCHAR)".formatted(tableName),
                    "INSERT INTO %s SELECT i, 'data' || i FROM range(100) t(i)".formatted(tableName)
            };
            ConnectionPool.executeBatchInTxn(conn, setup);

            String GET_TABLE_ID = "SELECT table_id FROM %s.ducklake_table WHERE table_name='%s'";
            Long tableId = ConnectionPool.collectFirst(conn, GET_TABLE_ID.formatted(metadataDb, tableName), Long.class);

            var files = MergeTableOpsUtil.listFiles(metadataDb, tableId, 100L, 1_000_000L);

            assertFalse(files.isEmpty(), "Should find files in size range");

            for (var f : files) {
                assertTrue(f.size() >= 100L, "File should be >= min size");
                assertTrue(f.size() <= 1_000_000L, "File should be <= max size");
            }
        }
    }

    @Test
    @Order(6)
    void testReplaceWithEmptyAddList() throws Exception {
        String tableName = "pg_s3_empty_add";

        try (Connection conn = ConnectionPool.getConnection()) {
            String[] setup = {
                    "USE " + catalog,
                    "CREATE TABLE %s (id INT)".formatted(tableName),
                    "INSERT INTO %s VALUES (1)".formatted(tableName)
            };
            ConnectionPool.executeBatchInTxn(conn, setup);

            String GET_TABLE_ID = "SELECT table_id FROM %s.ducklake_table WHERE table_name='%s'";
            Long tableId = ConnectionPool.collectFirst(conn, GET_TABLE_ID.formatted(metadataDb, tableName), Long.class);

            var it = ConnectionPool.collectFirstColumn(conn,
                    "SELECT path FROM %s.ducklake_data_file WHERE table_id = %s"
                            .formatted(metadataDb, tableId), String.class).iterator();

            List<String> names = new ArrayList<>();
            while (it.hasNext()) {
                String p = it.next();
                names.add(p.substring(p.lastIndexOf('/') + 1));
            }

            long snapshotId = MergeTableOpsUtil.replace(
                    catalog,
                    tableId,
                    metadataDb,
                    List.of(),
                    names
            );

            assertTrue(snapshotId > 0, "Should return valid snapshot ID");

            Long filesWithEndSnapshot = ConnectionPool.collectFirst(conn,
                    "SELECT COUNT(*) FROM %s.ducklake_data_file WHERE table_id=%s AND end_snapshot = %s"
                            .formatted(metadataDb, tableId, snapshotId), Long.class);
            assertEquals((long) names.size(), filesWithEndSnapshot, "All files should have end_snapshot set");

            Long activeCount = ConnectionPool.collectFirst(conn,
                    "SELECT COUNT(*) FROM %s.ducklake_data_file WHERE table_id=%s AND end_snapshot IS NULL"
                            .formatted(metadataDb, tableId), Long.class);
            assertEquals(0L, activeCount, "No active files should remain");

            Long scheduledCount = ConnectionPool.collectFirst(conn,
                    "SELECT COUNT(*) FROM %s.ducklake_files_scheduled_for_deletion".formatted(metadataDb), Long.class);
            assertEquals(0L, scheduledCount, "Files should NOT be scheduled for deletion until expire_snapshots is called");
        }
    }

    @Test
    @Order(7)
    void testReplaceWithEmptyRemoveList() throws Exception {
        String tableName = "pg_s3_empty_remove";

        try (Connection conn = ConnectionPool.getConnection()) {
            String[] setup = {
                    "USE " + catalog,
                    "CREATE TABLE %s (id INT, name VARCHAR)".formatted(tableName),
                    "INSERT INTO %s VALUES (1,'A')".formatted(tableName)
            };
            ConnectionPool.executeBatchInTxn(conn, setup);

            String GET_TABLE_ID = "SELECT table_id FROM %s.ducklake_table WHERE table_name='%s'";
            Long tableId = ConnectionPool.collectFirst(conn, GET_TABLE_ID.formatted(metadataDb, tableName), Long.class);

            // Count active files before
            Long beforeCount = ConnectionPool.collectFirst(conn,
                    "SELECT COUNT(*) FROM %s.ducklake_data_file WHERE table_id = %s AND end_snapshot IS NULL"
                            .formatted(metadataDb, tableId), Long.class);

            // Create a new file on S3 to add
            String s3NewFile = "s3://%s/data/main/%s/new_file.parquet".formatted(BUCKET_NAME, tableName);
            ConnectionPool.execute(conn, "COPY (SELECT * FROM %s.%s) TO '%s' (FORMAT PARQUET)"
                    .formatted(catalog, tableName, s3NewFile));

            // Replace with only adds, no removes
            long snapshotId = MergeTableOpsUtil.replace(
                    catalog,
                    tableId,
                    metadataDb,
                    List.of(s3NewFile),
                    List.of()
            );

            assertTrue(snapshotId > 0, "Should return valid snapshot ID");

            // Verify new file is added as active
            Long afterCount = ConnectionPool.collectFirst(conn,
                    "SELECT COUNT(*) FROM %s.ducklake_data_file WHERE table_id = %s AND end_snapshot IS NULL"
                            .formatted(metadataDb, tableId), Long.class);
            assertEquals(beforeCount + 1, afterCount, "Should have one more active file");

            // Verify data integrity - new file + old files both active
            Long rowCount = ConnectionPool.collectFirst(conn,
                    "SELECT COUNT(*) FROM %s.%s".formatted(catalog, tableName), Long.class);
            assertEquals(2L, rowCount, "Table should contain rows from both old and new files");
        }
    }

    @Test
    @Order(8)
    void testTransactionRollbackOnFailure() throws Exception {
        String tableName = "pg_s3_rollback";

        try (Connection conn = ConnectionPool.getConnection()) {
            String[] setup = {
                    "USE " + catalog,
                    "CREATE TABLE %s (id INT)".formatted(tableName),
                    "INSERT INTO %s VALUES (1)".formatted(tableName)
            };
            ConnectionPool.executeBatchInTxn(conn, setup);

            String GET_TABLE_ID = "SELECT table_id FROM %s.ducklake_table WHERE table_name='%s'";
            Long tableId = ConnectionPool.collectFirst(conn, GET_TABLE_ID.formatted(metadataDb, tableName), Long.class);

            // Count files and snapshots before failed operation
            Long fileCountBefore = ConnectionPool.collectFirst(conn,
                    "SELECT COUNT(*) FROM %s.ducklake_data_file WHERE table_id = %s"
                            .formatted(metadataDb, tableId), Long.class);
            Long snapshotCountBefore = ConnectionPool.collectFirst(conn,
                    "SELECT COUNT(*) FROM %s.ducklake_snapshot".formatted(metadataDb), Long.class);

            // Create a new file for testing
            String s3NewFile = "s3://%s/data/main/%s/new.parquet".formatted(BUCKET_NAME, tableName);
            ConnectionPool.execute(conn, "COPY (SELECT * FROM %s.%s) TO '%s' (FORMAT PARQUET)"
                    .formatted(catalog, tableName, s3NewFile));

            // Try to replace with non-existent file in toRemove - should rollback
            try {
                MergeTableOpsUtil.replace(
                        catalog,
                        tableId,
                        metadataDb,
                        List.of(s3NewFile),
                        List.of("existing.parquet", "nonexistent.parquet")
                );
                fail("Should have thrown exception for missing file");
            } catch (IllegalStateException expected) {
                // Expected exception
            }

            // Verify metadata unchanged after rollback
            Long fileCountAfter = ConnectionPool.collectFirst(conn,
                    "SELECT COUNT(*) FROM %s.ducklake_data_file WHERE table_id = %s"
                            .formatted(metadataDb, tableId), Long.class);
            assertEquals(fileCountBefore, fileCountAfter,
                    "Transaction should have rolled back, file count for main table should be unchanged");
        }
    }
}
