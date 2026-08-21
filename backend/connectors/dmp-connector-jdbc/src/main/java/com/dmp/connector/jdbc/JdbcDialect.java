package com.dmp.connector.jdbc;

import com.dmp.connector.api.ConnectorException;

import java.util.ArrayList;
import java.util.List;

/**
 * The parts of SQL that differ between databases.
 *
 * <p>Reading a table is nearly identical everywhere; writing one is not. Upsert in particular has
 * four incompatible spellings across the four databases here, and identifier quoting differs in
 * ways that matter the moment a column is called {@code order} or {@code user}. Isolating those
 * differences means one connector implementation serves every relational database rather than four
 * near-copies drifting apart.
 */
public interface JdbcDialect {

    /** Stable connector type, for example {@code jdbc-postgres}. */
    String connectorType();

    String displayName();

    /** Shown in the console as the URL placeholder. */
    String urlExample();

    /**
     * Quotes an identifier so a column named {@code order} or {@code user} still works.
     *
     * <p>Applied after the identifier has already passed the strict pattern in {@link JdbcConfig},
     * so this is about reserved words and case sensitivity rather than injection — that is handled
     * before anything reaches here.
     */
    String quote(String identifier);

    /**
     * Builds the write statement for the configured mode.
     *
     * @param table   already qualified and quoted
     * @param columns column names in bind order
     * @param keys    conflict key for upsert modes
     */
    String writeStatement(String table, List<String> columns, List<String> keys,
                          JdbcConfig.WriteMode mode);

    /** Whether this database can express an idempotent write at all. */
    default boolean supportsUpsert() {
        return true;
    }

    /**
     * Whether a server-side cursor needs autocommit disabled.
     *
     * <p>PostgreSQL silently ignores {@code setFetchSize} on an autocommit connection and buffers
     * the entire result set — the difference between a bounded memory footprint and an
     * out-of-memory kill on a large chunk. Most other drivers do not care.
     */
    default boolean requiresTransactionForStreaming() {
        return true;
    }

    static JdbcDialect forType(String connectorType) {
        return switch (connectorType) {
            case PostgresDialect.TYPE -> new PostgresDialect();
            case MySqlDialect.TYPE -> new MySqlDialect();
            case SqlServerDialect.TYPE -> new SqlServerDialect();
            case OracleDialect.TYPE -> new OracleDialect();
            default -> throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "No SQL dialect is registered for connector type '" + connectorType + "'");
        };
    }

    // ------------------------------------------------------------- PostgreSQL

    final class PostgresDialect implements JdbcDialect {

        static final String TYPE = "jdbc-postgres";

        @Override
        public String connectorType() {
            return TYPE;
        }

        @Override
        public String displayName() {
            return "PostgreSQL";
        }

        @Override
        public String urlExample() {
            return "jdbc:postgresql://host:5432/database";
        }

        @Override
        public String quote(String identifier) {
            return '"' + identifier + '"';
        }

        @Override
        public String writeStatement(String table, List<String> columns, List<String> keys,
                                     JdbcConfig.WriteMode mode) {
            String sql = insertPrefix(this, table, columns);

            return switch (mode) {
                case INSERT -> sql;
                case INSERT_IGNORE -> sql + " ON CONFLICT (" + quoteAll(this, keys) + ") DO NOTHING";
                case UPSERT -> {
                    List<String> assignments = new ArrayList<>();
                    for (String column : columns) {
                        if (!keys.contains(column)) {
                            assignments.add(quote(column) + " = EXCLUDED." + quote(column));
                        }
                    }
                    // Every column is a key, so there is nothing to update. Behaving as
                    // INSERT_IGNORE keeps the write idempotent rather than emitting invalid SQL.
                    yield assignments.isEmpty()
                            ? sql + " ON CONFLICT (" + quoteAll(this, keys) + ") DO NOTHING"
                            : sql + " ON CONFLICT (" + quoteAll(this, keys) + ") DO UPDATE SET "
                                    + String.join(", ", assignments);
                }
            };
        }
    }

    // ------------------------------------------------------------------ MySQL

    final class MySqlDialect implements JdbcDialect {

        static final String TYPE = "jdbc-mysql";

        @Override
        public String connectorType() {
            return TYPE;
        }

        @Override
        public String displayName() {
            return "MySQL / MariaDB";
        }

        @Override
        public String urlExample() {
            return "jdbc:mysql://host:3306/database";
        }

        @Override
        public String quote(String identifier) {
            return '`' + identifier + '`';
        }

        @Override
        public String writeStatement(String table, List<String> columns, List<String> keys,
                                     JdbcConfig.WriteMode mode) {
            String sql = insertPrefix(this, table, columns);

            return switch (mode) {
                case INSERT -> sql;
                // MySQL has no ON CONFLICT: INSERT IGNORE swallows the duplicate, which is the
                // same outcome by a different spelling.
                case INSERT_IGNORE -> sql.replaceFirst("^INSERT INTO", "INSERT IGNORE INTO");
                case UPSERT -> {
                    List<String> assignments = new ArrayList<>();
                    for (String column : columns) {
                        if (!keys.contains(column)) {
                            // VALUES(col) is deprecated in MySQL 8.0.20+ in favour of an alias, but
                            // it still works everywhere and the alias form does not work on 5.7 or
                            // MariaDB. Compatibility wins here.
                            assignments.add(quote(column) + " = VALUES(" + quote(column) + ")");
                        }
                    }
                    yield assignments.isEmpty()
                            ? sql.replaceFirst("^INSERT INTO", "INSERT IGNORE INTO")
                            : sql + " ON DUPLICATE KEY UPDATE " + String.join(", ", assignments);
                }
            };
        }

        @Override
        public boolean requiresTransactionForStreaming() {
            // MySQL streams with Integer.MIN_VALUE as the fetch size instead, so a transaction
            // buys nothing.
            return false;
        }
    }

    // ------------------------------------------------------------- SQL Server

    final class SqlServerDialect implements JdbcDialect {

        static final String TYPE = "jdbc-sqlserver";

        @Override
        public String connectorType() {
            return TYPE;
        }

        @Override
        public String displayName() {
            return "Microsoft SQL Server";
        }

        @Override
        public String urlExample() {
            return "jdbc:sqlserver://host:1433;databaseName=database;encrypt=true";
        }

        @Override
        public String quote(String identifier) {
            return '[' + identifier + ']';
        }

        @Override
        public String writeStatement(String table, List<String> columns, List<String> keys,
                                     JdbcConfig.WriteMode mode) {
            if (mode == JdbcConfig.WriteMode.INSERT) {
                return insertPrefix(this, table, columns);
            }

            // MERGE is the only idempotent write SQL Server offers. It is verbose, and it must end
            // with a semicolon — omitting it is a syntax error, which is an unusually sharp edge.
            String source = columns.stream()
                    .map(column -> "? AS " + quote(column))
                    .reduce((a, b) -> a + ", " + b)
                    .orElseThrow();

            String match = keys.stream()
                    .map(key -> "target." + quote(key) + " = source." + quote(key))
                    .reduce((a, b) -> a + " AND " + b)
                    .orElseThrow();

            StringBuilder sql = new StringBuilder("MERGE INTO ").append(table).append(" AS target")
                    .append(" USING (SELECT ").append(source).append(") AS source")
                    .append(" ON ").append(match);

            if (mode == JdbcConfig.WriteMode.UPSERT) {
                List<String> assignments = new ArrayList<>();
                for (String column : columns) {
                    if (!keys.contains(column)) {
                        assignments.add("target." + quote(column) + " = source." + quote(column));
                    }
                }
                if (!assignments.isEmpty()) {
                    sql.append(" WHEN MATCHED THEN UPDATE SET ").append(String.join(", ", assignments));
                }
            }

            sql.append(" WHEN NOT MATCHED THEN INSERT (").append(quoteAll(this, columns))
                    .append(") VALUES (")
                    .append(columns.stream().map(column -> "source." + quote(column))
                            .reduce((a, b) -> a + ", " + b).orElseThrow())
                    .append(");");

            return sql.toString();
        }
    }

    // ----------------------------------------------------------------- Oracle

    final class OracleDialect implements JdbcDialect {

        static final String TYPE = "jdbc-oracle";

        @Override
        public String connectorType() {
            return TYPE;
        }

        @Override
        public String displayName() {
            return "Oracle Database";
        }

        @Override
        public String urlExample() {
            return "jdbc:oracle:thin:@//host:1521/service";
        }

        @Override
        public String quote(String identifier) {
            // Oracle folds unquoted identifiers to upper case, so quoting a lower-case name makes
            // it a *different* column. Upper-casing here matches what an unquoted DDL produced,
            // which is what almost every Oracle schema actually contains.
            return '"' + identifier.toUpperCase(java.util.Locale.ROOT) + '"';
        }

        @Override
        public String writeStatement(String table, List<String> columns, List<String> keys,
                                     JdbcConfig.WriteMode mode) {
            if (mode == JdbcConfig.WriteMode.INSERT) {
                return insertPrefix(this, table, columns);
            }

            String source = columns.stream()
                    .map(column -> "? AS " + quote(column))
                    .reduce((a, b) -> a + ", " + b)
                    .orElseThrow();

            String match = keys.stream()
                    .map(key -> "target." + quote(key) + " = source." + quote(key))
                    .reduce((a, b) -> a + " AND " + b)
                    .orElseThrow();

            // DUAL rather than a VALUES clause: Oracle has no standalone row constructor.
            StringBuilder sql = new StringBuilder("MERGE INTO ").append(table).append(" target")
                    .append(" USING (SELECT ").append(source).append(" FROM DUAL) source")
                    .append(" ON (").append(match).append(")");

            if (mode == JdbcConfig.WriteMode.UPSERT) {
                List<String> assignments = new ArrayList<>();
                for (String column : columns) {
                    if (!keys.contains(column)) {
                        assignments.add("target." + quote(column) + " = source." + quote(column));
                    }
                }
                if (!assignments.isEmpty()) {
                    sql.append(" WHEN MATCHED THEN UPDATE SET ").append(String.join(", ", assignments));
                }
            }

            sql.append(" WHEN NOT MATCHED THEN INSERT (").append(quoteAll(this, columns))
                    .append(") VALUES (")
                    .append(columns.stream().map(column -> "source." + quote(column))
                            .reduce((a, b) -> a + ", " + b).orElseThrow())
                    .append(")");

            return sql.toString();
        }

        @Override
        public boolean requiresTransactionForStreaming() {
            return false;
        }
    }

    // ----------------------------------------------------------------- shared

    private static String insertPrefix(JdbcDialect dialect, String table, List<String> columns) {
        String placeholders = String.join(", ", java.util.Collections.nCopies(columns.size(), "?"));
        return "INSERT INTO " + table + " (" + quoteAll(dialect, columns) + ")"
                + " VALUES (" + placeholders + ")";
    }

    private static String quoteAll(JdbcDialect dialect, List<String> identifiers) {
        return identifiers.stream().map(dialect::quote).reduce((a, b) -> a + ", " + b).orElse("");
    }
}
