package com.dmp.connector.jdbc;

/**
 * The concrete relational connectors.
 *
 * <p>Each is a name and a dialect; everything else — splitting, streaming, type conversion, error
 * classification, resumption — is inherited. Four connectors for one implementation is the return
 * on isolating the SQL differences into {@link JdbcDialect}, and it means a fix to the read path
 * lands in all four at once rather than in whichever one someone remembered.
 *
 * <p>Kept in one file because each class is three lines; splitting them across four files would be
 * ceremony rather than organisation.
 */
public final class Dialects {

    private Dialects() {
    }

    public static final class Postgres extends JdbcConnector {
        @Override
        protected JdbcDialect dialect() {
            return new JdbcDialect.PostgresDialect();
        }
    }

    public static final class MySql extends JdbcConnector {
        @Override
        protected JdbcDialect dialect() {
            return new JdbcDialect.MySqlDialect();
        }
    }

    public static final class SqlServer extends JdbcConnector {
        @Override
        protected JdbcDialect dialect() {
            return new JdbcDialect.SqlServerDialect();
        }
    }

    public static final class Oracle extends JdbcConnector {
        @Override
        protected JdbcDialect dialect() {
            return new JdbcDialect.OracleDialect();
        }
    }
}
