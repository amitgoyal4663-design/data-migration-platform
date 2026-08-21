package com.dmp.connector.jdbc;

import com.dmp.common.json.Json;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Base64;

/**
 * Converts between JDBC values and {@code JsonNode}.
 *
 * <p>The platform's in-flight model is JSON, which cannot represent every SQL type natively. The
 * conversions here are chosen so that information loss is either impossible or explicit:
 *
 * <ul>
 *   <li>{@code NUMERIC} and {@code DECIMAL} become {@code BigDecimal}, never {@code double}. A
 *       currency column losing precision on the way through a migration tool is the single worst
 *       thing this connector could do.</li>
 *   <li>Timestamps become ISO-8601 strings in UTC, so ordering is preserved and offsets are not
 *       silently dropped.</li>
 *   <li>Binary becomes base64, which is lossless and survives JSON.</li>
 * </ul>
 */
final class JdbcValues {

    private JdbcValues() {
    }

    /** Reads the current row into a JSON object keyed by column label. */
    static ObjectNode toJson(ResultSet rs, ResultSetMetaData meta) throws SQLException {
        ObjectNode row = Json.newObject();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            row.set(meta.getColumnLabel(i), toJson(rs, i, meta.getColumnType(i)));
        }
        return row;
    }

    private static JsonNode toJson(ResultSet rs, int index, int sqlType) throws SQLException {
        Object value = rs.getObject(index);
        if (value == null || rs.wasNull()) {
            return Json.mapper().nullNode();
        }

        return switch (sqlType) {
            case Types.BIGINT, Types.INTEGER, Types.SMALLINT, Types.TINYINT ->
                    Json.mapper().getNodeFactory().numberNode(rs.getLong(index));

            // BigDecimal deliberately, not double: a 19-digit account balance must survive.
            case Types.DECIMAL, Types.NUMERIC ->
                    Json.mapper().getNodeFactory().numberNode(rs.getBigDecimal(index));

            case Types.FLOAT, Types.REAL, Types.DOUBLE ->
                    Json.mapper().getNodeFactory().numberNode(rs.getDouble(index));

            case Types.BOOLEAN, Types.BIT ->
                    Json.mapper().getNodeFactory().booleanNode(rs.getBoolean(index));

            case Types.DATE -> {
                LocalDate date = rs.getObject(index, LocalDate.class);
                yield Json.mapper().getNodeFactory().textNode(date.toString());
            }

            case Types.TIMESTAMP -> {
                Timestamp timestamp = rs.getTimestamp(index);
                yield Json.mapper().getNodeFactory().textNode(timestamp.toInstant().toString());
            }

            case Types.TIMESTAMP_WITH_TIMEZONE -> {
                OffsetDateTime offset = rs.getObject(index, OffsetDateTime.class);
                yield Json.mapper().getNodeFactory().textNode(offset.toInstant().toString());
            }

            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> {
                byte[] bytes = rs.getBytes(index);
                yield Json.mapper().getNodeFactory()
                        .textNode(Base64.getEncoder().encodeToString(bytes));
            }

            case Types.ARRAY -> {
                Array array = rs.getArray(index);
                yield Json.mapper().valueToTree(array.getArray());
            }

            // Includes JSON and JSONB, which arrive as strings and are re-parsed so a nested
            // document stays a document rather than becoming an escaped string.
            case Types.OTHER -> parseIfJson(rs.getString(index));

            default -> Json.mapper().getNodeFactory().textNode(rs.getString(index));
        };
    }

    private static JsonNode parseIfJson(String value) {
        if (value == null) {
            return Json.mapper().nullNode();
        }
        String trimmed = value.strip();
        if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
            try {
                return Json.mapper().readTree(trimmed);
            } catch (Exception notJson) {
                // Fall through: an OTHER-typed column that merely looks like JSON stays a string.
            }
        }
        return Json.mapper().getNodeFactory().textNode(value);
    }

    /**
     * Binds a JSON value to a prepared statement parameter.
     *
     * <p>Uses {@code setObject} with the driver's own inference for most types. The exception is
     * numbers, where a {@code BigDecimal} is bound explicitly so the precision preserved on read is
     * not discarded on write.
     */
    static void bind(PreparedStatement statement, int index, JsonNode value) throws SQLException {
        if (value == null || value.isNull()) {
            statement.setObject(index, null);
            return;
        }
        if (value.isBigDecimal() || value.isDouble() || value.isFloat()) {
            statement.setBigDecimal(index, value.decimalValue());
            return;
        }
        if (value.isIntegralNumber()) {
            statement.setLong(index, value.asLong());
            return;
        }
        if (value.isBoolean()) {
            statement.setBoolean(index, value.asBoolean());
            return;
        }
        if (value.isObject() || value.isArray()) {
            // Handed over as text; the driver casts it into a json/jsonb column.
            statement.setString(index, value.toString());
            return;
        }

        String text = value.asText();
        Object temporal = asTemporal(text);
        if (temporal != null) {
            statement.setObject(index, temporal);
            return;
        }
        statement.setString(index, text);
    }

    /**
     * Recognises ISO-8601 text so it binds as a temporal rather than a string.
     *
     * <p>Without this, a timestamp read out as a string would be written back into a
     * {@code timestamptz} column as text and rejected by the driver — the round trip would fail on
     * exactly the columns most likely to be present.
     */
    private static Object asTemporal(String text) {
        if (text.length() < 8) {
            return null;
        }
        try {
            return LocalDate.parse(text);
        } catch (Exception notADate) {
            // Not a date; try the richer forms below.
        }
        try {
            return OffsetDateTime.parse(text);
        } catch (Exception notAnOffsetDateTime) {
            // Not an offset date-time; try an instant.
        }
        try {
            return OffsetDateTime.ofInstant(Instant.parse(text), java.time.ZoneOffset.UTC);
        } catch (Exception notAnInstant) {
            // Not an instant; try a local date-time.
        }
        try {
            return LocalDateTime.parse(text);
        } catch (Exception notATemporal) {
            return null;
        }
    }

    /** Converts a split-boundary JSON value into something comparable in SQL. */
    static Object boundaryValue(JsonNode value) {
        if (value == null || value.isNull()) {
            return null;
        }
        if (value.isIntegralNumber()) {
            return value.asLong();
        }
        if (value.isNumber()) {
            return new BigDecimal(value.asText());
        }
        Object temporal = asTemporal(value.asText());
        return temporal != null ? temporal : value.asText();
    }
}
