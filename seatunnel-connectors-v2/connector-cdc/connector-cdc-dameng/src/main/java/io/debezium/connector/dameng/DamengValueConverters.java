/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.debezium.connector.dameng;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import dm.jdbc.driver.DmdbIntervalDT;
import dm.jdbc.driver.DmdbTimestamp;
import dm.jdbc.driver.DmdbType;
import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.dameng.logminer.BlobChunkList;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.ResultReceiver;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.time.Date;
import io.debezium.time.MicroDuration;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.HexConverter;
import io.debezium.util.NumberConversions;
import io.debezium.util.Strings;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.debezium.util.NumberConversions.BYTE_FALSE;
import static io.debezium.util.NumberConversions.BYTE_ZERO;

public class DamengValueConverters extends JdbcValueConverters {

    private static final Pattern INTERVAL_DAY_SECOND_PATTERN =
            Pattern.compile("([+\\-])?(\\d+) (\\d+):(\\d+):(\\d+).(\\d+)");

    private static final ZoneId GMT_ZONE_ID = ZoneId.of("GMT");

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                    .optionalStart()
                    .appendPattern(".")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
                    .optionalEnd()
                    .toFormatter();

    private static final DateTimeFormatter TIMESTAMP_AM_PM_SHORT_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern("dd-MMM-yy hh.mm.ss")
                    .optionalStart()
                    .appendPattern(".")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
                    .optionalEnd()
                    .appendPattern(" a")
                    .toFormatter(Locale.ENGLISH);

    private static final DateTimeFormatter TIMESTAMP_TZ_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                    .optionalStart()
                    .appendPattern(".")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
                    .optionalEnd()
                    .optionalStart()
                    .appendPattern(" ")
                    .optionalEnd()
                    .appendOffset("+HH:MM", "")
                    .toFormatter();

    private static final String EMPTY_BLOB_FUNCTION = "EMPTY_BLOB()";
    private static final String EMPTY_CLOB_FUNCTION = "EMPTY_CLOB()";
    private static final String HEXTORAW_FUNCTION_START = "HEXTORAW('";
    private static final String HEXTORAW_FUNCTION_END = "')";

    private static final Pattern TO_TIMESTAMP =
            Pattern.compile("TO_TIMESTAMP\\('(.*)'\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern TO_TIMESTAMP_TZ =
            Pattern.compile("TO_TIMESTAMP_TZ\\('(.*)'\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern TO_DATE =
            Pattern.compile("TO_DATE\\('(.*)',[ ]*'(.*)'\\)", Pattern.CASE_INSENSITIVE);

    private final DamengConnection connection;
    private final boolean lobEnabled;

    public DamengValueConverters(DamengConnectorConfig config, DamengConnection connection) {
        super(
                config.getDecimalMode(),
                config.getTemporalPrecisionMode(),
                ZoneOffset.UTC,
                null,
                null,
                null);
        this.connection = connection;
        this.lobEnabled = config.isLobEnabled();
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        logger.debug(
                "Building schema for column {} of type {} named {} with constraints ({},{})",
                column.name(),
                column.jdbcType(),
                column.typeName(),
                column.length(),
                column.scale());

        switch (column.jdbcType()) {
                // Oracle's float is not float as in Java but a NUMERIC without scale
            case Types.FLOAT:
                return variableScaleSchema(column);
            case Types.NUMERIC:
                return getNumericSchema(column);
            case DmdbType.REAL:
                return SchemaBuilder.float32();
            case DmdbType.DOUBLE:
                return SchemaBuilder.float64();
            case DmdbType.DATETIME_TZ:
            case DmdbType.DATETIME2_TZ:
                return ZonedTimestamp.builder();
            case DmdbType.INTERVAL_YM:
            case DmdbType.INTERVAL_DT:
                return MicroDuration.builder();
            case Types.STRUCT:
                return SchemaBuilder.string();
            default:
                {
                    SchemaBuilder builder = super.schemaBuilder(column);
                    logger.debug(
                            "JdbcValueConverters returned '{}' for column '{}'",
                            builder != null ? builder.getClass().getName() : null,
                            column.name());
                    return builder;
                }
        }
    }

    private SchemaBuilder getNumericSchema(Column column) {
        if (column.scale().isPresent()) {
            // return sufficiently sized int schema for non-floating point types
            Integer scale = column.scale().get();

            // a negative scale means rounding, e.g. NUMBER(10, -2) would be rounded to hundreds
            if (scale <= 0) {
                int width = column.length() - scale;
                if (width < 3) {
                    return SchemaBuilder.int8();
                } else if (width < 5) {
                    return SchemaBuilder.int16();
                } else if (width < 10) {
                    return SchemaBuilder.int32();
                } else if (width < 19) {
                    return SchemaBuilder.int64();
                }
            }

            // larger non-floating point types and floating point types use Decimal
            return super.schemaBuilder(column);
        } else {
            return variableScaleSchema(column);
        }
    }

    private SchemaBuilder variableScaleSchema(Column column) {
        if (decimalMode == DecimalMode.PRECISE) {
            return VariableScaleDecimal.builder();
        }
        return SpecialValueDecimal.builder(decimalMode, column.length(), column.scale().orElse(-1));
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        switch (column.jdbcType()) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.STRUCT:
            case Types.CLOB:
                return data -> convertString(column, fieldDefn, data);
            case Types.BLOB:
                return data -> convertBinary(column, fieldDefn, data, binaryMode);
            case DmdbType.REAL:
                return data -> convertFloat(column, fieldDefn, data);
            case DmdbType.DOUBLE:
                return data -> convertDouble(column, fieldDefn, data);
            case Types.NUMERIC:
                return getNumericConverter(column, fieldDefn);
            case Types.FLOAT:
                return data -> convertVariableScale(column, fieldDefn, data);
            case DmdbType.DATETIME_TZ:
            case DmdbType.DATETIME2_TZ:
                return (data) -> convertTimestampWithZone(column, fieldDefn, data);
            case DmdbType.INTERVAL_YM:
                return (data) -> convertIntervalYearMonth(column, fieldDefn, data);
            case DmdbType.INTERVAL_DT:
                return (data) -> convertIntervalDaySecond(column, fieldDefn, data);
        }

        return super.converter(column, fieldDefn);
    }

    private ValueConverter getNumericConverter(Column column, Field fieldDefn) {
        if (column.scale().isPresent()) {
            Integer scale = column.scale().get();

            if (scale <= 0) {
                int width = column.length() - scale;
                if (width < 3) {
                    return data -> convertNumericAsTinyInt(column, fieldDefn, data);
                } else if (width < 5) {
                    return data -> convertNumericAsSmallInt(column, fieldDefn, data);
                } else if (width < 10) {
                    return data -> convertNumericAsInteger(column, fieldDefn, data);
                } else if (width < 19) {
                    return data -> convertNumericAsBigInteger(column, fieldDefn, data);
                }
            }

            // larger non-floating point types and floating point types use Decimal
            return data -> convertNumeric(column, fieldDefn, data);
        } else {
            return data -> convertVariableScale(column, fieldDefn, data);
        }
    }

    @Override
    protected Object convertString(Column column, Field fieldDefn, Object data) {
        if (data instanceof Clob) {
            if (!lobEnabled) {
                return null;
            }
            try {
                Clob clob = (Clob) data;
                // Note that java.sql.Clob specifies that the first character starts at 1
                // and that length must be greater-than or equal to 0. So for an empty
                // clob field, a call to getSubString(1, 0) is perfectly valid.
                return clob.getSubString(1, (int) clob.length());
            } catch (SQLException e) {
                throw new DebeziumException(
                        "Couldn't convert value for column " + column.name(), e);
            }
        }
        if (data instanceof String) {
            String s = (String) data;
            if (EMPTY_CLOB_FUNCTION.equals(s)) {
                return column.isOptional() ? null : "";
            }
        }

        return super.convertString(column, fieldDefn, data);
    }

    @Override
    protected Object convertBinary(
            Column column,
            Field fieldDefn,
            Object data,
            CommonConnectorConfig.BinaryHandlingMode mode) {
        try {
            if (data instanceof String) {
                String str = (String) data;
                if (EMPTY_BLOB_FUNCTION.equals(str)) {
                    if (column.isOptional()) {
                        return null;
                    }
                    data = "";
                } else if (((String) data).startsWith("0x0")) {
                    data = ((String) data).substring(3);
                } else if (((String) data).startsWith("0x")) {
                    data = ((String) data).substring(2);
                }
            } else if (data instanceof BlobChunkList) {
                if (!lobEnabled) {
                    return null;
                }
                data = convertBlobChunkList((BlobChunkList) data);
            } else if (data instanceof Blob) {
                if (!lobEnabled) {
                    return null;
                }
                Blob blob = (Blob) data;
                data = blob.getBytes(1, Long.valueOf(blob.length()).intValue());
            }

            return super.convertBinary(column, fieldDefn, data, mode);
        } catch (SQLException e) {
            throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
        }
    }

    @Override
    protected Object convertBinaryToBytes(Column column, Field fieldDefn, Object data) {
        return convertValue(
                column,
                fieldDefn,
                data,
                BYTE_ZERO,
                (r) -> {
                    if (data instanceof String) {
                        r.deliver(((String) data).getBytes(StandardCharsets.UTF_8));
                    } else if (data instanceof char[]) {
                        r.deliver(new String((char[]) data).getBytes(StandardCharsets.UTF_8));
                    } else if (data instanceof byte[]) {
                        r.deliver((byte[]) data);
                    } else {
                        // An unexpected value
                        r.deliver(unexpectedBinary(data, fieldDefn));
                    }
                });
    }

    private byte[] convertBlobChunkList(BlobChunkList chunks) throws SQLException {
        if (chunks.isEmpty()) {
            // if there are no chunks, simply return null
            return null;
        }

        // Iterate each chunk's hex-string and combine them together.
        final StringBuilder hexString = new StringBuilder();
        for (String chunk : chunks) {
            hexString.append(getHexToRawHexString(chunk));
        }

        return HexConverter.convertFromHex(hexString.toString());
    }

    @Override
    protected Object convertInteger(Column column, Field fieldDefn, Object data) {
        return super.convertInteger(column, fieldDefn, data);
    }

    @Override
    protected Object convertFloat(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            return Float.parseFloat((String) data);
        }

        return super.convertFloat(column, fieldDefn, data);
    }

    @Override
    protected Object convertDouble(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            return Double.parseDouble((String) data);
        }

        return super.convertDouble(column, fieldDefn, data);
    }

    @Override
    protected Object convertDecimal(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            // In the case when the value is of String, convert it to a BigDecimal so that we can
            // then
            // aptly apply the scale adjustment below.
            data = toBigDecimal(column, fieldDefn, data);
        }

        // adjust scale to column's scale if the column's scale is larger than the one from
        // the value (e.g. 4.4444 -> 4.444400)
        if (data instanceof BigDecimal) {
            data = withScaleAdjustedIfNeeded(column, (BigDecimal) data);
        }

        // When SimpleDmlParser is removed, the following block can be removed.
        // This is necessitated by the fact SimpleDmlParser invokes the converters internally and
        // won't be needed when that parser is no longer part of the source.
        if (data instanceof Struct) {
            SpecialValueDecimal value = VariableScaleDecimal.toLogical((Struct) data);
            return value.getDecimalValue().orElse(null);
        }

        return super.convertDecimal(column, fieldDefn, data);
    }

    @Override
    protected Object convertNumeric(Column column, Field fieldDefn, Object data) {
        return convertDecimal(column, fieldDefn, data);
    }

    protected Object convertNumericAsTinyInt(Column column, Field fieldDefn, Object data) {
        return convertTinyInt(column, fieldDefn, data);
    }

    protected Object convertNumericAsSmallInt(Column column, Field fieldDefn, Object data) {
        return super.convertSmallInt(column, fieldDefn, data);
    }

    protected Object convertNumericAsInteger(Column column, Field fieldDefn, Object data) {
        return super.convertInteger(column, fieldDefn, data);
    }

    protected Object convertNumericAsBigInteger(Column column, Field fieldDefn, Object data) {
        return super.convertBigInt(column, fieldDefn, data);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#BOOLEAN}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type;
     *     never null
     * @return the converted value, or null if the conversion could not be made and the column
     *     allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not
     *     allow nulls
     */
    @Override
    protected Object convertBoolean(Column column, Field fieldDefn, Object data) {
        if (data instanceof BigDecimal) {
            return ((BigDecimal) data).byteValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        }
        if (data instanceof String) {
            return Byte.parseByte((String) data) == 0 ? Boolean.FALSE : Boolean.TRUE;
        }
        return super.convertBoolean(column, fieldDefn, data);
    }

    @Override
    protected Object convertTinyInt(Column column, Field fieldDefn, Object data) {
        return convertValue(
                column,
                fieldDefn,
                data,
                BYTE_FALSE,
                (r) -> {
                    if (data instanceof Byte) {
                        r.deliver(data);
                    } else if (data instanceof Number) {
                        Number value = (Number) data;
                        r.deliver(value.byteValue());
                    } else if (data instanceof Boolean) {
                        r.deliver(NumberConversions.getByte((boolean) data));
                    } else if (data instanceof String) {
                        r.deliver(Byte.parseByte((String) data));
                    }
                });
    }

    protected Object convertVariableScale(Column column, Field fieldDefn, Object data) {
        data = convertNumeric(column, fieldDefn, data); // provides default value

        if (data == null) {
            return null;
        }
        // TODO Need to handle special values, it is not supported in variable scale decimal
        if (decimalMode == DecimalMode.PRECISE) {
            if (data instanceof SpecialValueDecimal) {
                return VariableScaleDecimal.fromLogical(
                        fieldDefn.schema(), (SpecialValueDecimal) data);
            } else if (data instanceof BigDecimal) {
                return VariableScaleDecimal.fromLogical(
                        fieldDefn.schema(), new SpecialValueDecimal((BigDecimal) data));
            }
        } else {
            return data;
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    protected Object fromOracleTimeClasses(Column column, Object data) {
        if (data instanceof DmdbTimestamp) {
            data = ((DmdbTimestamp) data).toLocalDateTime();
        }
        return data;
    }

    @Override
    protected Object convertTimestampToEpochMillisAsDate(
            Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            data = resolveTimestampStringAsInstant((String) data);
        }
        return super.convertTimestampToEpochMillisAsDate(
                column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    @Override
    protected Object convertTimestampToEpochMicros(Column column, Field fieldDefn, Object data) {
        if (data instanceof Long) {
            return data;
        }
        if (data instanceof String) {
            data = resolveTimestampStringAsInstant((String) data);
        }
        return super.convertTimestampToEpochMicros(
                column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    @Override
    protected Object convertTimestampToEpochMillis(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            data = resolveTimestampStringAsInstant((String) data);
        }
        return super.convertTimestampToEpochMillis(
                column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    @Override
    protected Object convertTimestampToEpochNanos(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            data = resolveTimestampStringAsInstant((String) data);
        }
        return super.convertTimestampToEpochNanos(
                column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    private Instant resolveTimestampStringAsInstant(String data) {
        LocalDateTime dateTime;

        final Matcher toTimestampMatcher = TO_TIMESTAMP.matcher(data);
        if (toTimestampMatcher.matches()) {
            String dateText = toTimestampMatcher.group(1);
            if (dateText.indexOf(" AM") > 0 || dateText.indexOf(" PM") > 0) {
                dateTime =
                        LocalDateTime.from(TIMESTAMP_AM_PM_SHORT_FORMATTER.parse(dateText.trim()));
            } else {
                dateTime = LocalDateTime.from(TIMESTAMP_FORMATTER.parse(dateText.trim()));
            }
            return dateTime.atZone(GMT_ZONE_ID).toInstant();
        }

        final Matcher toDateMatcher = TO_DATE.matcher(data);
        if (toDateMatcher.matches()) {
            dateTime = LocalDateTime.from(TIMESTAMP_FORMATTER.parse(toDateMatcher.group(1)));
            return dateTime.atZone(GMT_ZONE_ID).toInstant();
        }

        // Unable to resolve
        return null;
    }

    @Override
    protected Object convertTimestampWithZone(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            final Matcher toTimestampTzMatcher = TO_TIMESTAMP_TZ.matcher((String) data);
            if (toTimestampTzMatcher.matches()) {
                String dateText = toTimestampTzMatcher.group(1);
                data = ZonedDateTime.from(TIMESTAMP_TZ_FORMATTER.parse(dateText.trim()));
            }
        }
        return super.convertTimestampWithZone(
                column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    protected Object convertIntervalYearMonth(Column column, Field fieldDefn, Object data) {
        return convertValue(
                column,
                fieldDefn,
                data,
                NumberConversions.LONG_FALSE,
                (r) -> {
                    if (data instanceof Number) {
                        // we expect to get back from the plugin a double value
                        r.deliver(((Number) data).longValue());
                    }
                });
    }

    protected Object convertIntervalDaySecond(Column column, Field fieldDefn, Object data) {
        return convertValue(
                column,
                fieldDefn,
                data,
                NumberConversions.LONG_FALSE,
                (r) -> {
                    if (data instanceof Number) {
                        // we expect to get back from the plugin a double value
                        r.deliver(((Number) data).longValue());
                    } else if (data instanceof DmdbIntervalDT) {
                        convertOracleIntervalDaySecond(data, r);
                    }
                });
    }

    private void convertOracleIntervalDaySecond(Object data, ResultReceiver r) {
        final String interval = ((DmdbIntervalDT) data).toString();
        final Matcher m = INTERVAL_DAY_SECOND_PATTERN.matcher(interval);
        if (m.matches()) {
            final int sign = "-".equals(m.group(1)) ? -1 : 1;
            r.deliver(
                    MicroDuration.durationMicros(
                            0,
                            0,
                            sign * Integer.valueOf(m.group(2)),
                            sign * Integer.valueOf(m.group(3)),
                            sign * Integer.valueOf(m.group(4)),
                            sign * Integer.valueOf(m.group(5)),
                            sign * Integer.valueOf(Strings.pad(m.group(6), 6, '0')),
                            MicroDuration.DAYS_PER_MONTH_AVG));
        }
    }

    /**
     * Get the {@code HEXTORAW} function argument, removing the function call prefix/suffix if
     * present.
     *
     * @param hexToRawValue the hex-to-raw string, optionally wrapped by the function call, never
     *     {@code null}
     * @return the hex-to-raw argument, never {@code null}.
     */
    private String getHexToRawHexString(String hexToRawValue) {
        if (isHexToRawFunctionCall(hexToRawValue)) {
            return hexToRawValue.substring(10, hexToRawValue.length() - 2);
        }
        return hexToRawValue;
    }

    /**
     * Returns whether the provided value is a {@code HEXTORAW} function, format {@code
     * HEXTORAW('<hex>')}.
     *
     * @param value the value to inspect and validate, may be {@code null}
     * @return true if the value is a {@code HEXTORAW} function call; false otherwise.
     */
    private boolean isHexToRawFunctionCall(String value) {
        return value != null
                && value.startsWith(HEXTORAW_FUNCTION_START)
                && value.endsWith(HEXTORAW_FUNCTION_END);
    }
}
