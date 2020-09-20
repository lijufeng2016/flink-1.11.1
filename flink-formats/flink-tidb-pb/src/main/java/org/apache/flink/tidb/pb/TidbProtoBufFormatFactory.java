package org.apache.flink.tidb.pb;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class TidbProtoBufFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {

	public static final String IDENTIFIER = "tidb-pb";
	/** Formatter for SQL string representation of a time value. */
	static final DateTimeFormatter SQL_TIME_FORMAT = new DateTimeFormatterBuilder()
		.appendPattern("HH:mm:ss")
		.appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
		.toFormatter();

	/** Formatter for SQL string representation of a timestamp value (without UTC timezone). */
	static final DateTimeFormatter SQL_TIMESTAMP_FORMAT = new DateTimeFormatterBuilder()
		.append(DateTimeFormatter.ISO_LOCAL_DATE)
		.appendLiteral(' ')
		.append(SQL_TIME_FORMAT)
		.toFormatter();
	/** Formatter for ISO8601 string representation of a timestamp value (without UTC timezone). */
	static final DateTimeFormatter ISO8601_TIMESTAMP_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

	public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = ConfigOptions
		.key("ignore-parse-errors")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional flag to skip fields and rows with parse errors instead of failing;\n"
			+ "fields are set to null in case of errors, false by default");
	@Override
	public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
		FactoryUtil.validateFactoryOptions(this, formatOptions);
		final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);

		return new TidbDecodingFormat(ignoreParseErrors);
	}

	@Override
	public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
		throw new UnsupportedOperationException("Canal format doesn't support as a sink format yet.");
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return Collections.emptySet();
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(IGNORE_PARSE_ERRORS);
		return options;
	}


}
