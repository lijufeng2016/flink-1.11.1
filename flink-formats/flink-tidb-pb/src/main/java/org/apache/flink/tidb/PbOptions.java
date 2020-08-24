package org.apache.flink.tidb;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class PbOptions {

	public static final ConfigOption<Boolean> FAIL_ON_MISSING_FIELD = ConfigOptions
		.key("fail-on-missing-field")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional flag to specify whether to fail if a field is missing or not, false by default");

	public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = ConfigOptions
		.key("ignore-parse-errors")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional flag to skip fields and rows with parse errors instead of failing;\n"
			+ "fields are set to null in case of errors, false by default");

	public static final ConfigOption<String> TIMESTAMP_FORMAT = ConfigOptions
		.key("timestamp-format.standard")
		.stringType()
		.defaultValue("SQL")
		.withDescription("Optional flag to specify timestamp format, SQL by default." +
			" Option ISO-8601 will parse input timestamp in \"yyyy-MM-ddTHH:mm:ss.s{precision}\" format and output timestamp in the same format." +
			" Option SQL will parse input timestamp in \"yyyy-MM-dd HH:mm:ss.s{precision}\" format and output timestamp in the same format.");

	// --------------------------------------------------------------------------------------------
	// Option enumerations
	// --------------------------------------------------------------------------------------------

	public static final String SQL = "SQL";
	public static final String ISO_8601 = "ISO-8601";

	public static final Set<String> TIMESTAMP_FORMAT_ENUM = new HashSet<>(Arrays.asList(
		SQL,
		ISO_8601
	));

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	public static TimestampFormat getTimestampFormat(ReadableConfig config){
		String timestampFormat = config.get(TIMESTAMP_FORMAT);
		switch (timestampFormat){
			case SQL:
				return TimestampFormat.SQL;
			case ISO_8601:
				return TimestampFormat.ISO_8601;
			default:
				throw new TableException(
					String.format("Unsupported timestamp format '%s'. Validator should have checked that.", timestampFormat));
		}
	}
}
