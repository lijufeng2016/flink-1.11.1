package org.apache.flink.tidb.pb;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.tidb.pb.BinlogMessage.*;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.List;


import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.flink.tidb.pb.TidbProtoBufFormatFactory.*;

public class TidbProtobufDeserializationSchema implements DeserializationSchema<RowData> {
	private static final String OP_INSERT = "Insert";
	private static final String OP_UPDATE = "Update";
	private static final String OP_DELETE = "Delete";
	private DeserializationRuntimeConverter converter;
	private TypeInformation<RowData> resultTypeInfo;
	private boolean ignoreParseErrors;

	/**
	 * Timestamp format specification which is used to parse timestamp.
	 */
	private final TimestampFormat timestampFormat;

	public TidbProtobufDeserializationSchema(RowType rowType, TypeInformation<RowData> resultTypeInfo, boolean ignoreParseErrors) {
		this.resultTypeInfo = resultTypeInfo;
		this.ignoreParseErrors = ignoreParseErrors;
		this.converter = createRowConverter(rowType);
		this.timestampFormat = null;
	}


	@Override
	public RowData deserialize(byte[] message) throws IOException {
		throw new RuntimeException(
			"Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
	}

	@Override
	public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
		try {
			Binlog binlog = Binlog.parseFrom(message);
			// only process dml sql
			if (binlog.hasDmlData()) {
				DMLData dmlData = binlog.getDmlData();
				List<Table> tablesList = dmlData.getTablesList();
				for (int i = 0; i < tablesList.size(); i++) {
					Table table = tablesList.get(i);
					List<ColumnInfo> columnInfoList = table.getColumnInfoList();
					// one table may contains multiple mutations
					List<TableMutation> mutationsList = table.getMutationsList();
					for (int j = 0; j < mutationsList.size(); j++) {
						TableMutation tableMutation = mutationsList.get(j);
						String opName = tableMutation.getType().name();
						if (OP_INSERT.equals(opName)) {
							List<Column> columnsList = tableMutation.getRow().getColumnsList();
							Tuple2<List<ColumnInfo>, List<Column>> columnsInfoAndValuesTuple = new Tuple2<>(columnInfoList, columnsList);
							RowData insert = (RowData) converter.convert(columnsInfoAndValuesTuple);
							insert.setRowKind(RowKind.INSERT);
							out.collect(insert);
						} else if (OP_UPDATE.equals(opName)) {
							List<Column> columnsList = tableMutation.getRow().getColumnsList();
							List<Column> changeColumnsList = tableMutation.getChangeRow().getColumnsList();

							Tuple2<List<ColumnInfo>, List<Column>> columnsInfoAndValuesTuple = new Tuple2<>(columnInfoList, columnsList);
							Tuple2<List<ColumnInfo>, List<Column>> changeColumnsInfoAndValuesTuple = new Tuple2<>(columnInfoList, changeColumnsList);

							RowData before = (RowData) converter.convert(changeColumnsInfoAndValuesTuple);
							RowData after = (RowData) converter.convert(columnsInfoAndValuesTuple);

							before.setRowKind(RowKind.UPDATE_BEFORE);
							after.setRowKind(RowKind.UPDATE_AFTER);

							out.collect(before);
							out.collect(after);
						} else if (OP_DELETE.equals(opName)) {
							List<Column> columnsList = tableMutation.getRow().getColumnsList();
							Tuple2<List<ColumnInfo>, List<Column>> columnsInfoAndValuesTuple = new Tuple2<>(columnInfoList, columnsList);
							RowData delete = (RowData) converter.convert(columnsInfoAndValuesTuple);
							delete.setRowKind(RowKind.DELETE);
							out.collect(delete);
						} else {
							if (!ignoreParseErrors) {
								throw new IOException(format(
									"Unknown \"type\" value \"%s\". The Tidb protobuf message is '%s'", opName, new String(message)));
							}
						}
					}
				}
			}
		} catch (Throwable t) {
			// a big try catch to protect the processing.
			if (!ignoreParseErrors) {
				throw new IOException(format(
					"Corrupt TiDb protobuf message '%s'.", new String(message)), t);
			}
		}
	}

	@Override
	public boolean isEndOfStream(RowData nextElement) {
		return false;
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return resultTypeInfo;
	}

	public static Object getColumnValue(Column column, ColumnInfo columnInfo) {
		if (column.getIsNull()) {
			return null;
		}
		String mysqlType = columnInfo.getMysqlType();
		Object value;
		if (mysqlType.equals("bigint")) {
			value = column.getInt64Value();
		} else if (mysqlType.contains("int")) {
			value = Long.valueOf(column.getInt64Value()).intValue();
		} else if (mysqlType.equals("double")) {
			value = column.getDoubleValue();
		} else if (mysqlType.contains("blob")) {
			value = column.getBytesValue().toByteArray();
		} else {
			value = StringData.fromString(column.getStringValue());
		}

		return value;
	}

	@FunctionalInterface
	private interface DeserializationRuntimeConverter extends Serializable {
		Object convert(Object object);
	}


	private Object convertField(
		DeserializationRuntimeConverter fieldConverter,
		Object value) {
		return fieldConverter.convert(value);

	}


	private DeserializationRuntimeConverter createRowConverter(RowType rowType) {
		final DeserializationRuntimeConverter[] fieldConverters = rowType.getFields().stream()
			.map(RowType.RowField::getType)
			.map(this::createConverter)
			.toArray(DeserializationRuntimeConverter[]::new);
		final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

		return value -> {
			Tuple2<List<ColumnInfo>, List<Column>> columnsInfoAndValuesTuple = (Tuple2<List<ColumnInfo>, List<Column>>) value;
			List<Column> columnList = columnsInfoAndValuesTuple.f1;
			List<ColumnInfo> columnInfoList = columnsInfoAndValuesTuple.f0;
			int arity = fieldNames.length;
			GenericRowData row = new GenericRowData(arity);
			for (int i = 0; i < arity; i++) {
				Column column = columnList.get(i);
				ColumnInfo columnInfo = columnInfoList.get(i);
				Object columnValue = getColumnValue(column, columnInfo);
				Object convertedField = convertField(fieldConverters[i], columnValue);
				row.setField(i, convertedField);
			}
			return row;
		};
	}

	private DeserializationRuntimeConverter wrapIntoNullableConverter(
		DeserializationRuntimeConverter converter) {
		return object -> {
			if (object == null) {
				return null;
			}
			try {
				return converter.convert(object);
			} catch (Throwable t) {
				if (!ignoreParseErrors) {
					throw t;
				}
				return null;
			}
		};
	}

	/**
	 * Creates a runtime converter which is null safe.
	 */
	private DeserializationRuntimeConverter createConverter(LogicalType type) {
		return wrapIntoNullableConverter(createNotNullConverter(type));
	}

	/**
	 * Creates a runtime converter which assuming input object is not null.
	 */
	private DeserializationRuntimeConverter createNotNullConverter(LogicalType type) {
		switch (type.getTypeRoot()) {
			case NULL:
				return value -> null;
			case BOOLEAN:
				return value -> Boolean.parseBoolean(value.toString());
			case TINYINT:
				return value -> Byte.parseByte(value.toString());
			case SMALLINT:
				return value -> Short.parseShort(value.toString());
			case INTEGER:
			case INTERVAL_YEAR_MONTH:
				return value -> Integer.parseInt(value.toString());
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return value -> Long.parseLong(value.toString());
			case DATE:
				return this::convertToDate;
			case TIME_WITHOUT_TIME_ZONE:
				return this::convertToTime;
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return this::convertToTimestamp;
			case FLOAT:
				return value -> Float.parseFloat(value.toString());
			case DOUBLE:
				return value -> Double.parseDouble(value.toString());
			case CHAR:
			case VARCHAR:
				return value -> StringData.fromString(value.toString());
			case BINARY:
			case VARBINARY:
				return this::convertToBytes;
			case DECIMAL:
				return createDecimalConverter((DecimalType) type);
			default:
				throw new UnsupportedOperationException("Unsupported type: " + type);
		}
	}


	private int convertToDate(Object value) {
		LocalDate date = ISO_LOCAL_DATE.parse(value.toString()).query(TemporalQueries.localDate());
		return (int) date.toEpochDay();
	}

	private int convertToTime(Object value) {
		TemporalAccessor parsedTime = SQL_TIME_FORMAT.parse(value.toString());
		LocalTime localTime = parsedTime.query(TemporalQueries.localTime());
		// get number of milliseconds of the day
		return localTime.toSecondOfDay() * 1000;
	}

	private TimestampData convertToTimestamp(Object value) {
		TemporalAccessor parsedTimestamp;
		switch (timestampFormat) {
			case SQL:
				parsedTimestamp = SQL_TIMESTAMP_FORMAT.parse(value.toString());
				break;
			case ISO_8601:
				parsedTimestamp = ISO8601_TIMESTAMP_FORMAT.parse(value.toString());
				break;
			default:
				throw new TableException(String.format("Unsupported timestamp format '%s'. Validator should have checked that.", timestampFormat));
		}
		LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
		LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());

		return TimestampData.fromLocalDateTime(LocalDateTime.of(localDate, localTime));
	}

	private byte[] convertToBytes(Object value) {
		byte[] bytes = value.toString().getBytes();
		return bytes;
	}

	private DeserializationRuntimeConverter createDecimalConverter(DecimalType decimalType) {
		final int precision = decimalType.getPrecision();
		final int scale = decimalType.getScale();
		return value -> {
			BigDecimal bigDecimal = new BigDecimal(value.toString());
			return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
		};
	}
}


