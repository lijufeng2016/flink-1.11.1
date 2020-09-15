package org.apache.flink.tidb.pb;

import com.googlecode.protobuf.format.JsonFormat;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.tidb.pb.BinlogMessage.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class TidbProtobufDeserializationSchema implements DeserializationSchema<RowData> {
	private static final String OP_INSERT = "Insert";
	private static final String OP_UPDATE = "Update";
	private static final String OP_DELETE = "Delete";

	private RowType rowType;
	private TypeInformation<RowData> resultTypeInfo;
	private boolean ignoreParseErrors;

	/**
	 * Runtime converter that converts {@link JsonNode}s into
	 * objects of Flink SQL internal data structures. **/
	private final DeserializationRuntimeConverter runtimeConverter;

	public TidbProtobufDeserializationSchema(RowType rowType, TypeInformation<RowData> resultTypeInfo, boolean ignoreParseErrors) {
		this.rowType = rowType;
		this.resultTypeInfo = resultTypeInfo;
		this.ignoreParseErrors = ignoreParseErrors;
		this.runtimeConverter = createRowConverter(checkNotNull(rowType));

	}

	private DeserializationRuntimeConverter createRowConverter(RowType rowType) {
		final DeserializationRuntimeConverter[] fieldConverters = rowType.getFields().stream()
			.map(RowType.RowField::getType)
			.map(this::createConverter)
			.toArray(DeserializationRuntimeConverter[]::new);
		final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

		return jsonNode -> {
			ObjectNode node = (ObjectNode) jsonNode;
			int arity = fieldNames.length;
			GenericRowData row = new GenericRowData(arity);
			for (int i = 0; i < arity; i++) {
				String fieldName = fieldNames[i];
				JsonNode field = node.get(fieldName);
				Object convertedField = convertField(fieldConverters[i], fieldName, field);
				row.setField(i, convertedField);
			}
			return row;
		};
	}

	private Object convertField(
		DeserializationRuntimeConverter fieldConverter,
		String fieldName,
		JsonNode field) {
		if (field == null) {
			if (failOnMissingField) {
				throw new JsonParseException(
					"Could not find field with name '" + fieldName + "'.");
			} else {
				return null;
			}
		} else {
			return fieldConverter.convert(field);
		}
	}
	@Override
	public RowData deserialize(byte[] message) throws IOException {
		throw new RuntimeException(
			"Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
	}

	@Override
	public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
		int fieldCount = rowType.getFieldCount();
		Binlog binlog = Binlog.parseFrom(message);
		ObjectMapper mapper = new ObjectMapper();
		String jsonBinlog = JsonFormat.printToString(binlog);
		JsonNode jsonNode = mapper.readTree(jsonBinlog);

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
						GenericRowData insert = new GenericRowData(RowKind.INSERT, fieldCount);
						for (int k = 0; k < columnsList.size(); k++) {
							ColumnInfo columnInfo = columnInfoList.get(k);
							Column column = columnsList.get(k);
							Object value = getColumnValueString(column, columnInfo);
							insert.setField(k, value);
							out.collect(insert);
						}
					} else if (OP_UPDATE.equals(opName)) {
						List<Column> columnsList = tableMutation.getRow().getColumnsList();
						List<Column> changeColumnsList = tableMutation.getChangeRow().getColumnsList();

						GenericRowData before = new GenericRowData(RowKind.UPDATE_BEFORE, fieldCount);
						GenericRowData after = new GenericRowData(RowKind.UPDATE_AFTER, fieldCount);

						for (int k = 0; k < columnsList.size(); k++) {
							ColumnInfo columnInfo = columnInfoList.get(k);

							Column column = columnsList.get(k);
							Column changeColumn = changeColumnsList.get(k);

							Object value = getColumnValueString(column, columnInfo);
							Object changeValue = getColumnValueString(changeColumn, columnInfo);

							before.setField(k, value);
							after.setField(k, changeValue);

							out.collect(before);
							out.collect(after);
						}
					} else if (OP_DELETE.equals(opName)) {

						List<Column> columnsList = tableMutation.getRow().getColumnsList();
						GenericRowData delete = new GenericRowData(RowKind.DELETE, fieldCount);
						for (int k = 0; k < columnsList.size(); k++) {
							ColumnInfo columnInfo = columnInfoList.get(k);
							Column column = columnsList.get(k);
							Object value = getColumnValueString(column, columnInfo);
							delete.setField(k, value);
							out.collect(delete);
						}
					} else {
						if (!ignoreParseErrors) {
							throw new IOException(format(
								"Unknown \"type\" value \"%s\". The Tidb protobuf message is '%s'", opName, new String(message)));
						}
					}
				}
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

	public static Object getColumnValueString(Column column, ColumnInfo columnInfo) {
		String mysqlType = columnInfo.getMysqlType();
		Object value;
		if(mysqlType.equals("bigint")){
			value = column.getInt64Value();
		}else if(mysqlType.contains("int")){
			value = Long.valueOf(column.getInt64Value()).intValue();
		}else if(mysqlType.equals("double")){
			value = column.getDoubleValue();
		}else if(mysqlType.contains("blob")){
			value = column.getBytesValue().toByteArray();
		}else {
			value = StringData.fromString(column.getStringValue());
		}
		return value;
	}

	/**
	 * Runtime converter that converts {@link JsonNode}s into objects of Flink Table & SQL
	 * internal data structures.
	 */
	@FunctionalInterface
	interface DeserializationRuntimeConverter extends Serializable {
		Object convert(JsonNode jsonNode);
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
//		switch (type.getTypeRoot()) {
//			case NULL:
//				return jsonNode -> null;
//			case BOOLEAN:
//				return this::convertToBoolean;
//			case TINYINT:
//				return jsonNode -> Byte.parseByte(jsonNode.asText().trim());
//			case SMALLINT:
//				return jsonNode -> Short.parseShort(jsonNode.asText().trim());
//			case INTEGER:
//			case INTERVAL_YEAR_MONTH:
//				return this::convertToInt;
//			case BIGINT:
//			case INTERVAL_DAY_TIME:
//				return this::convertToLong;
//			case DATE:
//				return this::convertToDate;
//			case TIME_WITHOUT_TIME_ZONE:
//				return this::convertToTime;
//			case TIMESTAMP_WITHOUT_TIME_ZONE:
//				return this::convertToTimestamp;
//			case FLOAT:
//				return this::convertToFloat;
//			case DOUBLE:
//				return this::convertToDouble;
//			case CHAR:
//			case VARCHAR:
//				return this::convertToString;
//			case BINARY:
//			case VARBINARY:
//				return this::convertToBytes;
//			case DECIMAL:
//				return createDecimalConverter((DecimalType) type);
//			case ARRAY:
//				return createArrayConverter((ArrayType) type);
//			case MAP:
//			case MULTISET:
//				return createMapConverter((MapType) type);
//			case ROW:
//				return createRowConverter((RowType) type);
//			case RAW:
//			default:
//				throw new UnsupportedOperationException("Unsupported type: " + type);
//		}
	}

	private DeserializationRuntimeConverter wrapIntoNullableConverter(
		DeserializationRuntimeConverter converter) {
		return jsonNode -> {
			if (jsonNode == null || jsonNode.isNull()) {
				return null;
			}
			try {
				return converter.convert(jsonNode);
			} catch (Throwable t) {
				if (!ignoreParseErrors) {
					throw t;
				}
				return null;
			}
		};
	}
}
