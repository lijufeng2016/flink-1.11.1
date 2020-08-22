package org.apache.flink.tidb.pb;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.tidb.TimestampFormat;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.tidb.pb.BinlogMessage.*;

import java.io.IOException;
import java.util.List;

import static java.lang.String.format;

public class TidbProtobufDeserializationSchema implements DeserializationSchema<RowData> {
	private static final String OP_INSERT = "Insert";
	private static final String OP_UPDATE = "Update";
	private static final String OP_DELETE = "Delete";

	private RowType rowType;
	private TypeInformation<RowData> resultTypeInfo;
	private boolean ignoreParseErrors;
	// TODO
	private TimestampFormat timestampFormatOption;

	public TidbProtobufDeserializationSchema(RowType rowType, TypeInformation<RowData> resultTypeInfo, boolean ignoreParseErrors, TimestampFormat timestampFormatOption) {
		this.rowType = rowType;
		this.resultTypeInfo = resultTypeInfo;
		this.ignoreParseErrors = ignoreParseErrors;
		this.timestampFormatOption = timestampFormatOption;
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
		// only process dml sql
		if (binlog.hasDmlData()) {
			DMLData dmlData = binlog.getDmlData();
			List<Table> tablesList = dmlData.getTablesList();
			for (int i = 0; i < tablesList.size(); i++) {
				Table table = tablesList.get(i);
				// one table may contains multiple mutations
				List<TableMutation> mutationsList = table.getMutationsList();
				for (int j = 0; j < mutationsList.size(); j++) {
					TableMutation tableMutation = mutationsList.get(j);
					String opName = tableMutation.getType().name();
					if (OP_INSERT.equals(opName)) {
						List<Column> columnsList = tableMutation.getRow().getColumnsList();
						GenericRowData insert = new GenericRowData(RowKind.INSERT, fieldCount);
						for (int k = 0; k < columnsList.size(); k++) {
							Column column = columnsList.get(k);
							insert.setField(k, column.getStringValue());
							out.collect(insert);
						}
					} else if (OP_UPDATE.equals(opName)) {
						List<Column> columnsList = tableMutation.getRow().getColumnsList();
						List<Column> changeColumnsList = tableMutation.getChangeRow().getColumnsList();

						GenericRowData before = new GenericRowData(RowKind.UPDATE_BEFORE, fieldCount);
						GenericRowData after = new GenericRowData(RowKind.UPDATE_AFTER, fieldCount);
						for (int k = 0; k < columnsList.size(); k++) {
							Column column = columnsList.get(k);
							Column changeColumn = changeColumnsList.get(k);

							before.setField(k, column.getStringValue());
							after.setField(k, changeColumn.getStringValue());

							out.collect(before);
							out.collect(after);
						}
					} else if (OP_DELETE.equals(opName)) {
						List<Column> columnsList = tableMutation.getRow().getColumnsList();
						GenericRowData delete = new GenericRowData(RowKind.DELETE, fieldCount);
						for (int k = 0; k < columnsList.size(); k++) {
							Column column = columnsList.get(k);
							delete.setField(k, column.getStringValue());
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
}
