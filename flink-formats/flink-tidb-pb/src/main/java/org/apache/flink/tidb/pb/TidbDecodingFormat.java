package org.apache.flink.tidb.pb;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

public class TidbDecodingFormat implements DecodingFormat<DeserializationSchema<RowData>> {
	private boolean ignoreParseErrors;
	private String filterTablename;
	public TidbDecodingFormat(boolean ignoreParseErrors, String filterTablename) {
		this.ignoreParseErrors = ignoreParseErrors;
		this.filterTablename = filterTablename;
	}

	@Override
	public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType producedDataType) {
		final RowType rowType = (RowType) producedDataType.getLogicalType();
		final TypeInformation<RowData> rowDataTypeInfo =
			(TypeInformation<RowData>) context.createTypeInformation(producedDataType);
		return new TidbProtobufDeserializationSchema(
			rowType,
			rowDataTypeInfo,
			ignoreParseErrors,
			filterTablename);
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.newBuilder()
			.addContainedKind(RowKind.INSERT)
			.addContainedKind(RowKind.UPDATE_BEFORE)
			.addContainedKind(RowKind.UPDATE_AFTER)
			.addContainedKind(RowKind.DELETE)
			.build();
	}
}
