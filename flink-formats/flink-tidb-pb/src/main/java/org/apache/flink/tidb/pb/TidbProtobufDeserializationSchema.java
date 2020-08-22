package org.apache.flink.tidb.pb;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.tidb.TimestampFormat;
import org.apache.flink.util.Collector;
import org.apache.flink.tidb.pb.BinlogMessage.Binlog;
import java.io.IOException;

public class TidbProtobufDeserializationSchema implements DeserializationSchema<RowData> {

	private RowType rowType;
	private TypeInformation<RowData> rowDataTypeInfo;
	private boolean ignoreParseErrors;
	private TimestampFormat timestampFormatOption;
	public TidbProtobufDeserializationSchema(RowType rowType, TypeInformation<RowData> rowDataTypeInfo, boolean ignoreParseErrors, TimestampFormat timestampFormatOption) {
		this.rowType = rowType;
		this.rowDataTypeInfo = rowDataTypeInfo;
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
		Binlog binlog = Binlog.parseFrom(message);

	}

	@Override
	public boolean isEndOfStream(RowData nextElement) {
		return false;
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return null;
	}
}
