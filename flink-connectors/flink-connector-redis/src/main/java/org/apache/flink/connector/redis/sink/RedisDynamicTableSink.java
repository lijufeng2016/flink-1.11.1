package org.apache.flink.connector.redis.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.redis.util.WriteTypeEnum;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;

/**
 * RedisDynamicTableSink.
 */
public class RedisDynamicTableSink implements DynamicTableSink {
	private Configuration configuration;

	public RedisDynamicTableSink(Configuration configuration) {
		this.configuration = configuration;
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		ChangelogMode.Builder builder = ChangelogMode.newBuilder();
		for (RowKind kind : requestedMode.getContainedKinds()) {
			if (kind != RowKind.UPDATE_BEFORE) {
				builder.addContainedKind(kind);
			}
		}
		return builder.build();
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		WriteTypeEnum writeTypeEnum = null;
		String writeType = configuration.getString(RedisDynamicTableFactory.REDIS_WRITE_TYPE);
		WriteTypeEnum[] values = WriteTypeEnum.values();
		for (int i = 0; i < values.length; i++) {
			WriteTypeEnum value = values[i];
			if (value.getLabel().equals(writeType)) {
				writeTypeEnum = value;
			}
		}
		String redisHosts = configuration.getString(RedisDynamicTableFactory.REDIS_HOSTS);
		String redisAuth = configuration.getString(RedisDynamicTableFactory.REDIS_AUTH);
		Integer expire = configuration.getInteger(RedisDynamicTableFactory.REDIS_KEY_EXPIRE, Integer.MAX_VALUE);
		RedisSinkFunction redisSinkFunction = new RedisSinkFunction(redisHosts, redisAuth, writeTypeEnum, expire);
		return SinkFunctionProvider.of(redisSinkFunction);
	}

	@Override
	public DynamicTableSink copy() {
		return new RedisDynamicTableSink(configuration);
	}

	@Override
	public String asSummaryString() {
		return "Redis";
	}
}
