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
		return ChangelogMode.newBuilder()
			.addContainedKind(RowKind.INSERT)
			.addContainedKind(RowKind.UPDATE_BEFORE)
			.addContainedKind(RowKind.UPDATE_AFTER)
			.addContainedKind(RowKind.DELETE)
			.build();
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		WriteTypeEnum writeTypeEnum = null;
		String writeType = configuration.getString(RedisDynamicTableFactory.REDIS_WRITE_TYPE);
		String keyPrefix = configuration.getString(RedisDynamicTableFactory.REDIS_KEY_PREFIX, "");
		String suffix = configuration.getString(RedisDynamicTableFactory.REDIS_KEY_SUFFIX, "");
		WriteTypeEnum[] values = WriteTypeEnum.values();
		for (int i = 0; i < values.length; i++) {
			WriteTypeEnum value = values[i];
			if (value.getLabel().equals(writeType)) {
				writeTypeEnum = value;
			}
		}
		String redisHosts = configuration.getString(RedisDynamicTableFactory.REDIS_HOSTS);
		String redisAuth = configuration.getString(RedisDynamicTableFactory.REDIS_AUTH);
		boolean ignoreErrorData = configuration.getBoolean(RedisDynamicTableFactory.IGNOE_ERROR_DATA);
		Integer expire = configuration.getInteger(RedisDynamicTableFactory.REDIS_KEY_EXPIRE, Integer.MAX_VALUE);
		RedisSinkFunction redisSinkFunction = new RedisSinkFunction(redisHosts, redisAuth, writeTypeEnum, expire, keyPrefix, suffix, ignoreErrorData);
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
