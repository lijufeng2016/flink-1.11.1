package org.apache.flink.connector.redis.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.redis.util.WriteTypeEnum;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/**
 * RedisDynamicTableFactory.
 */
public class RedisDynamicTableFactory implements DynamicTableSinkFactory {

	protected static final String IDENTIFIER = "redis";


	protected static final ConfigOption<String> REDIS_HOSTS = ConfigOptions
		.key("redis.hosts")
		.stringType()
		.noDefaultValue()
		.withDescription("redis集群地址host1:p1,host2:p2...逗号隔开");

	protected static final ConfigOption<String> REDIS_AUTH = ConfigOptions
		.key("redis.auth")
		.stringType()
		.noDefaultValue()
		.withDescription("redis集群密码");

	protected static final ConfigOption<String> REDIS_WRITE_TYPE = ConfigOptions
		.key("redis.write.type")
		.stringType()
		.defaultValue("set")
		.withDescription("Redis写入集群的类型，有：set,lpush,rpush,hset,del,hdel");

	protected static final ConfigOption<Integer> REDIS_KEY_EXPIRE = ConfigOptions
		.key("redis.key.expire")
		.intType()
		.noDefaultValue()
		.withDescription("redis key的过期时间,单位秒");

	protected static final ConfigOption<String> REDIS_KEY_PREFIX = ConfigOptions
		.key("redis.key.prefix")
		.stringType()
		.noDefaultValue()
		.withDescription("redis key的前缀");

	protected static final ConfigOption<String> REDIS_KEY_SUFFIX = ConfigOptions
		.key("redis.key.suffix")
		.stringType()
		.noDefaultValue()
		.withDescription("redis key的后缀");

	protected static final ConfigOption<Boolean> IGNOE_ERROR_DATA = ConfigOptions
		.key("ignore-parse-errors")
		.booleanType()
		.defaultValue(Boolean.FALSE)
		.withDescription("忽略错误的数据");

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
		helper.validate();
		Configuration configuration = new Configuration();
		context.getCatalogTable()
			.getOptions()
			.forEach(configuration::setString);
		validateParams(configuration);

		return new RedisDynamicTableSink(configuration);
	}

	/**
	 * 根据规则验证参数.
	 *
	 * @param configuration 配置
	 */
	private void validateParams(Configuration configuration) {
		String writeType = configuration.getString(REDIS_WRITE_TYPE);
		Integer expire = configuration.getInteger(REDIS_KEY_EXPIRE, Integer.MAX_VALUE);
		String keyPrefix = configuration.getString(REDIS_KEY_PREFIX);
		String keySuffix = configuration.getString(REDIS_KEY_SUFFIX);

		// 如果是删除操作，不能有过期时间和keyPrefix、keySuffix
		if (writeType.equals(WriteTypeEnum.DEL.getLabel()) || writeType.equals(WriteTypeEnum.HDEL.getLabel())) {
			if (Integer.MAX_VALUE != expire || StringUtils.isNotBlank(keyPrefix) || StringUtils.isNotBlank(keySuffix)) {
				throw new IllegalArgumentException("del和hdel操作不允许keyPrefix、keySuffix、expire");
			}
		}
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> set = new HashSet<>();
		set.add(REDIS_HOSTS);
		set.add(REDIS_AUTH);
		set.add(REDIS_WRITE_TYPE);
		return set;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> set = new HashSet<>();
		set.add(REDIS_KEY_PREFIX);
		set.add(REDIS_KEY_SUFFIX);
		set.add(REDIS_KEY_EXPIRE);
		set.add(IGNOE_ERROR_DATA);
		return set;
	}
}
