package org.apache.flink.connector.redis.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.redis.util.JedisUtil;
import org.apache.flink.connector.redis.util.WriteTypeEnum;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisException;

/**
 * RedisSinkFunction.
 */

public class RedisSinkFunction extends RichSinkFunction<RowData> {
	private String redisHost;
	private String auth;
	private WriteTypeEnum writeTypeEnum;
	private Integer expire;
	private JedisCluster jedisCluster;
	private RedisOperator operator;
	private String keyPrefix;
	private String suffix;
	private boolean ignoreErrorData;

	private static final Logger LOG = LoggerFactory.getLogger(RedisSinkFunction.class);

	public RedisSinkFunction(String redisHosts, String auth, WriteTypeEnum writeTypeEnum, Integer expire, String keyPrefix, String suffix, boolean ignoreErrorData) {
		this.redisHost = redisHosts;
		this.auth = auth;
		this.writeTypeEnum = writeTypeEnum;
		this.expire = expire;
		this.keyPrefix = keyPrefix;
		this.suffix = suffix;
		this.ignoreErrorData = ignoreErrorData;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		jedisCluster = JedisUtil.getJedisCluster(redisHost, auth);
		operator = new RedisOperator(jedisCluster, keyPrefix, suffix);
		super.open(parameters);
	}

	@Override
	public void invoke(RowData rowData, Context context) throws Exception {
		try {
			switch (writeTypeEnum) {
				case CDC:
					operator.cdc(rowData, expire);
					break;
				case DEL:
					operator.del(rowData);
					break;
				case SET:
					operator.set(rowData, expire);
					break;
				case HDEL:
					operator.hdel(rowData);
					break;
				case HSET:
					operator.hset(rowData, expire);
					break;
				case LPUSH:
					operator.lpush(rowData, expire);
					break;
				case RPUSH:
					operator.rpush(rowData, expire);
					break;
				default:
					throw new IllegalArgumentException("请设置正确的写入方法，参数名：redis.write.type");
			}
		} catch (JedisException e) {
			if (ignoreErrorData) {
				LOG.warn("忽略错误的数据：{0}", rowData.toString());
			} else {
				throw e;
			}
		} catch (IllegalArgumentException e) {
			throw e;
		}

	}

	@Override
	public void close() throws Exception {
		jedisCluster.close();
	}
}
