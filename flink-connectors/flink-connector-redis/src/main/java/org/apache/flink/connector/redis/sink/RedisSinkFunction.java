package org.apache.flink.connector.redis.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.redis.util.JedisUtil;
import org.apache.flink.connector.redis.util.WriteTypeEnum;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

import redis.clients.jedis.JedisCluster;

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

	public RedisSinkFunction(String redisHosts, String auth, WriteTypeEnum writeTypeEnum, Integer expire) {
		this.redisHost = redisHosts;
		this.auth = auth;
		this.writeTypeEnum = writeTypeEnum;
		this.expire = expire;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		jedisCluster = JedisUtil.getJedisCluster(redisHost, auth);
		operator = new RedisOperator(jedisCluster);
		super.open(parameters);
	}

	@Override
	public void invoke(RowData rowData, Context context) throws Exception {
		switch (writeTypeEnum) {
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

	}

	@Override
	public void close() throws Exception {
		jedisCluster.close();
	}
}
