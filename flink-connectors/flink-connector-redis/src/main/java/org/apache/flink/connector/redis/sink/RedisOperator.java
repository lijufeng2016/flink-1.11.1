package org.apache.flink.connector.redis.sink;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;

import redis.clients.jedis.JedisCluster;

/**
 * RedisOperator.
 */
public class RedisOperator {
	private JedisCluster jedisCluster;
	private String keyPrefix;
	private String suffix;

	public RedisOperator(JedisCluster jedisCluster, String keyPrefix, String suffix) {
		this.jedisCluster = jedisCluster;
		this.keyPrefix = keyPrefix;
		this.suffix = suffix;
	}

	public void set(RowData rowData, Integer expire) {
		String key = rowData.getString(0).toString();
		key = keyPrefix + key + suffix;
		String val = rowData.getString(1).toString();
		jedisCluster.setex(key, expire, val);
	}

	public void lpush(RowData rowData, Integer expire) {
		String key = rowData.getString(0).toString();
		key = keyPrefix + key + suffix;
		ArrayData array = rowData.getArray(1);
		String[] values = new String[array.size()];
		for (int i = 0; i < array.size(); i++) {
			values[i] = array.getString(i).toString();
		}
		jedisCluster.lpush(key, values);
		jedisCluster.expire(key, expire);
	}

	public void rpush(RowData rowData, Integer expire) {
		String key = rowData.getString(0).toString();
		key = keyPrefix + key + suffix;
		ArrayData array = rowData.getArray(1);
		String[] values = new String[array.size()];
		for (int i = 0; i < array.size(); i++) {
			values[i] = array.getString(i).toString();
		}
		jedisCluster.rpush(key, values);
		jedisCluster.expire(key, expire);

	}

	public void hset(RowData rowData, Integer expire) {
		String key = rowData.getString(0).toString();
		key = keyPrefix + key + suffix;
		MapData map = rowData.getMap(1);
		ArrayData keyArray = map.keyArray();
		ArrayData valueArray = map.valueArray();
		for (int i = 0; i < map.size(); i++) {
			String filed = keyArray.getString(i).toString();
			String value = valueArray.getString(i).toString();
			jedisCluster.hset(key, filed, value);
		}
		jedisCluster.expire(key, expire);
	}

	public void del(RowData rowData) {
		String key = rowData.getString(0).toString();
		key = keyPrefix + key + suffix;
		jedisCluster.del(key);
	}

	public void hdel(RowData rowData) {
		String key = rowData.getString(0).toString();
		key = keyPrefix + key + suffix;
		String fileds = rowData.getString(1).toString();
		String[] split = fileds.split(",");
		jedisCluster.hdel(key, split);
	}
}
