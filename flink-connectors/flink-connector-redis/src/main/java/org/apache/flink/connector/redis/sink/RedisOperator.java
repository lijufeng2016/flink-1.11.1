package org.apache.flink.connector.redis.sink;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;

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

	public void hdelCdc(RowData rowData) {
		String key = rowData.getString(0).toString();
		key = keyPrefix + key + suffix;
		MapData map = rowData.getMap(1);
		ArrayData keyArray = map.keyArray();
		for (int i = 0; i < map.size(); i++) {
			String filed = keyArray.getString(i).toString();
			jedisCluster.hdel(key, filed);
		}
	}

	public void cdc(RowData rowData, Integer expire) {
		GenericRowData genericRowData = (GenericRowData) rowData;
		RowKind rowKind = genericRowData.getRowKind();
		switch (rowKind) {
			case DELETE:
				// 区分是key的delete还是field的delete
				if (genericRowData.getField(1) instanceof MapData) {
					hdelCdc(rowData);
				} else {
					del(rowData);
				}
				break;
			case INSERT:
				// insert支持redis的list，hash，string类型
				Object valueInsert = genericRowData.getField(1);
				if (valueInsert instanceof ArrayData) {
					lpush(genericRowData, expire);
				} else if (valueInsert instanceof StringData) {
					set(genericRowData, expire);
				} else if (valueInsert instanceof MapData) {
					hset(rowData, expire);
				}
				break;
			case UPDATE_AFTER:
				// update支持redis的list，hash，string类型
				Object valueUpdateAfter = genericRowData.getField(1);
				if (valueUpdateAfter instanceof StringData) {
					set(genericRowData, expire);
				} else if (valueUpdateAfter instanceof MapData) {
					hset(rowData, expire);
				} else if (valueUpdateAfter instanceof ArrayData) {
					del(rowData);
					lpush(rowData, expire);
				}
				break;
		}
	}
}
