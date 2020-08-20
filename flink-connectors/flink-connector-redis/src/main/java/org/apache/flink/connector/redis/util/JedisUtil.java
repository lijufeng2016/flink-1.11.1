package org.apache.flink.connector.redis.util;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.Set;

/**
 * jedis工具类.
 */
public class JedisUtil {
	private JedisUtil() {
	}

	/**
	 * 单例 jedisCluster.
	 */
	private static JedisCluster jedisCluster;

	/**
	 * 獲取JedisCluster.
	 *
	 * @return
	 */
	public static JedisCluster getJedisCluster(String hosts, String auth) {

		if (null != jedisCluster) {
			return jedisCluster;
		}
		String[] split = hosts.split(",");
		Set<HostAndPort> hostAndPortsSet = new HashSet<>();

		for (int i = 0; i < split.length; i++) {
			String node = split[i];
			String[] hostAndPort = node.split(":");
			String host = hostAndPort[0];
			int port = Integer.parseInt(hostAndPort[1]);
			hostAndPortsSet.add(new HostAndPort(host, port));
		}
		// Jedis连接池配置
		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
		// 最大空闲连接数, 默认8个
		jedisPoolConfig.setMaxIdle(100);
		// 最大连接数, 默认8个
		jedisPoolConfig.setMaxTotal(100);
		//最小空闲连接数, 默认0
		jedisPoolConfig.setMinIdle(0);
		// 获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间,  默认-1  设置2秒
		jedisPoolConfig.setMaxWaitMillis(10000);
		// 对拿到的connection进行validateObject校验
		jedisPoolConfig.setTestOnBorrow(true);
		jedisCluster = new JedisCluster(hostAndPortsSet, 10000, 10000, 3, auth, jedisPoolConfig);
		return jedisCluster;
	}

}
