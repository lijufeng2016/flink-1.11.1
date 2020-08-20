package org.apache.flink.connector.redis.util;

/**
 * redis写入类型的枚举类.
 */
public enum WriteTypeEnum {

	SET("set", "string类型普通set值"),
	LPUSH("lpush", "list类型左边push"),
	RPUSH("rpush", "list类型右边push"),
	HSET("hset", "hash类型添加field值"),
	DEL("del", "删除redis key"),
	HDEL("hdel", "删除hash类型的filed");

	String label;
	String massage;

	WriteTypeEnum(String label, String massage) {
		this.label = label;
		this.massage = massage;
	}

	public String getLabel() {
		return label;
	}

	public String getMassage() {
		return massage;
	}
}
