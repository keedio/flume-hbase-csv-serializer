package org.keedio.flume.sink.hbase.utils;

import java.io.UnsupportedEncodingException;
import java.util.Random;
import java.util.UUID;

public class RowKeyGenerator {
	
	public static byte[] generateRowKey(KeyType keyType) throws UnsupportedEncodingException{
		
		if (keyType == KeyType.TS) {
			return (String.valueOf(System.currentTimeMillis())).getBytes("UTF8");
		} else if (keyType == KeyType.RANDOM) {
			return (String.valueOf(new Random().nextLong())).getBytes("UTF8");
		} else if (keyType == KeyType.TSNANO) {
			return (String.valueOf(System.nanoTime())).getBytes("UTF8");
		} else {
			return (UUID.randomUUID().toString()).getBytes("UTF8");
		}
	}
	
	/* TODO: Make the rowKey configurable by one field content
	public static generateKeyFromField(String fieldName){
		
	}
	*/
}
