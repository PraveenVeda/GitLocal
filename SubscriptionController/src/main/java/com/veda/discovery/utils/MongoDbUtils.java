package com.veda.discovery.utils;

import java.net.UnknownHostException;
import java.util.Properties;

import com.icici.seg.utils.PropertyUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

public class MongoDbUtils {
	
	private Mongo mongoClient = null;
	private String collectionName;
	public static String configFile = "config/mongodb.properties";
	public static Properties properties;
	public static String server;
	public static Integer port;
	public static String database;
	public static String subscriptionDatabase;
	public static final String MONGO_SERVER = "MONGO_SERVER";
	public static final String MONGO_PORT = "MONGO_PORT";
	public static final String MONGO_DATABASE = "MONGO_DATABASE";
	public static final String MONGO_DATABASE_SUBSCRIPTION = "MONGO_DATABASE_SUBSCRIPTION";
	
	
	static{
		properties = PropertyUtil.loadSetting(configFile);
		server = properties.getProperty(MONGO_SERVER);
		port = Integer.parseInt(properties.getProperty(MONGO_PORT));
		database = properties.getProperty(MONGO_DATABASE);
		subscriptionDatabase = properties.getProperty(MONGO_DATABASE_SUBSCRIPTION);
	}

	public String retrieveCollectionName(String collectionName) {
		if (properties != null) {
			this.collectionName = properties.getProperty(collectionName);
		} else {
			System.out.println("File Not Found: mongodb.properties");
		}
		return this.collectionName;
	}

	public DB getMongoDbConnection() {
		DB mdb = null;
		if (properties != null) {
			try {
				mongoClient = new MongoClient(server, port);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
			mdb = mongoClient.getDB(database);
		} else {
			System.out.println("File Not Found: mongodb.properties");
		}
		return mdb;
	}
	
	public DB getSubscriptionDbConnection() {
		DB mdb = null;
		if (properties != null) {
			try {
				mongoClient = new MongoClient(server, port);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
			mdb = mongoClient.getDB(subscriptionDatabase);
		} else {
			System.out.println("File Not Found: mongodb.properties");
		}
		return mdb;
	}
	
	/**
	 * Converts any data type to String.
	 */
	public String convertAnytypeToString(BasicDBObject basicDBObject, String fieldName) {
		if(basicDBObject.get(fieldName) != null) {
			if(!(basicDBObject.get(fieldName) instanceof String)) {
				return String.valueOf(basicDBObject.get(fieldName));
			} else {
				return (basicDBObject.get(fieldName)).toString();
			}
		}
		return "";
	}
	
	public void closeMongoConnection(){
		if(mongoClient != null){
			mongoClient.close();
		}
	}
}
