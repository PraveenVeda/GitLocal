package com.veda.discovery.usageStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.datasift.examples.PoolerClient;
import org.json.JSONException;

import com.icici.seg.utils.PropertyUtil;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import com.veda.discovery.commons.ProjectsCollection;
import com.veda.discovery.commons.SubscriptionCollection;
import com.veda.discovery.utils.MongoDbUtils;

public class PollerStopper {
	private static final String MONGO_STREAM_DPU_USAGE = "MONGO_STREAM_DPU_USAGE";
	private static final String MONGO_STREAM_DPU_HISTORY = "MONGO_STREAM_DPU_HISTORY";
	private static final String MONGO_PROJECTS = "MONGO_PROJECTS";
	
	private static final String STATUS_INACTIVE = "inactive";
	private static final String NA = "NA";
	
	public static Properties configProperties;
	public static String server;
	public static Integer port;
	
	static {
		String Config_File_Path = "config/config.properties";
		configProperties = PropertyUtil.loadSetting(Config_File_Path);
		server = configProperties.getProperty("DATASIFT_POOLER_SERVER");
		port = Integer.parseInt(configProperties.getProperty("DATASIFT_POOLER_PORT"));
	}
	
	private static Logger logger = Logger.getLogger(PollerStopper.class);
	
	/**
	 * Stops the poller for all the active projects 
	 * of a client based on client-Id
	 * @param clientId :ID of the client
	 */
	public void stopPollerForClient(String clientId) throws Exception{
		List<String> snetIdList = getAllStreamingProjects(clientId);
		System.out.println("------------------------------");
		System.out.println("DataStreaming is stopping for Client : " + clientId);
		System.out.println("** Active projects for Client : " + snetIdList);
		logger.info("------------------------------");
		logger.info("DataStreaming is stopped for Client : " + clientId);
		logger.info("** Active projects for Client : " + snetIdList);
		for (String snetId : snetIdList) {
			PoolerClient poolerClient = new PoolerClient();
			if (poolerClient.isServerInitialised(server, port)) {
				try {
					poolerClient.stopPooler(snetId);
					logger.info("DataStreaming is stopped for this project : " + snetId);
					System.out.println("DataStreaming is stopped for this project : "+ snetId);
					// updates the project status to inactive
					System.out.println("Updating Project Status after Stopping pooler ");
					logger.info("Updating Project Status after Stopping pooler ");
					updateProjectStatus(clientId,snetId);
					// updates the project Stream status in stream_dpu_usage collection
					System.out.println("Updating Project Status in Stream Usage after Stopping pooler ");
					logger.info("Updating Project Status in Stream Usage after Stopping pooler ");
					updateProjectStreamStatus(clientId,snetId);
					// updates the Stream History status in stream_dpu_history collection
					System.out.println("Updating Project Status in Stream History after Stopping pooler ");
					logger.info("Updating Project Status in Stream History after Stopping pooler ");
					updateStreamHistoryStatus(clientId,snetId);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (JSONException e) {
					e.printStackTrace();
				}
				
			} 
		}
	}
	
	/**
	 * Updates the project status to suspend for Active Off-line Projects
	 * @param clientId : Id of the client
	 * @param snetId : Id of the project
	 */
	public void updateOfflineProjectStatus( String clientId) throws Exception {
		List<String> snetIdList = getActiveOfflineProjects(clientId);
		for (String snetId : snetIdList) {
				updateProjectStatus(clientId, snetId);
			} 
	}
	
	/**
	 * Updates the project status to suspended when the poller stops polling
	 * @param clientId : Id of the client
	 * @param snetId : Id of the project
	 */
	private void updateProjectStatus(String clientId, String snetId) throws Exception {
		MongoDbUtils mdbUtils = new MongoDbUtils();
		try {
			DB mDb = mdbUtils.getMongoDbConnection();
			String projects = mdbUtils.retrieveCollectionName(MONGO_PROJECTS);
			DBCollection mongoCollection = mDb.getCollection(projects);
			BasicDBObject query = new BasicDBObject();
			query.append("client_id", new ObjectId(clientId));
			query.append("snet_id", snetId);
			BasicDBObject updateQuery = new BasicDBObject();
			updateQuery.append("is_active", 0);
			mongoCollection.update(query,new BasicDBObject("$set",updateQuery));
				
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception : ", e);
		} finally {
			mdbUtils.closeMongoConnection();
		}
	}
	
	/**
	 * Gets the active and real-time projects of a client
	 * @param clientId : Id of the client
	 * @return List of active projects for a client
	 */
	private List<String> getActiveProjects(String clientId) {
		MongoDbUtils mdbUtils = new MongoDbUtils();
		List<String> snetIdList = new ArrayList<String>();
		try { 
			DB mDb = mdbUtils.getMongoDbConnection();
			String projects = mdbUtils.retrieveCollectionName(MONGO_PROJECTS);
			DBCollection mongoCollection = mDb.getCollection(projects);
			BasicDBList projectStatus = new BasicDBList();
			projectStatus.add("offline");
			BasicDBObject query = new BasicDBObject();
			query.append("client_id", new ObjectId(clientId));
			query.append("is_active", 1);
			
			query.append("realtime_or_offline_status", new BasicDBObject()
														.append("$nin",projectStatus));
			DBCursor dbCursor = mongoCollection.find(query);
			while (dbCursor.hasNext()) {
				DBObject dbObject = dbCursor.next();
				String snetId = (String) dbObject.get("snet_id");
				snetIdList.add(snetId);
				System.out.println(snetId);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception : ", e);
		} finally {
			mdbUtils.closeMongoConnection();
		}
		return snetIdList;
	}
	
	/**
	 * Gets the active and real-time projects of a client
	 * @param clientId : Id of the client
	 * @return List of active projects for a client
	 */
	private List<String> getAllStreamingProjects(String clientId) {
		MongoDbUtils mdbUtils = new MongoDbUtils();
		List<String> snetIdList = new ArrayList<String>();
		try { 
			DB mDb = mdbUtils.getMongoDbConnection();
			String projects = mdbUtils.retrieveCollectionName(MONGO_PROJECTS);
			DBCollection mongoCollection = mDb.getCollection(projects);
			BasicDBList projectStatus = new BasicDBList();
			projectStatus.add("offline");
			BasicDBObject query = new BasicDBObject();
			query.append("client_id", new ObjectId(clientId));
			//query.append("is_active", 1);
			
			query.append("realtime_or_offline_status", new BasicDBObject()
														.append("$nin",projectStatus));
			DBCursor dbCursor = mongoCollection.find(query);
			while (dbCursor.hasNext()) {
				DBObject dbObject = dbCursor.next();
				String snetId = (String) dbObject.get("snet_id");
				snetIdList.add(snetId);
				System.out.println(snetId);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception : ", e);
		} finally {
			mdbUtils.closeMongoConnection();
		}
		return snetIdList;
	}
	
	/**
	 * Gets the active and real-time projects of a client
	 * @param clientId : Id of the client
	 * @return List of active projects for a client
	 */
	private List<String> getActiveOfflineProjects(String clientId) throws Exception {
		MongoDbUtils mdbUtils = new MongoDbUtils();
		List<String> snetIdList = new ArrayList<String>();
		try { 
			DB mDb = mdbUtils.getMongoDbConnection();
			String projects = mdbUtils.retrieveCollectionName(MONGO_PROJECTS);
			DBCollection mongoCollection = mDb.getCollection(projects);
			BasicDBList projectStatus = new BasicDBList();
			projectStatus.add("offline");
			BasicDBObject query = new BasicDBObject();
			query.append("client_id", new ObjectId(clientId));
			query.append("is_active", 1);
			query.append("realtime_or_offline_status", new BasicDBObject()
														.append("$in",projectStatus));
			DBCursor dbCursor = mongoCollection.find(query);
			while (dbCursor.hasNext()) {
				DBObject dbObject = dbCursor.next();
				String snetId = (String) dbObject.get("snet_id");
				snetIdList.add(snetId);
				System.out.println(snetId);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception : ", e);
		} finally {
			mdbUtils.closeMongoConnection();
		}
		return snetIdList;
	}
	
	/**
	 * Updates the status of project to Inactive
	 * @param clientId : Id of the client
	 * @param snetId : Unique Id of the project
	 */
	public void updateProjectStreamStatus(String clientId, String snetId) {
		
		MongoDbUtils mdbUtils = new MongoDbUtils();
		DB db = null;
		try {
			db = mdbUtils.getMongoDbConnection();
			DBCollection mdbCollection = db.getCollection(mdbUtils
												.retrieveCollectionName(MONGO_STREAM_DPU_USAGE));
			BasicDBObject query = new BasicDBObject();
			query.append("client_id", new ObjectId(clientId));
			query.append("snet_id", snetId);
			String subscriptionId = new SubscriptionCollection().getActiveSubscriptionId(clientId);
			if (subscriptionId != null) {
				query.append("subscription_id", new ObjectId(subscriptionId));
				BasicDBObject upsertQuery = new BasicDBObject();
				upsertQuery.append("$set", new BasicDBObject("status", STATUS_INACTIVE));
				mdbCollection.update(query, upsertQuery);
			}
		}catch (Exception e) {
			e.printStackTrace();
		} finally {
			mdbUtils.closeMongoConnection();
		}
	}
	
	/**
	 * Deactivates the current active stream for a client
	 * @param clientId : Id of the client
	 * @param snetId : Unique Id of the project
	 */
	public void updateStreamHistoryStatus(String clientId, String snetId) {
		
		MongoDbUtils mdbUtils = new MongoDbUtils();
		DB db = null;
		try {
			db = mdbUtils.getMongoDbConnection();
			DBCollection mdbCollection = db.getCollection(mdbUtils
												.retrieveCollectionName(MONGO_STREAM_DPU_HISTORY));
			BasicDBObject query = new BasicDBObject();
			query.append("snet_id", snetId);
			String subscriptionId = new SubscriptionCollection().getActiveSubscriptionId(clientId);
			System.out.println("In Updating Stream History, ActiveSubscriptionId = " + subscriptionId);
			logger.info("In Updating Stream History, ActiveSubscriptionId = " + subscriptionId);
			if (subscriptionId != null) {
				String csdlQuery = new ProjectsCollection().retreiveCsdlQuery(snetId);
				System.out.println("subscriptionId = " + subscriptionId);
				System.out.println("csdlQuery = " + csdlQuery);
				System.out.println("snetId = " + snetId);
				query.append("subscription_id", new ObjectId(subscriptionId));
				query.append("csdl_query", csdlQuery);
				query.append("stream_usage.end_date", NA);
				System.out.println("QUERY TO UPDATE HISTORY = " + query);
				logger.info("QUERY TO UPDATE HISTORY = " + query);
				BasicDBObject setobject = new BasicDBObject();
				setobject.append("$set", new BasicDBObject("stream_usage.$.end_date", new Date())
												   .append("status", STATUS_INACTIVE));
				WriteResult res = mdbCollection.update(query, setobject);
				System.out.println("WriteResult Status = " + res);
				logger.info("WriteResult Status = " + res);
			}
		}catch (Exception e) {
			e.printStackTrace();
		} finally {
			mdbUtils.closeMongoConnection();
		}
	}
	
	public static void main(String[] args) {
		new PollerStopper().updateStreamHistoryStatus("5301a920e32ad5bb170732d9", "Praveen341");
	}

}
