package com.veda.discovery.usageStats;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.veda.discovery.commons.SubscriptionCollection;
import com.veda.discovery.handler.CheckSubscription;
import com.veda.discovery.utils.MongoDbUtils;


public class UsageStats { 
	
	private static final String MONGO_USAGE_STATISTICS = "MONGO_USAGE_STATISTICS";
	private static final String MONGO_SUBSCRIPTIONS = "MONGO_SUBSCRIPTIONS";
	private static final String MONGO_PROJECTS = "MONGO_PROJECTS";
	private static Logger logger = Logger.getLogger(UsageStats.class);
	
	public void analyzeSubscription(String clientId, String snetId, Integer sentencesCount, 
									Integer messagesCount, Double dpuCount) throws Exception {
		
		//Checks if the subscription expired and stops the poller for
		//the client
		boolean isSubscriptionActive = new CheckSubscription().enquireSubscriptionForClient(clientId);
		if (!isSubscriptionActive) {
			// upserts the details of sentence, message and 
			//dpu counts into Usage-Statistics collection
			new UsageStats().upsertStats(clientId, snetId, sentencesCount,
				 						 messagesCount, dpuCount);
		}
	}
	
	/**
	 * Inserts the record to the usage statistics collection for the client if
	 * the record is not present and Updates if the record is present 
	 * 
	 * @param clientId : Id of the client
	 * @param snetId : unique Id of the project
	 * @param projectName : name of the project created by client
	 * @param sentencesCount : count of sentences processed for a project
	 * @param messagesCount : count of messages processed for a project
	 * @param dpuCount : count of credits processed for a project
	 */
	public void upsertStats(String clientId, String snetId, Integer sentencesCount, 
							Integer messagesCount, Double dpuCount) {
		MongoDbUtils mdbUtils = new MongoDbUtils();
		try {
			DB mDb = mdbUtils.getSubscriptionDbConnection();
			String stats = mdbUtils.retrieveCollectionName(MONGO_USAGE_STATISTICS);
			DBCollection mongoCollection = mDb.getCollection(stats);
			BasicDBObject query = new BasicDBObject();
			query.append("client_id", new ObjectId(clientId));
			String activePlanId = getActivePlanRefId(clientId);
			String subId = new SubscriptionCollection().getActiveSubscriptionId(clientId);
			if (subId != null) {
				query.append("subscription_id", new ObjectId(subId));
				query.append("plan_ref_id", activePlanId);
				query.append("statistics.snet_id", snetId);
				// checks if the snetId is present in the statistics list
				DBObject dbObject = mongoCollection.findOne(query);
				if (dbObject == null) {
					// If the snetId is not present, checks if the client record exists
					// in the database
					query.remove("statistics.snet_id");
					BasicDBObject statsDetails = new BasicDBObject();
					statsDetails.append("project_name", getProjectLabel(snetId));
					statsDetails.append("snet_id", snetId);
					//if messagesCount is null, adds messagesCount as 0
					// else adds the incoming messages count 
					if (messagesCount == null) {
						statsDetails.append("messages_count", 0);
					} else {
						statsDetails.append("messages_count", messagesCount);
					}
					//if sentencesCount is null, adds sentencesCount as 0
					// else adds the incoming sentences count 
					if (sentencesCount == null) {
						statsDetails.append("sentences_count", 0);
					} else {
						statsDetails.append("sentences_count", sentencesCount);
					}
					//if dpuCount is null, adds dpuCount as 0
					// else adds the incoming dpu count 
					if (dpuCount == null) {
						statsDetails.append("dpu", 0.0);
					} else {
						statsDetails.append("dpu", dpuCount);
					}
					BasicDBObject push = new BasicDBObject();
					push.append("$push", new BasicDBObject("statistics",statsDetails));
					// Creates and pushes the details into the statistics list
					mongoCollection.update(query, push, true, false);
				} else {
					// If the snetId is present, updates the counts for that snetId
					BasicDBObject updateQuery = new BasicDBObject();
					BasicDBObject incrementObj =  new BasicDBObject();
					//increments the sentences count if not null
					if (sentencesCount != null) {
						updateQuery.append("$inc", incrementObj.append(
								"statistics.$.sentences_count", sentencesCount));
					}
					//increments the messages count if not null
					if (messagesCount != null) {
						updateQuery.append("$inc", incrementObj.append(
								"statistics.$.messages_count", messagesCount));
					}
					//sets the dpu count if not null
					if (dpuCount != null) {
						updateQuery.append("$set", new BasicDBObject().append(
								"statistics.$.dpu", dpuCount));
					}
					mongoCollection.update(query, updateQuery);
				}
			}
		} catch (IllegalArgumentException e) {
			System.out.println("IllegalArgumentException : Parsing null objectId in UpsertStats of class UsageStats");
			logger.error("IllegalArgumentException : " + e);
			e.printStackTrace();
		} catch (NullPointerException e) {
			System.out.println("NullPointerException : ObjectId may be null");
			logger.error("NullPointerException : " + e);
			e.printStackTrace();
		} catch(Exception e){
			logger.error("Exception : " + e);
			e.printStackTrace();
		} finally {
			mdbUtils.closeMongoConnection();
		}
	}
	
	
	/**
	 * gets the active plan of a client
	 * @param clientId : Id of a client
	 * @return active Plan reference Id
	 */
	private String getActivePlanRefId (String clientId) {
		if (clientId == null) {
			return null;
		}
		MongoDbUtils mdbUtils = new MongoDbUtils();
		try {
			DB mDb = mdbUtils.getSubscriptionDbConnection();
			String subscription = mdbUtils.retrieveCollectionName(MONGO_SUBSCRIPTIONS);
			DBCollection mongoCollection = mDb.getCollection(subscription);
			BasicDBObject query = new BasicDBObject();
			query.append("client_id", new ObjectId(clientId));
			query.append("is_active", 1);
			DBObject dBObject = mongoCollection.findOne(query);
			if (dBObject != null) {
				String planRefId = dBObject.get("plan_ref_id").toString();
				return planRefId;
			}
		} catch (NullPointerException e) {
			System.out.println("Exception : The user has no active plans ");
			logger.error("Exception : The user has no active plans ", e);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception : ", e);
		} finally {
			mdbUtils.closeMongoConnection();
		}
		return null;
	}
	
	/**
	 * <p> Get label of a project </p>
	 * @param snetId : unique Id of the project
	 * @return string label : Name of the project
	 * @throws Exception
	 */
	private String getProjectLabel(String snetId) {
		String label = "";
		MongoDbUtils mdbUtils = new MongoDbUtils();
		try {
			DB mDb = mdbUtils.getMongoDbConnection();
			String projects = mdbUtils.retrieveCollectionName(MONGO_PROJECTS);
			DBCollection projectsCollection = mDb.getCollection(projects);
			BasicDBObject query = new BasicDBObject("snet_id", snetId);
			BasicDBObject project = new BasicDBObject("label", 1);
			DBObject dbObject = projectsCollection.findOne(query, project);
			if (dbObject != null) {
				label = (String) dbObject.get("label");
			}
		} catch(Exception e) {
			logger.error("Exception in getProjectLabel snetId : " + snetId, e);
			e.printStackTrace();
		} finally {
			mdbUtils.closeMongoConnection();	
		}
		return label;
	}
	
	public static void main(String[] args) {
		//new UsageStats().getActivePlanId("52c7aa334c31b4e1ce6f93d9");
		//new UsageStats().upsertStats("52fc6258e4b049743727f1ed", "MOO",null, null, 0.2);
		//new UsageStats().getProjectLabel("Politics2014");
		//new UsageStats().getActivePlanId("52fc6258e4b049743727f1ed");
		
	}
}
