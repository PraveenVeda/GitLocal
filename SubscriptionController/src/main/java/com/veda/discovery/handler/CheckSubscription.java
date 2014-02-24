package com.veda.discovery.handler;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.veda.discovery.usageStats.PollerStopper;
import com.veda.discovery.utils.MongoDbUtils;


public class CheckSubscription {
	
	private static final String MONGO_USAGE_STATISTICS = "MONGO_USAGE_STATISTICS";
	private static final String MONGO_SUBSCRIPTIONS = "MONGO_SUBSCRIPTIONS";
	private static final String MONGO_CLIENTS = "MONGO_CLIENTS";
	private static final String MONGO_PROJECTS = "MONGO_PROJECTS";
	
	private static Logger logger = Logger.getLogger(CheckSubscription.class);
	
	/**
	 * This method checks all the conditions to stop polling for client i.e 
	 * credits, sentences and end-date are expired or exhausted
	 */
	public void enquireSubscription() throws Exception{
		List<String> clientIdList = getAllClients();
		if (clientIdList.size() > 0) {
			for (String clientId : clientIdList) {
				if (clientId != null) {
					if (checkExpiryDate(clientId)) {
						// stopping poller
						new PollerStopper().stopPollerForClient(clientId);
					} else if (checkSentences(clientId)) {
						// stopping Poller
						new PollerStopper().stopPollerForClient(clientId);
					} else if (checkCredits(clientId)) {
						// stopping poller
						new PollerStopper().stopPollerForClient(clientId);
					}
				}
			}
		}
	}
	
	/**
	 * This method checks all the conditions i.e credits, sentences and end-date 
	 * are expired or exhausted to stop polling for client and deactivates the active
	 * subscription and active projects of the client
	 * @param clientId : Id of the client
	 * @return
	 */
	public boolean enquireSubscriptionForClient(String clientId) throws Exception{
		if (clientId != null) {
			if (checkExpiryDate(clientId)) {
				System.out.println("Expiry-Date : Check");
				// stopping poller
				new PollerStopper().stopPollerForClient(clientId);
				//sets the status of subscription to inactive
				makeSubscriptionInactive(clientId);
				return true;
			} else if (checkSentences(clientId)) {
				System.out.println("Sentences : Check");
				// stopping Poller
				new PollerStopper().stopPollerForClient(clientId);
				//sets the status of subscription to inactive
				makeSubscriptionInactive(clientId);
				return true;
			} else if (checkCredits(clientId)) {
				System.out.println("Credits : Check");
				// stopping poller
				new PollerStopper().stopPollerForClient(clientId);
				//sets the status of subscription to inactive
				makeSubscriptionInactive(clientId);
				return true;
			}
		}
		return false;
	}
	
	/**
	 * This method checks all the conditions i.e credits, sentences and end-date 
	 * are expired or exhausted to stop polling for client and deactivates the active
	 * subscription and active projects of the client
	 * @param clientId : Id of the client
	 * @return
	 */
	public boolean enquireSubscriptionForClientOffline(String clientId) throws Exception{
		if (clientId != null && (checkExpiryDate(clientId) ||
				checkSentences(clientId) || checkCredits(clientId))) {
			new PollerStopper().updateOfflineProjectStatus(clientId);
			//sets the status of subscription to inactive
			makeSubscriptionInactive(clientId);
			return true;
		}
		return false;
	}
	
	/**
	 * Checks whether the sentences given to a client are exhausted or not
	 * @param clientId : Id of the client
	 * @return true : if the current sentences count exceeds the total sentences count
	 */
	public boolean checkSentences(String clientId) throws Exception{
		/**
		 * Query Used : 
		 * db.usage_statistics.aggregate(
		 * {"$match": {"client_id" : ObjectId("52a728abc07f8ad68acedccc")}
           },
           {"$unwind" : "$statistics"},
           {"$group" : {"_id" : "$subscription_id",
                        "count" : { "$sum" : "$statistics.sentences_count"}
           }
            })
		 */
		
		MongoDbUtils mdbUtils = new MongoDbUtils();
		try { 
			DB mDb = mdbUtils.getSubscriptionDbConnection();
			String stats = mdbUtils.retrieveCollectionName(MONGO_USAGE_STATISTICS);
			DBCollection mongoCollection = mDb.getCollection(stats);
			BasicDBObject matchQuery = new BasicDBObject();
			//Matches the client-ID in the query
			matchQuery.append("$match", new BasicDBObject()
											.append("client_id", new ObjectId(clientId)));
			
			//Unwinds the array
			BasicDBObject unwindQuery = new BasicDBObject("$unwind","$statistics");
			BasicDBObject groupQuery = new BasicDBObject();
			//Groups the elements based on subscription-ID 
			groupQuery.append("$group", new BasicDBObject().append("_id", "$subscription_id")
											.append("count", new BasicDBObject()
											.append("$sum", "$statistics.sentences_count")));
			
			AggregationOutput result = mongoCollection.aggregate(matchQuery,
																 unwindQuery, 
																 groupQuery);
			int sentencesCount = 0;
			String activeSubId = null;
			for (DBObject dbObject : result.results()) { 
				 if (checkActiveSubscription(dbObject.get("_id").toString())) {
					 activeSubId = dbObject.get("_id").toString();
					//gets the used no.of sentences till date
					sentencesCount = (Integer) dbObject.get("count");
					break;
				 }
			}
			
			if (activeSubId != null) {
				//gets the total sentences of client for subscription
				int totSentences = getTotalSentencesforSubscription(activeSubId);
				//checks if processed sentences count exceeds the total sentences assigned
				// for client to stop the poller
				if (sentencesCount >= totSentences) {
					System.out.println("Stopping Poller...as Sentences Exhausted at :"
										+ new Date() + " for clientId : " + clientId);
					logger.error("Stopping Poller...as Sentences Exhausted at :"
										+ new Date() + " for clientId : " + clientId);
					return true;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception : ", e);
		} finally {
			mdbUtils.closeMongoConnection();
		}
		return false;
	}
	
	/**
	 * Checks whether the credits given to a client are exhausted or not
	 * @param clientId : Id of the client
	 * @return true : if the current credits count exceeds the total credits 
	 */
	public boolean checkCredits(String clientId) throws Exception{
		/**
		 * Query Used : 
		 * db.usage_statistics.aggregate(
		 * {"$match": {"client_id" : ObjectId("52a728abc07f8ad68acedccc")}
           },
           {"$unwind" : "$statistics"},
           {"$group" : {"_id" : "$subscription_id",
                        "count" : { "$sum" : "$statistics.credits"}
           }
            })
		 * 
		 */
		
		MongoDbUtils mdbUtils = new MongoDbUtils();
		try { 
			DB mDb = mdbUtils.getSubscriptionDbConnection();
			String stats = mdbUtils.retrieveCollectionName(MONGO_USAGE_STATISTICS);
			DBCollection mongoCollection = mDb.getCollection(stats);
			BasicDBObject matchQuery = new BasicDBObject();
			//Matches the client-ID in the query
			matchQuery.append("$match", new BasicDBObject()
											.append("client_id", new ObjectId(clientId)));
			//Unwinds the array
			BasicDBObject unwindQuery = new BasicDBObject("$unwind","$statistics");
			BasicDBObject groupQuery = new BasicDBObject();
			//Groups the elements based on subscription-ID 
			groupQuery.append("$group", new BasicDBObject().append("_id", "$subscription_id")
											.append("count", new BasicDBObject()
											.append("$sum", "$statistics.dpu")));
			
			AggregationOutput result = mongoCollection.aggregate(matchQuery,
																 unwindQuery, 
																 groupQuery);
			double currentCreditCount = 0;
			String activeSubId = null;
			for (DBObject dbObject : result.results()) {
				if (checkActiveSubscription(dbObject.get("_id").toString())) {
					 activeSubId = dbObject.get("_id").toString();
					// gets the used no.of credits till date 
					// rounding off the currentCreditCount upto two decimals
					currentCreditCount = Math.round((Double) dbObject.get("count")*100.0)/100.0;
					break;
				 }
				
			}
			if (activeSubId != null) {
				// gets the total credits for subscription
				double totCreditCount = getTotalCreditsforSubscription(activeSubId);
				//checks if used credit count exceeds the total credits assigned
				// for client to stop the poller
				if (currentCreditCount >= totCreditCount) {
					System.out.println("Stopping Poller...as Credits Exhausted at : "
									+ new Date() + " for clientId : " + clientId);
					logger.error("Stopping Poller...as Credits Exhausted at : "
									+ new Date() + " for clientId : " + clientId);
					return true;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception : ", e);
		} finally {
			mdbUtils.closeMongoConnection();
		}
		return false;
	}
	
	/**
	 * Checks whether the subscription endDate is expired 
	 * @param clientId : Id of the client
	 * @return true : if the current date crosses the subscription end date
	 */
	public boolean checkExpiryDate(String clientId) throws Exception{
		MongoDbUtils mdbUtils = new MongoDbUtils();
		try {
			DB mDb = mdbUtils.getSubscriptionDbConnection();
			String subs = mdbUtils.retrieveCollectionName(MONGO_SUBSCRIPTIONS);
			DBCollection mongoCollection = mDb.getCollection(subs);
			BasicDBObject query = new BasicDBObject();
			query.append("client_id", new ObjectId(clientId));
			query.append("is_active", 1);
			DBObject dbObject = mongoCollection.findOne(query);
			if (dbObject != null) {
				Date endDate = (Date) dbObject.get("end_date");
				Date date = new Date();
				//checks if the current date is after the end date 
				// to stop the poller
				if (date.after(endDate)) {
					System.out.println("Stopping Poller...as Date Expired at : "
										+ new Date() + " for clientId : " + clientId);
					logger.error("Stopping Poller...as Date Expired at : "
										+ new Date() + " for clientId : " + clientId);
					return true;
				}
			} else {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception : ", e);
		} finally {
			mdbUtils.closeMongoConnection();
		}
		return false;
	}
	
	/**
	 * Gets the total no.of sentences for a subscription
	 * @param subscriptionId :Id of subscription
	 * @return total no.of sentences for a subscription
	 */
	private int getTotalSentencesforSubscription(String subscriptionId) {
		if (subscriptionId == null) {
			return 0;
		}
		MongoDbUtils mdbUtils = new MongoDbUtils();
		int totalSentences = 0;
		try { 
			DB mDb = mdbUtils.getSubscriptionDbConnection();
			String subscription = mdbUtils.retrieveCollectionName(MONGO_SUBSCRIPTIONS);
			DBCollection mongoCollection = mDb.getCollection(subscription);
			BasicDBObject query = new BasicDBObject();
			query.append("_id", new ObjectId(subscriptionId));
			DBObject dbObject = mongoCollection.findOne(query);
			if (dbObject != null) {
				totalSentences = (Integer) dbObject.get("total_sentences");
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception : ", e);
		} finally {
			mdbUtils.closeMongoConnection();
		}
		return totalSentences;
	}
	
	/**
	 * Gets the total no.of credits for a subscription
	 * @param subscriptionId :Id of subscription
	 * @return total no.of credits for a subscription
	 */
	private double getTotalCreditsforSubscription(String subscriptionId) {
		if (subscriptionId == null) {
			return 0.0;
		}
		MongoDbUtils mdbUtils = new MongoDbUtils();
		double credits = 0;
		try { 
			DB mDb = mdbUtils.getSubscriptionDbConnection();
			String subscription = mdbUtils.retrieveCollectionName(MONGO_SUBSCRIPTIONS);
			DBCollection mongoCollection = mDb.getCollection(subscription);
			BasicDBObject query = new BasicDBObject();
			query.append("_id", new ObjectId(subscriptionId));
			DBObject dbObject = mongoCollection.findOne(query);
			if (dbObject != null) {
				credits = (Double) dbObject.get("total_credits");
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception : ", e);
		} finally {
			mdbUtils.closeMongoConnection();
		}
		return credits;
	}
	
	
	/**
	 * Returns the list of client Ids from the database
	 * @return
	 */
	private List<String> getAllClients() {
		MongoDbUtils mdbUtils = new MongoDbUtils();
		List<String> clientIdList = new ArrayList<String>();
		try { 
			DB mDb = mdbUtils.getMongoDbConnection();
			String clients = mdbUtils.retrieveCollectionName(MONGO_CLIENTS);
			DBCollection mongoCollection = mDb.getCollection(clients);
			DBCursor dbCursor = mongoCollection.find();
			while (dbCursor.hasNext()) {
				DBObject dbObject = dbCursor.next();
				String clientId = dbObject.get("_id").toString();
				clientIdList.add(clientId);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception : ", e);
		} finally {
			mdbUtils.closeMongoConnection();
		}
		return clientIdList;
	}
	
	/**
	 * Checks if the subscription is active or not
	 * @param subId : Id of the subscription
	 * @return true if the subscription is Active
	 */
	private boolean checkActiveSubscription(String subId) {
		MongoDbUtils mdbUtils = new MongoDbUtils();
		try {
			DB mDb = mdbUtils.getSubscriptionDbConnection();
			String subscription = mdbUtils.retrieveCollectionName(MONGO_SUBSCRIPTIONS);
			DBCollection mongoCollection = mDb.getCollection(subscription);
			BasicDBObject query = new BasicDBObject();
			query.append("_id", new ObjectId(subId));
			DBObject dbObject = mongoCollection.findOne(query);
			int subStatus = (Integer) dbObject.get("is_active");
			if (subStatus == 0) {
				return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception : ", e);
		} finally {
			mdbUtils.closeMongoConnection();
		}
		return true;
	}
	
	/**
	 * Deactivates the active subscription 
	 * @param clientId : Id of the client
	 */
	private void makeSubscriptionInactive(String clientId) throws Exception {
		MongoDbUtils mdbUtils = new MongoDbUtils();
		try {
			DB mDb = mdbUtils.getSubscriptionDbConnection();
			String subscription = mdbUtils.retrieveCollectionName(MONGO_SUBSCRIPTIONS);
			DBCollection mongoCollection = mDb.getCollection(subscription);
			BasicDBObject query = new BasicDBObject();
			query.append("client_id", new ObjectId(clientId));
			query.append("is_active", 1);
			BasicDBObject updateQuery = new BasicDBObject("$set",
												new BasicDBObject("is_active", 0));
			mongoCollection.update(query, updateQuery);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception : ", e);
		} finally {
			mdbUtils.closeMongoConnection();
		}
	}
	
	public static void main(String[] args) {
		//new CheckSubscription().checkSentences("52c7aa334c31b4e1ce6f93d9");
		//new CheckSubscription().checkCredits("52c7aa334c31b4e1ce6f93d9");
		try {
			new CheckSubscription().enquireSubscriptionForClient("52df67e330d24de9fbce81eb");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//new CheckSubscription().makeSubscriptionInactive("52c7aa334c31b4e1ce6f93d9");
		//new CheckSubscription().getActiveProjects("52fc6258e4b049743727f1ed");
	}
}
