package com.veda.discovery.commons;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.veda.discovery.utils.MongoDbUtils;

public class SubscriptionCollection {

	private static final String MONGO_SUBSCRIPTIONS = "MONGO_SUBSCRIPTIONS";
	private static Logger logger = Logger.getLogger(SubscriptionCollection.class);
	
	
	/**
	 * gets the subscription-ID for a plan
	 * @param clientId : Id of the client
	 * @return
	 */
	public String getActiveSubscriptionId (String clientId) {
		if (clientId == null) {
			return null;
		}
		MongoDbUtils mdbUtils = new MongoDbUtils();
		try {
			DB mDb = mdbUtils.getSubscriptionDbConnection();
			String plans = mdbUtils.retrieveCollectionName(MONGO_SUBSCRIPTIONS);
			DBCollection mongoCollection = mDb.getCollection(plans);
			BasicDBObject query= new BasicDBObject();
			query.append("client_id", new ObjectId(clientId));
			query.append("is_active", 1);
			DBObject dBObject = mongoCollection.findOne(query);
			if (dBObject != null) {
				String subId = dBObject.get("_id").toString();
				return subId;
			}
		} catch (NullPointerException e) {
			System.out.println("Exception : The user may not be subscribed");
			logger.error("Exception : The user may not be subscribed", e);
		}  catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception : ", e);
		} finally {
			mdbUtils.closeMongoConnection();
		}
		return null;
	}
}
