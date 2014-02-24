package com.veda.discovery.commons;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.veda.discovery.utils.MongoDbUtils;

public class ProjectsCollection {
	
	private static final String MONGO_PROJECTS = "MONGO_PROJECTS";
	
	
	/**
	 * Retrieve the client id for specified project. 
	 */
	public String retreiveCsdlQuery(String snetId) {
		MongoDbUtils mongoDb = new MongoDbUtils();
		String csdlQuery = null;
		DB db = null;
		try {
			if (snetId != null) {
				db = mongoDb.getMongoDbConnection();
				DBCollection collection = db.getCollection(mongoDb.retrieveCollectionName(MONGO_PROJECTS));
				BasicDBObject query = new BasicDBObject();
				query.append("snet_id", snetId);
				BasicDBObject project = new BasicDBObject("csdl_query" , 1);
				DBCursor cursor = collection.find(query, project);
				if (cursor.hasNext()) {
					csdlQuery = (String) cursor.next().get("csdl_query");
				}
			} 
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (mongoDb != null) {
				mongoDb.closeMongoConnection();
			}
			
		}
		return csdlQuery;
	}
}
