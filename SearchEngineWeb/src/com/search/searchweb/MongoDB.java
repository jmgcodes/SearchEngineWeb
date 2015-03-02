package com.search.searchweb;


import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.ServerAddress;

import java.net.UnknownHostException;
import java.util.*;
import java.util.Map.Entry;
import java.math.*;


public class MongoDB{
	
	
	private class RankObject{
		
		int qrWC;
		double tfidf_score;
		Set<Integer> posSet;
		double cos_score;
		double mag_doc;
		
		double page_rank;

		RankObject(){
			this.qrWC = 0;
			this.tfidf_score = 0.0;
			this.posSet = new TreeSet<Integer>();
			this.cos_score = 0.0;
			this.mag_doc = 0.0;
			
			this.page_rank = 0.0;

		}
		
	}
	
	
	static MongoClient mongoClient;
	static DB db;
	static DBCollection coll;
	static DBCollection coll1;
	
	public void fnInit() throws UnknownHostException{
		mongoClient = new MongoClient( "localhost" , 27017 );
		db = mongoClient.getDB("InvertedIndex");
		coll = db.getCollection("InvIndex");
	    coll1 = db.getCollection("DocMap");
	}
   
   public void fnFind(String search) throws UnknownHostException{
	   
	    
        BasicDBObject query = new BasicDBObject();
        query.put("word", search);
        
        DBCursor cursor = coll.find(query);
        if (cursor.hasNext()) { 
      	  
            DBObject obj = (DBObject) cursor.next();
                      
            List<BasicDBObject> temp = (List<BasicDBObject>) obj.get("doc");

            for(BasicDBObject tempObj: temp){
            	
                BasicDBObject queryUrl = new BasicDBObject();
                queryUrl.put("docID", tempObj.get("id"));
                DBCursor cursorUrl = coll1.find(queryUrl);
                
                if (cursorUrl.hasNext()) { 
              	  
                    DBObject objUrl = (DBObject) cursorUrl.next();
                              
                	System.out.println("[" +tempObj.get("frequency")+"] " + objUrl.get("url"));
                }
                
            	
            }
     	 
        }
        else{
        	System.out.println("Sorry! Word not found");

        }
        
   }
   

   public List<String> fnSearch(String search) throws UnknownHostException{
	   	   
       int N = 85000;

	   String[] searchArr = search.split(" ");
	   int querySize = searchArr.length;
	   double mag_query = 0.0;
	   
	   Map<String, Double> queryMap = new HashMap<String, Double>();
	   for(String word: searchArr){
		   
		   double queryWC = 0.0;
		   if(queryMap.containsKey(word)){
			   
			   queryWC = queryMap.get(word);
		   }
		   queryWC += 1.0;
		   queryMap.put(word, queryWC);
		   
	   }

	   int queryWordPros = 0;
	   Map<String,RankObject> AndOrTempMap = new HashMap<String, RankObject>();
	   Map<String,RankObject> AndOrMap = new HashMap<String, RankObject>();
	   Map<String,RankObject> AndOrTitleMap = new HashMap<String, RankObject>();
	   Map<String,RankObject> AndOrTitleFinalMap = new HashMap<String, RankObject>();
	   List<String> result = new ArrayList<String>();

	   for(String word: searchArr){
		   
		   queryWordPros++;
		   BasicDBObject query = new BasicDBObject();
	       query.put("word", word);
	       DBCursor cursor = coll.find(query);
	       
	       if (cursor.hasNext()) {   
	    	   DBObject obj = (DBObject) cursor.next();	                     
	           List<BasicDBObject> temp = (List<BasicDBObject>) obj.get("doc");
	           
	           double queryTF = queryMap.get(word);
	           queryTF = (1+Math.log10(queryTF))*Math.log10((double)N/temp.size());
	           queryMap.put(word, queryTF);
	           
	           mag_query = Math.sqrt((Math.pow(mag_query, 2)+Math.pow(queryTF, 2)));
	           

	           List<Integer> tempTitle = (List<Integer>) obj.get("title");
	           Set<Integer> tempTitleSet = new HashSet();
	           tempTitleSet.addAll(tempTitle);
	           tempTitle.clear();
	           tempTitle.addAll(tempTitleSet);
	           
	           for(int docIDTitle: tempTitle){
	        	   
	   				String strDocIDTitle = "" + docIDTitle;

	   				RankObject objTitle;
	        	   
	        	   if(AndOrTitleMap.containsKey(strDocIDTitle)){
	        		   
	        		   objTitle = AndOrTitleMap.get(strDocIDTitle);
	        		   objTitle.qrWC++;
	        		   
	        	   }
	        	   else{
	        		   objTitle = new RankObject();
	        		   objTitle.qrWC++;
	        		   
	        	   }
	        	   
	        	   AndOrTitleMap.put(strDocIDTitle,objTitle);
	        	  

	           }
	           
	           
	           for(BasicDBObject tempObj: temp){
	        	   int countrobj = 1;
	        	   
	        	   String docid = tempObj.get("id").toString();
	        	   double tfidfrobj = (double)tempObj.get("tfidf");
	        	   	        	   
	        	   RankObject robj;
	        	   if(AndOrTempMap.containsKey(docid)){
	        		   robj = AndOrTempMap.get(docid);
	        		   countrobj = robj.qrWC;
	        		   robj.qrWC = countrobj+1;
	        		   
	        		   robj.tfidf_score += tfidfrobj;
	        		   
	        	   }
	        	   else{
	        		   robj = new RankObject();
	        		   robj.qrWC = countrobj;
	        		   robj.tfidf_score = tfidfrobj;
	        		   
	        	   }
	        	   robj.cos_score += (tfidfrobj*queryTF);
	        	   robj.mag_doc = Math.sqrt((Math.pow(robj.mag_doc, 2)+Math.pow(tfidfrobj, 2)));

        		   robj.posSet.addAll((List<Integer>)tempObj.get("pos"));
        		   
        		   if(queryWordPros == querySize){
        			   
        			   if(robj.qrWC == querySize){
        				   
        				   //Check
        					Iterator treIterator = robj.posSet.iterator();
        					int prev = 0;
        					boolean flag = false;
        					while(treIterator.hasNext()){
        						int curr = (int)treIterator.next();
        						if(prev>0){
        							if(prev == curr-1){
        								flag = true;
        								break;
        							}
        						}
    							prev = curr;
        					}
        					if(!flag)
             				   robj.posSet.clear();
        			   }
        			   else
        				   robj.posSet.clear();
        			   
        		   }
        		   
        		   AndOrTempMap.put(docid, robj);
	        	   	        	   
	           }
	       } 
	   }
	   queryMap.clear();

	   /*
       Iterator itrTitle = AndOrTitleMap.keySet().iterator();
       while(itrTitle.hasNext()){
    	   
    	  
    	   
    	   String keyTitle = (String)itrTitle.next();
    	   
    	   if(AndOrTitleMap.get(keyTitle).qrWC == querySize){
    		   AndOrTitleFinalMap.put(keyTitle, AndOrTitleMap.get(keyTitle));
    	   }
    	   
       }
	*/
       //AndOrTitleMap.clear();
       AndOrTitleFinalMap.clear();

       Iterator itrMap = AndOrTempMap.keySet().iterator();
       while(itrMap.hasNext()){
    	   
    	   String keyTemp = (String)itrMap.next();
    	   RankObject robjTemp = AndOrTempMap.get(keyTemp);
    	   double cos = robjTemp.cos_score/(robjTemp.mag_doc*mag_query);
    	   
    	   if(AndOrTitleMap.containsKey(keyTemp))
    		   robjTemp.page_rank = 10*(cos)+10*AndOrTitleMap.get(keyTemp).qrWC + robjTemp.tfidf_score;
    	   else
    		   robjTemp.page_rank = 10*(cos) + robjTemp.tfidf_score;

    	   AndOrTempMap.put(keyTemp, robjTemp);
    	   //if(!AndOrTitleFinalMap.containsKey(keyTemp)){
    		   AndOrMap.put(keyTemp, AndOrTempMap.get(keyTemp));
    	   //}
    	   
       }

       AndOrTempMap.clear();
	   
	   if(AndOrMap.size() == 0 && AndOrTitleFinalMap.size()==0){
		   
		   System.out.println("Not found\n");
		   return null;
		   
	   }
		   
	   	List<Entry<String, RankObject>> tokenPairList = new ArrayList<Entry<String, RankObject>>();

	   	if(AndOrTitleFinalMap.size() > 0){
	   		
			List<Entry<String, RankObject>> tokenPairTitleList = new ArrayList<Entry<String, RankObject>>(AndOrTitleFinalMap.entrySet());
			Collections.sort( tokenPairTitleList, new Comparator<Map.Entry<String, RankObject>>()
			{
				public int compare( Map.Entry<String, RankObject> mapEntry1, Map.Entry<String, RankObject> mapEntry2 )
				{
					return (new Integer(mapEntry2.getValue().qrWC)).compareTo( new Integer(mapEntry1.getValue().qrWC));
				}
			
			} );

		   	//tokenPairList.addAll(tokenPairTitleList);

	   	}

	   	
	   	if(AndOrMap.size()>0){
		  
	   Map<String,RankObject> AndOrMapTopOcc = new HashMap<String, RankObject>();
	   Map<String,RankObject> AndOrMapTop = new HashMap<String, RankObject>();
	   Map<String,RankObject> AndOrMapHigh = new HashMap<String, RankObject>();
	   Map<String,RankObject> AndOrMapLow = new HashMap<String, RankObject>();
 
	   	List<Entry<String, RankObject>> AndOrList = new ArrayList<Entry<String, RankObject>>(AndOrMap.entrySet());
	   //	List<Entry<String, RankObject>> AndOrListLow = new ArrayList<Entry<String, RankObject>>();

		for(Map.Entry<String, RankObject> mapEntry:AndOrList){

			boolean flag_title = false;
			if(AndOrTitleMap.containsKey(mapEntry.getKey())){
				//if(AndOrTitleMap.get(mapEntry.getKey()).qrWC > querySize/2)
					flag_title = true;
			}
			
			if((mapEntry.getValue().qrWC == querySize && mapEntry.getValue().posSet.size()>0) || flag_title){
				AndOrMapTopOcc.put(mapEntry.getKey(), mapEntry.getValue());
			}
			else if(mapEntry.getValue().qrWC == querySize){
				AndOrMapTop.put(mapEntry.getKey(), mapEntry.getValue());
			}
			else if(mapEntry.getValue().qrWC > ((querySize>3)?(querySize-2):1)){
				AndOrMapHigh.put(mapEntry.getKey(), mapEntry.getValue());
			}
			else{
				AndOrMapLow.put(mapEntry.getKey(), mapEntry.getValue());
			}
	
		}
	   	
		   	List<Entry<String, RankObject>> tokenPairTOList = new ArrayList<Entry<String, RankObject>>();
			   //	tokenPairTList = fnPositionSort(AndOrMapTop,searchArr);
			   	tokenPairTOList = fnSort(AndOrMapTopOcc);

	   	List<Entry<String, RankObject>> tokenPairTList = new ArrayList<Entry<String, RankObject>>();
	   //	tokenPairTList = fnPositionSort(AndOrMapTop,searchArr);
	   	tokenPairTList = fnSort(AndOrMapTop);


	   	List<Entry<String, RankObject>> tokenPairHList = new ArrayList<Entry<String, RankObject>>();
	   	tokenPairHList = fnSort(AndOrMapHigh);
	   	

		List<Entry<String, RankObject>> tokenPairLList = new ArrayList<Entry<String, RankObject>>();
		tokenPairLList = fnSort(AndOrMapLow);
		
	   	
	   	tokenPairList.addAll(tokenPairTOList);
	   	tokenPairList.addAll(tokenPairTList);
	   	tokenPairList.addAll(tokenPairHList);
	   	tokenPairList.addAll(tokenPairLList);
	   }
	   	

	   	System.out.println(tokenPairList.size());
		for(int i = 0; i< tokenPairList.size(); i++){
			Map.Entry<String, RankObject> mapEntry = tokenPairList.get(i);
			String key = mapEntry.getKey();
			
       		result.add(key);
		}

	   	return result;
	   	
       
  }
   
   public List<Entry<String, RankObject>> fnSort(Map<String,RankObject> mapToSort){
	   
		List<Entry<String, RankObject>> tokenPairList = new ArrayList<Entry<String, RankObject>>(mapToSort.entrySet());
		Collections.sort( tokenPairList, new Comparator<Map.Entry<String, RankObject>>()
		{
			public int compare( Map.Entry<String, RankObject> mapEntry1, Map.Entry<String, RankObject> mapEntry2 )
			{
			    Double value2 = Double.valueOf(mapEntry2.getValue().page_rank);    
			    Double value1 = Double.valueOf(mapEntry1.getValue().page_rank);
			    
				return (value2).compareTo( value1 );
			}
		
		} );

		return tokenPairList;
	   
  }
  
  
  public List<Entry<String, RankObject>> fnPositionSort(Map<String,RankObject> mapToSort, String[] searchArr){

		List<Entry<String, RankObject>> tokenPairList = new ArrayList<Entry<String, RankObject>>(mapToSort.entrySet());
		List<Entry<String, RankObject>> tokenPairHList = new ArrayList<Entry<String, RankObject>>();
		List<Entry<String, RankObject>> tokenPairLList = new ArrayList<Entry<String, RankObject>>();

		String docID = "";
		
		System.out.println(tokenPairList.size());
		int count = 0;
		
		for(Entry<String, RankObject> entry: tokenPairList){
			
			docID = entry.getKey();
			List<Integer> prevPosList = new ArrayList<Integer>();
			List<Integer> currPosList = new ArrayList<Integer>();
			boolean flag = false;
			
		   for(String word: searchArr){
			   
			   
			   
			   System.out.println(docID + "," + word);
			   
			   BasicDBObject query = new BasicDBObject();
		       query.put("word", word);
		       DBCursor cursor = coll.find(query);
		       
		       if (cursor.hasNext()) {   
		           DBObject obj = (DBObject) cursor.next();	                     
		           List<BasicDBObject> temp = (List<BasicDBObject>) obj.get("doc");
		           
		           for(BasicDBObject docobj:temp){
		        	   
		        	   if(docobj.get("id").equals(docID)){
		        		   
		        		   currPosList = (List<Integer>)docobj.get("pos");
		        		   
		        		   if(!prevPosList.isEmpty()){
		        			   
		        			   for(int doc:currPosList){
		        				   
		        				   if(prevPosList.contains(doc-1)){
		        					   flag = true;
		        					   break;
		        				   }
		        				   else{
		        					   flag=false;
		        				   }
		        				   
		        			   }
		        			   
		        			   
		        		   }
		        		   else{
		        			   flag = true;
		        		   }
	        			   prevPosList.clear();
	        			   prevPosList.addAll(currPosList);
	        			   break;
		        	   }
		        	   
		           }
		       }
		       
		       if(!flag)
		    	   break;
		       
		   }
		   
		   if(flag){
				System.out.println("if " + entry.getKey());

			   tokenPairHList.add(entry);
		   }
		   else{
				System.out.println("else " + entry.getKey());

			   tokenPairLList.add(entry);
		   }
		}
		
		Collections.sort( tokenPairHList, new Comparator<Map.Entry<String, RankObject>>()
		{
			public int compare( Map.Entry<String, RankObject> mapEntry1, Map.Entry<String, RankObject> mapEntry2 )
			{
			    Double value2 = Double.valueOf(mapEntry2.getValue().tfidf_score);    
			    Double value1 = Double.valueOf(mapEntry1.getValue().tfidf_score);
			    
				return (value2).compareTo( value1 );
			}
		
		} );

		Collections.sort( tokenPairLList, new Comparator<Map.Entry<String, RankObject>>()
		{
			public int compare( Map.Entry<String, RankObject> mapEntry1, Map.Entry<String, RankObject> mapEntry2 )
			{
			    Double value2 = Double.valueOf(mapEntry2.getValue().tfidf_score);    
			    Double value1 = Double.valueOf(mapEntry1.getValue().tfidf_score);
			    
				return (value2).compareTo( value1 );
			}
		
		} );

		tokenPairList.clear();
		tokenPairList.addAll(tokenPairHList);
		tokenPairList.addAll(tokenPairLList);
		
		return tokenPairList;
	   
  }

   
   public Map<String,ResultObject> fnGetUrl(List<String> docids, String search){
	   
	   Map<String,ResultObject> result = new HashMap<String, ResultObject>();

		for(int i = 0; i< docids.size(); i++){
			
			 BasicDBObject queryUrl = new BasicDBObject();
			 queryUrl.put("docID", docids.get(i));
			 DBCursor cursorUrl = coll1.find(queryUrl);
           
			 if (cursorUrl.hasNext()) { 
				 
				DBObject objUrl = (DBObject) cursorUrl.next();
                   ResultObject resObj = new ResultObject();
                   
                   //Process text to get snippet
                   String preview = "";
                   String text = objUrl.get("text").toString().toLowerCase();
            	   String[] searchArr = search.toLowerCase().split(" ");
            	   int lastIndex = text.length()-1;

            	   int prevPos = 0;
            	   
                   for(String word: searchArr){
                  
               	   
                	   if(text.contains(word) && lastIndex > 0){
                	   
                		   int pos = text.indexOf(word);
                		   
                		   
                		   if(prevPos != 0 && (pos-prevPos) < 250 ){
                			   
                			   continue;
                			   
                		   }
                		   
                		   if(pos-50 > 0){
                			   
                			   if(lastIndex-pos > 250){
                    			   preview += text.substring(pos-50, pos+250) + "... ";
                			   }
                			   else{
                    			   preview += text.substring(pos-50, lastIndex) + "... ";
                			   }
                			   
                		   }
                		   else if(pos-20 > 0){
                			   if(lastIndex-pos > 250){
                    			   preview += text.substring(pos-20, pos+250) + "... ";
                			   }
                			   else{
                    			   preview += text.substring(pos-20, lastIndex) + "... ";
                			   }
                		   }
                		   else{
                			   
                			   if(lastIndex-pos > 250){
                    			   preview += text.substring(pos, pos+250) + "... ";
                			   }
                			   else{
                    			   preview += text.substring(pos, lastIndex) + "... ";
                			   }
                		   }
                	   
                		   prevPos = pos;
                	   }
                   
                   }
                   
                   resObj.title = objUrl.get("title").toString();
                   resObj.preview = preview;
                   result.put(objUrl.get("url").toString(), resObj);
           }
		}
		
		return result;

	   
   }
   
   
   public void fnWriteDocMap(Map<String, String> docMap) throws UnknownHostException{
	   
	    Iterator itr = docMap.keySet().iterator();
	    while(itr.hasNext()){
	    	
	    	String id = (String)itr.next();
	    	String url = docMap.get(id);
	    	
            coll1.insert(new BasicDBObject("docID",id).
          		  append("url",url));

	    }
	    
   }
   
   public void fnCalculateTFIDF() throws UnknownHostException{
	   

        DBCursor cursor = coll1.find();
        int N = cursor.length();
        System.out.println("N: " + cursor.length());
	   
	    DBCollection coll = db.getCollection("InvIndex");
        DBCursor cursor1 = coll.find();
        
        while(cursor1.hasNext()){
        	
            DBObject obj = (DBObject) cursor1.next();
            
            List<BasicDBObject> temp = (List<BasicDBObject>) obj.get("doc");
            List<BasicDBObject> tempnew = new ArrayList<BasicDBObject>();
            int NT = temp.size();

            for(BasicDBObject tempInstObj: temp){
            	
            	
            	int tf = (int)tempInstObj.get("frequency");
            	
                double tfidf = (Math.log(1+tf))*(Math.log((float)N/NT));

                tempInstObj.put("tfidf", tfidf);
                tempnew.add(tempInstObj);
            	
            }
        	obj.put("doc", tempnew);
        	coll.save(obj);
        	
        }
   }
   
}
