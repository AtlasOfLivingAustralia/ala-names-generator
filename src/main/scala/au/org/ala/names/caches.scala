package au.org.ala.names

object CachedObjects{  
  val cache = new scala.collection.mutable.HashMap[String, scala.collection.mutable.HashMap[String, Option[String]]]
  cache.put("kingdom", new scala.collection.mutable.HashMap[String, Option[String]])
  cache.put("phylum", new scala.collection.mutable.HashMap[String, Option[String]])
  cache.put("class", new scala.collection.mutable.HashMap[String, Option[String]])
  cache.put("order", new scala.collection.mutable.HashMap[String, Option[String]])
  cache.put("family",new scala.collection.mutable.HashMap[String, Option[String]])
  def getLsidFromCache(rank:String, scientificName:String):Option[String]={
    val map = cache.getOrElseUpdate(rank, new scala.collection.mutable.HashMap[String, Option[String]])
    //if(map.isDefined){
      map.synchronized{
        map.getOrElse(scientificName,None)
      }
//    }
//    else
//      None
  }
  def putLsidInCache(rank:String, scientificName:String, lsid:String){
    val map = cache.getOrElseUpdate(rank, new scala.collection.mutable.HashMap[String, Option[String]])
    
      map.synchronized{
        map.put(scientificName, Some(lsid))
      }
    
  }
  def getLock(rank:String):scala.collection.mutable.HashMap[String,Option[String]]=cache.get(rank).get
}