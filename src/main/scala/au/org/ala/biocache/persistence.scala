package au.org.ala.biocache

import collection.JavaConversions
import org.apache.cassandra.thrift.{SlicePredicate, Column, ConsistencyLevel}
import org.wyki.cassandra.pelops.{Policy, Selector, Pelops}
import collection.mutable.ListBuffer
import org.slf4j.LoggerFactory
import com.google.inject.name.Named
import com.google.inject.Inject
import java.lang.Class

/**
 * This trait should be implemented for Cassandra,
 * but could also be implemented for Google App Engine
 * or another backend supporting basic key value pair storage
 *
 * @author Dave Martin (David.Martin@csiro.au)
 */
trait PersistenceManager {

    /**
     * Get a single property.
     */
    def get(uuid:String, entityName:String, propertyName:String) : Option[String]

    /**
     * Get a key value pair map for this record.
     */
    def get(uuid:String, entityName:String): Option[Map[String, String]]

    /**
     * Put a single property.
     */
    def put(uuid:String, entityName:String, propertyName:String, propertyValue:String)

    /**
     * Put a set of key value pairs.
     */
    def put(uuid:String, entityName:String, keyValuePairs:Map[String, String])

    /**
     * Add a batch of properties.
     */
    def putBatch(entityName:String, batch:Map[String, Map[String,String]])

    /**
     * Retrieve an array of objects.
     */
    def getList(uuid:String, entityName:String, propertyName:String, theClass:java.lang.Class[AnyRef]) : List[AnyRef]

    /**
     * @overwrite if true, current stored value will be replaced without a read.
     */
    def putList(uuid:String, entityName:String, propertyName:String, objectList:List[AnyRef], overwrite:Boolean)

    /**
     * Page over all entities, passing the retrieved UUID and property map to the supplied function.
     * Function should return false to exit paging.
     */
    def pageOverAll(entityName:String, proc:((String, Map[String,String])=>Boolean), pageSize:Int = 1000)

    /**
     * Page over the records, retrieving the supplied columns only.
     */
    def pageOverSelect(entityName:String, proc:((String, Map[String,String])=>Boolean), pageSize:Int, columnName:String*)

    /**
     * Select the properties for the supplied record UUIDs
     */
    def selectRows(uuids:Array[String],entityName:String,propertyNames:Array[String],proc:((Map[String,String])=>Unit))

    /**
     * Close db connections etc
     */
    def shutdown
}

/**
 * Cassandra based implementation of a persistence manager.
 * This should maintain most of the cassandra logic
 */
class CassandraPersistenceManager @Inject() (
    @Named("cassandraHosts") host:String = "localhost",
    @Named("cassandraPort") port:String = "9160",
    @Named("cassandraPoolName") poolName:String = "biocache-store-pool",
    @Named("cassandraKeyspace") keyspace:String = "occ") extends PersistenceManager {

    val maxColumnLimit = 10000
    import JavaConversions._
    protected val logger = LoggerFactory.getLogger("CassandraPersistenceManager")
    logger.info("Initialising cassandra connection pool with pool name: " + poolName)
    logger.info("Initialising cassandra connection pool with hosts: " + host)
    logger.info("Initialising cassandra connection pool with port: " + port)
    Pelops.addPool(poolName, Array(host), port.toInt, false, keyspace, new Policy)

    /**
     * Retrieve an array of objects, parsing the JSON stored.
     */
    def get(uuid:String, entityName:String) = {
        val selector = Pelops.createSelector(poolName,keyspace)
        val slicePredicate = Selector.newColumnsPredicateAll(true, maxColumnLimit)
        try {
            val columnList = selector.getColumnsFromRow(uuid, entityName, slicePredicate, ConsistencyLevel.ONE)
            if(columnList.isEmpty){
                None
            } else {
                Some(columnList2Map(columnList))
            }
        } catch {
            case e:Exception => logger.debug(e.getMessage, e); None
        }
    }

    /**
     * Retreive the column value, handling NotFoundExceptions from cassandra thrift.
     */
    def get(uuid:String, entityName:String, propertyName:String) = {
      try {
          val selector = Pelops.createSelector(poolName, keyspace)
          val column = selector.getColumnFromRow(uuid, entityName, propertyName.getBytes, ConsistencyLevel.ONE)
          Some(new String(column.value))
      } catch {
          case e:Exception => logger.debug(e.getMessage, e); None
      }
    }

    /**
     * Store the supplied batch of maps of properties as separate columns in cassandra.
     */
    def putBatch(entityName: String, batch: Map[String, Map[String, String]]) = {
        val mutator = Pelops.createMutator(poolName, keyspace)
        batch.foreach(uuidMap => {
            val uuid = uuidMap._1
            val keyValuePairs = uuidMap._2
            keyValuePairs.foreach( keyValue => {
              mutator.writeColumn(uuid, entityName, mutator.newColumn(keyValue._1.getBytes, keyValue._2))
            })
        })
        mutator.execute(ConsistencyLevel.ONE)
    }

    /**
     * Store the supplied map of properties as separate columns in cassandra.
     */
    def put(uuid:String, entityName:String, keyValuePairs:Map[String, String]) = {
        val mutator = Pelops.createMutator(poolName, keyspace)
        keyValuePairs.foreach( keyValue => {
          //NC: only add the column if the value is not null
          if(keyValue._2!=null)
            mutator.writeColumn(uuid, entityName, mutator.newColumn(keyValue._1.getBytes, keyValue._2))
        })
        mutator.execute(ConsistencyLevel.ONE)
    }

    /**
     * Store the supplied property value in the column
     */
    def put(uuid:String, entityName:String, propertyName:String, propertyValue:String) = {
        val mutator = Pelops.createMutator(poolName, keyspace)
        mutator.writeColumn(uuid, entityName, mutator.newColumn(propertyName.getBytes, propertyValue))
        mutator.execute(ConsistencyLevel.ONE)
    }

    /**
     * Retrieve the column value, and parse from JSON to Array
     */
    def getList(uuid:String, entityName:String, propertyName:String, theClass:java.lang.Class[AnyRef]): List[AnyRef] = {
        val column = getColumn(uuid, entityName, propertyName)
        if (column.isEmpty) {
            List()
        } else {
            val json = new String(column.get.getValue)
            Json.toList(json,theClass)
        }
    }

    /**
     * Store arrays in a single column as JSON.
     */
    def putList(uuid:String, entityName:String, propertyName:String, newList:List[AnyRef], overwrite:Boolean) = {

        //initialise the serialiser
//        val gson = new Gson
        val mutator = Pelops.createMutator(poolName, keyspace)

        if (overwrite) {
            val json = Json.toJSON(newList)
            mutator.writeColumn(uuid, entityName, mutator.newColumn(propertyName, json))
        } else {

            //retrieve existing values
            val column = getColumn(uuid, entityName, propertyName)
            //if empty, write, if populated resolve
            if (column.isEmpty) {
                //write new values
                val json = Json.toJSON(newList)
                mutator.writeColumn(uuid, entityName, mutator.newColumn(propertyName, json))
            } else {
                //retrieve the existing objects
                val currentJson = new String(column.get.getValue)
                var currentList = Json.toList(currentJson, newList(0).getClass.asInstanceOf[java.lang.Class[AnyRef]])
                //   gson.fromJson(currentJson, propertyArray.getClass).asInstanceOf[Array[AnyRef]]

                var written = false
                var buffer = new ListBuffer[AnyRef]

                for (theObject <- currentList) {
                    if (!newList.contains(theObject)) {
                        //add to buffer
                        buffer + theObject
                    }
                }

                //PRESERVE UNIQUENESS
                buffer ++= newList

                // check equals
                val newJson = Json.toJSON(buffer.toList)
                mutator.writeColumn(uuid, entityName, mutator.newColumn(propertyName, newJson))
            }
        }
        mutator.execute(ConsistencyLevel.ONE)
    }

    /**
     * Generic page over method. Individual pageOver methods should provide a slicePredicate that
     * is used to determine the columns that aer returned...
     */
    def pageOver(entityName:String,proc:((String, Map[String,String])=>Boolean), pageSize:Int, slicePredicate:SlicePredicate)={
      val selector = Pelops.createSelector(poolName, keyspace)
      var startKey = ""
      var keyRange = Selector.newKeyRange(startKey, "", pageSize+1)
      var hasMore = true
      var counter = 0
      var columnMap = selector.getColumnsFromRows(keyRange, entityName, slicePredicate, ConsistencyLevel.ONE)
      var continue = true
      while (columnMap.size>0 && continue) {
        val columnsObj = List(columnMap.keySet.toArray : _*)
        //convert to scala List
        val keys = columnsObj.asInstanceOf[List[String]]
        startKey = keys.last
        for(uuid<-keys){
          val columnList = columnMap.get(uuid)
          //procedure a map of key value pairs
          val map = columnList2Map(columnList)
          //pass the record ID and the key value pair map to the proc
          continue = proc(uuid, map)
        }
        counter += keys.size
        keyRange = Selector.newKeyRange(startKey, "", pageSize+1)
        columnMap = selector.getColumnsFromRows(keyRange, entityName, slicePredicate, ConsistencyLevel.ONE)
        columnMap.remove(startKey)
      }
      println("Finished paging. Total count: "+counter)
    }

    /**
     * Pages over all the records with the selected columns.
     * @param columnName The names of the columns that need to be provided for processing by the proc
     */
    def pageOverSelect(entityName:String, proc:((String, Map[String,String])=>Boolean), pageSize:Int, columnName:String*)={

      val slicePredicate = Selector.newColumnsPredicate(columnName:_*)
      pageOver(entityName, proc, pageSize, slicePredicate)
    }

    /**
     * Iterate over all occurrences, passing the objects to a function.
     * Function returns a boolean indicating if the paging should continue.
     *
     * @param occurrenceType
     * @param proc
     */
    def pageOverAll(entityName:String, proc:((String, Map[String,String])=>Boolean), pageSize:Int = 1000) {
      val slicePredicate = Selector.newColumnsPredicateAll(true, maxColumnLimit)
      pageOver(entityName, proc, pageSize, slicePredicate)
    }

    /**
     * Select fields from rows and pass to the supplied function.
     */
    def selectRows(uuids:Array[String], entityName:String, fields:Array[String], proc:((Map[String,String])=>Unit)) {
       val selector = Pelops.createSelector(poolName, keyspace)
       var slicePredicate = new SlicePredicate
       slicePredicate.setColumn_names(fields.toList.map(_.getBytes))

       //retrieve the columns
       var columnMap = selector.getColumnsFromRows(uuids.toList, entityName, slicePredicate, ConsistencyLevel.ONE)

       //write them out to the output stream
       val keys = List(columnMap.keySet.toArray : _*)

       for(key<-keys){
         val columnsList = columnMap.get(key)
         val fieldValues = columnsList.map(column => (new String(column.name, "UTF-8"),new String(column.value, "UTF-8"))).toArray
         val map = scala.collection.mutable.Map.empty[String,String]
         for(fieldValue <-fieldValues){
           map(fieldValue._1) = fieldValue._2
         }
         proc(map.toMap)
       }
     }

    /**
     * Convert a set of cassandra columns into a key-value pair map.
     */
    protected def columnList2Map(columnList:java.util.List[Column]) : Map[String,String] = {
        val tuples = {
            for(column <- columnList)
                yield (new String(column.name, "UTF-8"), new String(column.value, "UTF-8"))
        }
        //convert the list
        Map(tuples map {s => (s._1, s._2)} : _*)
    }

    /**
     * Convienience method for accessing values.
     */
    protected def getColumn(uuid:String, columnFamily:String, columnName:String): Option[Column] = {
        try {
            val selector = Pelops.createSelector(poolName, keyspace)
            Some(selector.getColumnFromRow(uuid, columnFamily, columnName.getBytes, ConsistencyLevel.ONE))
        } catch {
            case e:Exception => logger.debug(e.getMessage + " for " + uuid + " - " + columnFamily + " - " +columnName); None //expected behaviour when row doesnt exist
        }
    }

    def shutdown = Pelops.shutdown
}

/**
 * To be added
 */
class MongoDBPersistenceManager extends PersistenceManager {
    def shutdown = {}

    def selectRows(uuids: Array[String], entityName: String, propertyNames: Array[String], proc: (Map[String, String]) => Unit) = null

    def pageOverSelect(entityName: String, proc: (String, Map[String, String]) => Boolean, pageSize: Int, columnName: String*) = null

    def pageOverAll(entityName: String, proc: (String, Map[String, String]) => Boolean, pageSize: Int) = null

    def putList(uuid: String, entityName: String, propertyName: String, objectList: List[AnyRef], overwrite: Boolean) = null

    def getList(uuid: String, entityName: String, propertyName: String, theClass: Class[AnyRef]) = null

    def putBatch(entityName: String, batch: Map[String, Map[String, String]]) = null

    def put(uuid: String, entityName: String, keyValuePairs: Map[String, String]) = null

    def put(uuid: String, entityName: String, propertyName: String, propertyValue: String) = null

    def get(uuid: String, entityName: String) = null

    def get(uuid: String, entityName: String, propertyName: String) = null
}