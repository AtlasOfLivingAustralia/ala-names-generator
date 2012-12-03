package au.org.ala.names

import org.scalaquery.session.Database
import com.jolbox.bonecp.BoneCPConfig
import com.jolbox.bonecp.BoneCP
 
class BoneCPDatabase extends Database{
    import java.sql.Connection
  import org.scalaquery.ql.extended.MySQLDriver
  Class.forName("com.mysql.jdbc.Driver")
  val driver = MySQLDriver
  val config = new BoneCPConfig
  //config.setJdbcUrl("jdbc:mysql://localhost/names_2012")
  config.setJdbcUrl("jdbc:mysql://152.83.193.197/ala_names")
  config.setUsername("root")
  config.setPassword("password")
  config.setStatementsCacheSize(1000)
  config.setMaxConnectionsPerPartition(15)
  config.setMinConnectionsPerPartition(2)
  val connectionPool = new BoneCP(config)
  
  /*
   * 
  // setup the connection pool
            BoneCPConfig config = new BoneCPConfig();
            config.setJdbcUrl("jdbc:hsqldb:mem:test"); // jdbc url specific to your database, eg jdbc:mysql://127.0.0.1/yourdb
            config.setUsername("sa"); 
            config.setPassword("");
            config.setMinConnectionsPerPartition(5);
            config.setMaxConnectionsPerPartition(10);
            config.setPartitionCount(1);
            connectionPool = new BoneCP(config); // setup the connection pool

   * 
   */
  
  override def createConnection(): Connection = {
    connectionPool.getConnection()
  } 
}