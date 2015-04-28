package org.apache.spark.sql.cassandra


import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.{ResolvedDataSource, LogicalRelation}
import org.apache.spark.sql.types.{StructType, DataType}

import com.datastax.driver.core.{Row, PreparedStatement}
import com.datastax.spark.connector.cql.{Schema, CassandraConnector}

import CSQLContext._

trait MetaStore {

  /**
   * Get a table from meta store. If it's not found in meta store, then look
   * up Cassandra tables to get the source table.
   *
   * @param tableIdent
   * @return
   */
  def getTable(tableIdent: TableIdent) : LogicalPlan

  /**
   * Get all table names for a keyspace. If keyspace is empty, get all tables from
   * all keyspaces.
   * @param keyspace
   * @return
   */
  def getAllTables(keyspace: Option[String]) : Seq[(String, Boolean)]

  /**
   * Only Store customized tables meta data in meta store
   *
   * @param tableIdentifier
   * @param source
   * @param schema
   * @param options
   */
  def storeTable(
      tableIdentifier: TableIdent,
      source: String,
      schema: Option[StructType],
      options: Map[String, String]) : Unit


  /**
   * Remove table from meta store
   *
   * @param tableIdent
   */
  def removeTable(tableIdent: TableIdent) : Unit


  /**
   * Remove all tables from meta store
   */
  def removeAllTables() : Unit

  /**
   * Create meta store keyspace and table in Cassandra
   */
  def initMetaStore() : Unit

}

/**
 * Store only customized tables or other data source tables. Cassandra data source tables
 * are directly lookup from Cassandra tables
 */
class CassandraDataSourceMetaStore(sqlContext: SQLContext) extends MetaStore {

  import CassandraDataSourceMetaStore._
  import DefaultSource._

  private val metaStoreConn = new CassandraConnector(sqlContext.getCassandraConnConf(Option(getMetaStoreCluster())))

  private val CreateMetaStoreKeyspaceQuery =
    s"""
      |CREATE KEYSPACE IF NOT EXISTS ${getMetaStoreKeyspace()}
      | WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """.stripMargin.replaceAll("\n", " ")

  private val CreateMetaStoreTableQuery =
    s"""
      |CREATE TABLE IF NOT EXISTS ${getMetaStoreKeyspace()}.${getMetaStoreTable()}
      | (cluster_name text,
      |  keyspace_name text,
      |  table_name text,
      |  source_provider text,
      |  schema_json text,
      |  options map<text, text>,
      |  PRIMARY KEY (cluster_name, keyspace_name, table_name))
    """.stripMargin.replaceAll("\n", " ")

  private val InsertIntoMetaStoreQuery =
    s"""
      |INSERT INTO ${getMetaStoreKeyspace()}.${getMetaStoreTable()}
      | (cluster_name, keyspace_name, table_name, source_provider, schema_json, options)
      | values (?, ?, ?, ?, ?, ?)
    """.stripMargin.replaceAll("\n", " ")

  private val InsertIntoMetaStoreWithoutSchemaQuery =
    s"""
      |INSERT INTO ${getMetaStoreKeyspace()}.${getMetaStoreTable()}
      | (cluster_name, keyspace_name, table_name, source_provider, options) values (?, ?, ?, ?, ?)
    """.stripMargin.replaceAll("\n", " ")


  override def getTable(tableIdent: TableIdent): LogicalPlan = {
      getTableFromMetastore(tableIdent).getOrElse(getTableMayThrowException(tableIdent))
  }

  override def getAllTables(keyspace: Option[String]): Seq[(String, Boolean)] = {
    val selectQuery =
      s"""
      |SELECT table_name, keyspace_name
      |From ${getMetaStoreKeyspace()}.${getMetaStoreTable()}
      |WHERE cluster_name = '${sqlContext.getDefaultCluster}'
    """.stripMargin.replaceAll("\n", " ")
    val names = ListBuffer[(String, Boolean)]()
    // Add source tables from meta store
    metaStoreConn.withSessionDo {
      session =>
        val result = session.execute(selectQuery).iterator()
        while (result.hasNext) {
          val row: Row = result.next()
          val tableName = row.getString(0)
          if (keyspace.nonEmpty) {
            val ks = row.getString(1)
            if (ks == keyspace.get)
              names += ((tableName, false))
          } else {
            names += ((tableName, false))
          }
        }
        names
    }

    // Add source tables from Cassandra tables
    val conn = new CassandraConnector(sqlContext.getCassandraConnConf(Option(sqlContext.getDefaultCluster)))
    if (keyspace.nonEmpty) {
      val ksDef = Schema.fromCassandra(conn).keyspaceByName.get(keyspace.get)
      names ++= ksDef.map(_.tableByName.keySet).getOrElse(Set.empty).map((name => (name, false)))
    }
    names
  }

  /** Store a tale with the creation meta data */
  override def storeTable(
      tableIdent: TableIdent,
      source: String,
      schema: Option[StructType],
      options: Map[String, String]): Unit = {
    import collection.JavaConversions._

    if (schema.nonEmpty) {
      metaStoreConn.withSessionDo {
        session =>
          val preparedStatement: PreparedStatement = session.prepare(InsertIntoMetaStoreQuery)
          session.execute(preparedStatement.bind(
            tableIdent.cluster.get,
            tableIdent.keyspace,
            tableIdent.table,
            source,
            schema.get.json,
            mapAsJavaMap(options)))
      }
    } else {
      metaStoreConn.withSessionDo {
        session =>
          val preparedStatement: PreparedStatement = session.prepare(InsertIntoMetaStoreWithoutSchemaQuery)
          session.execute(preparedStatement.bind(
            tableIdent.cluster.get,
            tableIdent.keyspace,
            tableIdent.table,
            source,
            mapAsJavaMap(options)))
      }
    }

  }

  override def removeTable(tableIdent: TableIdent) : Unit = {
    val deleteQuery =
      s"""
        |DELETE FROM ${getMetaStoreKeyspace()}.${getMetaStoreTable()}
        |WHERE cluster_name = '${quoted(tableIdent.cluster.get)}'
        | AND keyspace_name = '${quoted(tableIdent.keyspace)}'
        | AND table_name = '${quoted(tableIdent.table)}'
      """.stripMargin.replaceAll("\n", " ")

    metaStoreConn.withSessionDo {
      session => session.execute(deleteQuery)
    }
  }

  override def removeAllTables() : Unit = {
    metaStoreConn.withSessionDo {
      session => session.execute(s"TRUNCATE ${getMetaStoreKeyspace()}.${getMetaStoreTable()}")
    }
  }

  override def initMetaStore() : Unit = {
    metaStoreConn.withSessionDo {
      session =>
        session.execute(CreateMetaStoreKeyspaceQuery)
        session.execute(CreateMetaStoreTableQuery)
    }
  }

  /** Look up source table from meta store */
  private def getTableFromMetastore(tableIdent: TableIdent): Option[LogicalPlan] = {
    val SelectTableMetadataQuery =
      s"""
        |SELECT source_provider, schema_json, options
        |FROM ${getMetaStoreKeyspace()}.${getMetaStoreTable()}
        |WHERE cluster_name = '${quoted(tableIdent.cluster.get)}'
        |  AND keyspace_name = '${quoted(tableIdent.keyspace)}'
        |  AND table_name = '${quoted(tableIdent.table)}'
      """.stripMargin.replaceAll("\n", " ")

    metaStoreConn.withSessionDo {
      session =>
        val result = session.execute(SelectTableMetadataQuery)
        if (!result.isExhausted()) {
          val row: Row = result.one()
          val options : java.util.Map[String, String] = row.getMap("options", classOf[String], classOf[String])
          val schemaJsonString =
            Option(row.getString("schema")).getOrElse(options.get(CassandraDataSourceUserDefinedSchemaNameProperty))
          val schema : Option[StructType] =
            Option(schemaJsonString).map(DataType.fromJson).map(_.asInstanceOf[StructType])

          // convert to scala Map
          import scala.collection.JavaConversions._
          val resolvedRelation =
            ResolvedDataSource(sqlContext, schema, row.getString("source_provider"), options.toMap)
          Option(LogicalRelation(resolvedRelation.relation))
        } else {
          None
        }
    }
  }

  /** Add Cassandra table as source table to meta store */
  private def getTableMayThrowException(tableIdent: TableIdent) : LogicalPlan = {
    val conn = new CassandraConnector(sqlContext.getCassandraConnConf(tableIdent.cluster))
    //Throw NoSuchElementException if can't find table in C*
    Schema.fromCassandra(conn).keyspaceByName(tableIdent.keyspace).tableByName(tableIdent.table)
    val sourceRelation = sqlContext.createCassandraSourceRelation(tableIdent, CassandraDataSourceOptions())
    LogicalRelation(sourceRelation)
  }

  /** Get meta store schema keyspace name */
  private def getMetaStoreKeyspace() : String = {
    sqlContext.conf.getConf(CassandraDataSourceMetaStoreKeyspaceNameProperty,
      DefaultCassandraDataSourceMetaStoreKeyspaceName)
  }

  /** Get meta store schema table name */
  private def getMetaStoreTable() : String = {
    sqlContext.conf.getConf(CassandraDataSourceMetaStoreTableNameProperty,
      DefaultCassandraDataSourceMetaStoreTableName)
  }

  /** Get cluster name where meta store resides */
  private def getMetaStoreCluster() : String = {
    sqlContext.conf.getConf(CassandraDataSourceMetaStoreClusterNameProperty, sqlContext.getDefaultCluster)
  }

  /** Get the cluster names. Those cluster tables are loaded into meta store */
  private def toLoadClusters(): Seq[String] = {
    val clusters =
      sqlContext.conf.getConf(CassandraDataSourceToLoadClustersProperty, sqlContext.getDefaultCluster)
    clusters.split(",").map(_.trim)
  }
}

object CassandraDataSourceMetaStore {
  val DefaultCassandraDataSourceMetaStoreKeyspaceName = "data_source_meta_store"
  val DefaultCassandraDataSourceMetaStoreTableName = "data_source_meta_store"

  val CassandraDataSourceMetaStoreClusterNameProperty = "spark.cassandra.datasource.metastore.cluster";
  val CassandraDataSourceMetaStoreKeyspaceNameProperty = "spark.cassandra.datasource.metastore.keyspace";
  val CassandraDataSourceMetaStoreTableNameProperty = "spark.cassandra.datasource.metastore.table";
  //Separated by comma
  val CassandraDataSourceToLoadClustersProperty = "spark.cassandra.datasource.toload.clusters";

  val Properties = Seq(
    CassandraDataSourceMetaStoreClusterNameProperty,
    CassandraDataSourceMetaStoreKeyspaceNameProperty,
    CassandraDataSourceMetaStoreTableNameProperty,
    CassandraDataSourceToLoadClustersProperty
  )
}