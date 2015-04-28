package org.apache.spark.sql.cassandra

import java.io.IOException

import com.google.common.cache.{LoadingCache, CacheBuilder, CacheLoader}
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
import org.apache.spark.sql.sources.{ResolvedDataSource, LogicalRelation}

import org.apache.spark.sql.types.StructType

private[cassandra] class CassandraCatalog(cc: CassandraSQLContext) extends Catalog with Logging {

  val caseSensitive: Boolean = true
  val metaStore: MetaStore = new CassandraDataSourceMetaStore(cc)

  // Create metastore keyspace and table if they don't exist
  metaStore.initMetaStore()

  /** A cache of Spark SQL data source tables that have been accessed. */
  private[cassandra] val cachedDataSourceTables: LoadingCache[Seq[String], LogicalPlan] = {
    val cacheLoader = new CacheLoader[Seq[String], LogicalPlan]() {
      override def load(tableIdent: Seq[String]): LogicalPlan = {
        logDebug(s"Creating new cached data source for $tableIdent")
        metaStore.getTable(tableIdentFromCatalystIdent(tableIdent))
      }
    }

    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  /** Obtain the Relation for a Cassandra table */
  override def lookupRelation(tableIdentifier: Seq[String], alias: Option[String]): LogicalPlan = {
    val id = processTableIdentifier(tableIdentifier).reverse.lift
    val tableName = id(0).getOrElse(throw new IOException(s"Missing table name"))
    val relation = cachedDataSourceTables.get(tableIdentifier)
    alias.map(a => Subquery(a, relation)).getOrElse(Subquery(tableName, relation))
  }

  /**
   * Only register table to local cache. To register table in meta store, use
   * registerTable(tableIdentfier, source, schema, options) method
   */
  override def registerTable(tableIdentifier: Seq[String], plan: LogicalPlan): Unit = {
    cachedDataSourceTables.put(tableIdentifier, plan)
  }

  /** Register a customized table meta data to local cache and meta store */
  def registerTable(
      tableIdentifier: Seq[String],
      source: String,
      schema: Option[StructType],
      options: Map[String, String]): Unit = {
    import DefaultSource._

    val tableIdent = tableIdentFromCatalystIdent(tableIdentifier)
    val resolvedRelation =
      if (source == CassandraDataSourceProviderName || source == DefaultSource.getClass.getCanonicalName) {
      val fullOptions = Map[String, String](
        CassandraDataSourceClusterNameProperty -> tableIdent.cluster.get,
        CassandraDataSourceKeyspaceNameProperty -> tableIdent.keyspace,
        CassandraDataSourceTableNameProperty -> tableIdent.table
      ) ++ options
      ResolvedDataSource(cc, schema, source, fullOptions)
    } else {
      ResolvedDataSource(cc, schema, source, options)
    }
    cachedDataSourceTables.put(tableIdentifier, LogicalRelation(resolvedRelation.relation))
    metaStore.storeTable(tableIdent, source, schema, options)
  }

  /** Unregister table from local cache and meta store. */
  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    cachedDataSourceTables.invalidate(tableIdentifier)
    metaStore.removeTable(tableIdentFromCatalystIdent(tableIdentifier))
  }

  /** Unregister all tables from local cache and meta store. */
  override def unregisterAllTables(): Unit = {
    cachedDataSourceTables.invalidateAll()
    metaStore.removeAllTables()
  }

  /** Check whether table exists */
  override def tableExists(tableIdentifier: Seq[String]): Boolean = {
    val tableIdent = tableIdentFromCatalystIdent(tableIdentifier)
    try {
      cachedDataSourceTables.get(tableIdentifier) != null
    } catch {
      case _: NoSuchElementException => false
    }
    false
  }

  /** All tables are not temporary tables */
  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    metaStore.getAllTables(databaseName)
  }

  /** Refresh CassandraContext schema cache, then refresh table in local cache */
  override def refreshTable(databaseName: String, tableName: String): Unit = {
    cc.refreshCassandraSchema(cc.getDefaultCluster)
    cachedDataSourceTables.refresh(Seq(cc.getDefaultCluster, databaseName, tableName))
  }

  /** Convert Catalyst tableIdentifier to TableIdent */
  private def tableIdentFromCatalystIdent(tableIdentifier: Seq[String]) : TableIdent = {
    val id = processTableIdentifier(tableIdentifier).reverse.lift
    val clusterName = id(2).getOrElse(cc.getDefaultCluster)
    val keyspaceName = id(1).getOrElse(cc.getKeyspace)
    val tableName = id(0).getOrElse(throw new IOException(s"Missing table name"))
    TableIdent(tableName, keyspaceName, Option(clusterName))
  }
}

object CassandraCatalog {
  val CassandraSQLSourceProviderDisableProperty = "spark.cassandra.sql.sources.disable"
}