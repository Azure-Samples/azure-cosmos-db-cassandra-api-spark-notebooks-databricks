// Databricks notebook source
// MAGIC %md 
// MAGIC ## Azure Databricks
// MAGIC ### Spark Structured streaming sample with with Azure Eventhub as source and Cosmosdb Cassandra as sink.
// MAGIC #### Databricks cluster configuration used for this sample: 
// MAGIC 
// MAGIC Runtime version 6.5(Spark 2.4.5, Scala 2.11) with 
// MAGIC [Spark Cassandra Connector 2.4.3 ](https://search.maven.org/artifact/com.datastax.spark/spark-cassandra-connector_2.11/2.4.3/jar) and [CosmosDB Cassandra API Spark helper]( https://search.maven.org/artifact/com.microsoft.azure.cosmosdb/azure-cosmos-cassandra-spark-helper/1.2.0/jar)
// MAGIC 
// MAGIC Sample Json data format
// MAGIC ```json
// MAGIC  {"PointId":"a45184f7-ac74-4f48-a4a3-2a550d079fbf","Temperature":20.41325593153632,"Humidity":55.11201771679894,"TimeStamp":"2020-06-16T14:05:14.4183386+05:30"}
// MAGIC ```
// MAGIC This sample consumes above json format data from eventhub and then for every distinct pointid's it creates 15 minute aggregations with avg, min and max temperature values and stores them to cosmosdb cassandra table.

// COMMAND ----------

import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra._
//Spark connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

//CosmosDB library for multiple retry
import com.microsoft.azure.cosmosdb.cassandra

// Specify connection factory for Cassandra
spark.conf.set("spark.cassandra.connection.factory", "com.microsoft.azure.cosmosdb.cassandra.CosmosDbConnectionFactory")

// Parallelism and throughput configs
spark.conf.set("spark.cassandra.output.batch.size.rows", "1")
spark.conf.set("spark.cassandra.connection.connections_per_executor_max", "10")
spark.conf.set("spark.cassandra.output.concurrent.writes", "100")
spark.conf.set("spark.cassandra.concurrent.reads", "512")
spark.conf.set("spark.cassandra.output.batch.grouping.buffer.size", "1000")
spark.conf.set("spark.cassandra.connection.keep_alive_ms", "60000000") //Increase this number as needed
spark.conf.set("spark.cassandra.output.ignoreNulls","true")

val connectionString = ConnectionStringBuilder("<YOUR EVENTHUB CONNECTION STRING>")
  .setEventHubName("<YOUR EVENTHUB NAME>")
  .build
val eventHubsConf = EventHubsConf(connectionString)
    .setConsumerGroup("$Default")
    .setStartingPosition(EventPosition.fromEndOfStream)
  
val eventhubs = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()

var sensorAggregateDF = eventhubs.select(get_json_object(($"body").cast("string"), "$.PointId").alias("PointId"),
                                get_json_object(($"body").cast("string"), "$.TimeStamp").alias("TimeStamp"),
                                get_json_object(($"body").cast("string"), "$.Temperature").alias("Temperature"))
                        .select($"PointId", 
                                to_timestamp($"TimeStamp").alias("TimeStamp"), 
                                ($"Temperature").cast("double"))
                        .groupBy($"PointId",window($"TimeStamp", "15 minute").as("TimeStamp")) // Create 15 minute tumbling windows for temperature aggregations for each unique point id.
                        .agg(avg("Temperature").as("Avg"),
                             max("Temperature").as("Max"),
                             min("Temperature").as("Min"), 
                             count("PointId").as("Count"))
    

sensorAggregateDF = sensorAggregateDF.select((concat($"PointId",lit("_"),$"TimeStamp.start")).as("id"), //Create Id as unique value for a given time window
                                            $"PointId".as("pointid"),
                                            $"TimeStamp.start".cast("string"),
                                            $"TimeStamp.end".cast("string"),
                                            $"Avg".as("avgtemp"),
                                            $"Min".as("mintemp"),
                                            $"Max".as("maxtemp"),
                                            $"Count".as("totalcount"))


//Write data to cosmosdb-casandra
val keySpace = "eventsdb"
val tableName="aggregations_15_min"
val query = sensorAggregateDF
                            .writeStream
                            .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
                              batchDF.write       // Use Cassandra batch data source to write streaming out
                                .cassandraFormat(tableName, keySpace)                               
                                .mode("append")
                                .save()
                            }
                            .outputMode("update")                            
                            .start()

