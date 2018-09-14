import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{HashMap, Queue}
import java.io.{File, PrintWriter}

import ml.sparkling.graph.operators.algorithms.community.pscan.PSCAN
import org.apache.spark.graphx.{Edge, Graph}


object Community_Sparkling {
  val USER_ID = 0
  val MOVIE_ID = 1
  val output_file = "Piyush_Umate_Community_Sparkling.txt"

  def user_movie_map(line: String): (Int, Int) = {
    var l = line.split(",")
    (l(USER_ID).toInt, l(MOVIE_ID).toInt)
  }

  def read_input(data: RDD[String]): RDD[(Int, Set[Int])] = {
    val header = data.first()

    data.filter(line => line != header)
      .map(line => user_movie_map(line))
      .groupByKey()
      .mapValues(_.toSet)
      .persist()
  }

  def get_user_graph_adjacency_list(user_graph_edges: RDD[(Int, Int)]): RDD[(Int, Set[Int])] = {
    val adjacency_list_part_1 = user_graph_edges
      .groupByKey
      .mapValues(_.toSet)
      .persist()

    val adjacency_list_part_2 = user_graph_edges
      .map(_.swap)
      .groupByKey
      .mapValues(_.toSet)
      .persist()

    adjacency_list_part_1.union(adjacency_list_part_2)
      .reduceByKey(_ union _)
  }

  def get_user_edges(movies: RDD[(Int, Set[Int])], threshold: Int): Array[(Int, Int)] = {
    val user_id_combinations = movies
      .keys
      .collect()
      .combinations(2)

    val user_movies = movies.collect().toMap
    var similar_users : Array[(Int, Int)] = Array()

    for(Array(user_id_1, user_id_2) <- user_id_combinations) {
      val similar_movies = user_movies(user_id_1)
        .intersect(user_movies(user_id_2))
      if (similar_movies.size >= threshold) {
        similar_users :+= (user_id_1, user_id_2)
      }
    }

    similar_users
  }

  def bfs_traversal(adjacency_map: Map[Int, Set[Int]], user_id: Int, users_count: Int): Array[Set[Int]] = {
    var visited_nodes = Array.fill[Boolean](users_count+1)(false)
    var queue_1 = Queue(user_id)
    visited_nodes(user_id) = true
    var queue_2 = Queue[Int]()
    var level_wise_list : Array[Set[Int]] = Array()
    var current_index = -1
    while(queue_1.nonEmpty || queue_2.nonEmpty) {
      if(queue_1.nonEmpty) {
        current_index = current_index + 1
        level_wise_list :+= Set[Int]()
      }

      while(queue_1.nonEmpty) {
        val popped_node = queue_1.dequeue()
        level_wise_list(current_index) += popped_node
        for (adjacent_user_id <- adjacency_map(popped_node)) {
          if (!visited_nodes(adjacent_user_id)) {
            visited_nodes(adjacent_user_id) = true
            queue_2.enqueue(adjacent_user_id)
          }
        }
      }

      if(queue_2.nonEmpty) {
        current_index = current_index + 1
        level_wise_list :+= Set[Int]()
      }

      while(queue_2.nonEmpty) {
        val popped_node = queue_2.dequeue()
        level_wise_list(current_index) += popped_node
        for (adjacent_user_id <- adjacency_map(popped_node)) {
          if(!visited_nodes(adjacent_user_id)) {
            visited_nodes(adjacent_user_id) = true
            queue_1.enqueue(adjacent_user_id)
          }
        }
      }

    }

    level_wise_list
  }

  def calculate_node_weights(adjacency_map: Map[Int, Set[Int]], level_wise_list: Array[Set[Int]]): HashMap[Int, Double] = {
    var user_weights = HashMap.empty[Int, Double]
    val levels = level_wise_list.size

    if (levels >= 2) {
      for(user_id <- level_wise_list(1)) {
        user_weights(user_id) = 1
      }
    }

    for(current_level <- 2 to levels-1) {
      val current_level_nodes = level_wise_list(current_level)
      val parent_level_nodes = level_wise_list(current_level-1)
      for (user_id <- current_level_nodes) {
        val adjacent_parent_nodes = adjacency_map(user_id).intersect(
          parent_level_nodes)
        var sum = 0.0
        for(parent_node <- adjacent_parent_nodes) {
          sum += user_weights(parent_node)
        }
        user_weights(user_id) = sum
      }
    }
    user_weights
  }

  def calculate_edge_weights(adjacency_map: Map[Int, Set[Int]], node_weights: HashMap[Int, Double], level_wise_list: Array[Set[Int]]): HashMap[(Int, Int), Double] = {
    var edge_weights = HashMap.empty[(Int,Int), Double]
    var split_value = 0.0
    for (index <- 0 until level_wise_list.size-1) {
      for (user_id <- level_wise_list(index)) {
        if (index == 0) {
          split_value = 1
        } else {
          val adjacent_parent_user_ids = adjacency_map(user_id).intersect(
            level_wise_list(index-1)
          )
          var sum = 0.0
          for (adjacent_parent_id <- adjacent_parent_user_ids) {
            if (user_id > adjacent_parent_id) {
              sum += edge_weights((adjacent_parent_id, user_id))
            } else {
              sum += edge_weights((user_id, adjacent_parent_id))
            }
          }
          split_value = 1 + sum
        }
        val successor_nodes = adjacency_map(user_id).intersect(
          level_wise_list(index+1)
        )

        var sum = 0.0
        for (successor_node <- successor_nodes) {
          sum += node_weights(successor_node)
        }
        for (successor_node <- successor_nodes) {
          val edge_value = (node_weights(successor_node).toDouble / sum.toDouble) * split_value.toDouble
          if (successor_node < user_id) {
            edge_weights((successor_node, user_id)) = edge_value
          } else {
            edge_weights((user_id, successor_node)) = edge_value
          }
        }
      }
    }
    edge_weights
  }

  def compute_betweeness(user_ids: Iterator[Int], user_ids_count: Int, adjacency_map:Map[Int, Set[Int]]): Iterator[((Int, Int), Double)] = {
    var combined_edge_weights = collection.mutable.Map.empty[(Int,Int),Double]

    for (user_id <- user_ids) {
      val level_wise_list = bfs_traversal(adjacency_map, user_id, user_ids_count)
      val node_weights = calculate_node_weights(adjacency_map, level_wise_list)
      node_weights(user_id) = 1
      val user_edge_weights = calculate_edge_weights(adjacency_map, node_weights, level_wise_list.reverse)
      combined_edge_weights = combined_edge_weights ++ user_edge_weights.map{
        case (k,v) => k -> (v + combined_edge_weights.getOrElse(k,0.0))
      }
    }
    combined_edge_weights.toIterator
  }

  def main(args: Array[String]): Unit = {
    val t0 = System.currentTimeMillis()
    val conf = new SparkConf()
    val threshold = 9
    val spark_context = new SparkContext(conf)
    spark_context.setLogLevel("WARN")
    val file_path = args(0)
    var user_movies = read_input(
      spark_context.textFile(file_path)
    )

    val user_graph_edges = spark_context.parallelize(
      get_user_edges(user_movies, threshold)
    ).persist()

//    val user_graph_adjacency_list = get_user_graph_adjacency_list(
//      user_graph_edges
//    )
//
//    val user_ids_count = user_graph_adjacency_list.count()
//
//    var user_graph_adjacency_map = user_graph_adjacency_list
//      .collect()
//      .toMap
//
//
//    var user_graph_adjacency = Map(
//      6 -> Set(2,3,9),
//      2 -> Set(1,3,6),
//      3 -> Set(1,2,6),
//      9 -> Set(7,11,6),
//      1 -> Set(2,3,4,5),
//      4 -> Set(1,8,7),
//      7 -> Set(9,4,10),
//      11->Set(9,10),
//      5 -> Set(1,8),
//      8->Set(4,5,10),
//      10 -> Set(8,7,11)
//    )
//
//    //    val level_wise_list = bfs_traversal(user_graph_adjacency,1, 11)
//    //    println(user_graph_adjacency_map.size)
//
//    //    var node_weights = calculate_node_weights(user_graph_adjacency, level_wise_list)
//    //
//    //    node_weights(1) = 1
//    //
//    //    calculate_edge_weights(user_graph_adjacency, node_weights, level_wise_list.reverse)
//    //    x.foreach(println)
//
//    var betweeness_edges = user_graph_adjacency_list
//      .keys.mapPartitions(
//      user_ids => compute_betweeness(
//        user_ids, user_ids_count.toInt, user_graph_adjacency_map
//      )
//    ).reduceByKey(_+_)
//      .mapValues(v => v.toDouble/2.toDouble)
//      .sortByKey(true)
//      .persist()

    val edges: RDD[Edge[Double]] = user_graph_edges.map(
      edge => Edge(edge._1.toLong, edge._2.toLong, 0.0)
    )

    val graph : Graph[Any, Double] = Graph.fromEdges(edges, 0.0)
    val components = PSCAN.computeConnectedComponents(graph, epsilon = 0.8305)
    var connected_components = components.vertices.map(_.swap).groupByKey().mapValues(
      _.toArray.sorted
    ).values.collect()


    val sorted_max_sorted_communities = connected_components.sortWith(
      (a1, a2) => (a1.head < a2.head)
    )



    val writer = new PrintWriter(new File(output_file))
    val last_index = sorted_max_sorted_communities.size - 1

    for ((community, index) <- sorted_max_sorted_communities.zipWithIndex) {

      val formatted_string = "[" + community.mkString(",") + "]"

      if (index != last_index) {
        writer.write(s"$formatted_string\n")
      } else {
        writer.write(s"$formatted_string")
      }
    }
    writer.close()
//
//    println(connected_components.size)

//    val all_betweeness_edges = betweeness_edges.collect()
//    val writer = new PrintWriter(new File(output_file))
//    val last_index = all_betweeness_edges.size - 1
//
//
//    for ((((user_id_1, user_id_2),betweeness), index) <- all_betweeness_edges.zipWithIndex) {
//
//      val formatted_string = "(" + Array(
//        user_id_1.toString(),
//        user_id_2.toString(),
//        betweeness.toString()
//      ).mkString(",") + ")"
//
//
//      if (index != last_index) {
//        writer.write(s"$formatted_string\n")
//      } else {
//        writer.write(s"$formatted_string")
//      }
//    }
//    writer.close()
    println("Time: " + (System.currentTimeMillis() - t0)/1000 + "sec")


  }

}
