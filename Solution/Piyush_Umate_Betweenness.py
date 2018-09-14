from pyspark import SparkContext, SparkConf
import sys
from itertools import combinations
from operator import add
from collections import deque, Counter
file_path = sys.argv[1]
output_file = "Piyush_Umate_Betweenness.txt"
threshold = 9
import  time
t = time.time()
def user_movie_map(line):
    l = line.split(',')
    return int(l[0]), int(l[1])



def calculate_edge_weights(adjacency_list, node_weights, level_wise_list):
    edge_weights = {}
    for index,current_level in enumerate(level_wise_list[:-1]):
        for user_id in current_level:
            if index == 0:
                split_value = 1
            else:
                adjacent_parent_user_ids = adjacency_list[user_id]\
                    .intersection(level_wise_list[index - 1])
                sum = 0.0
                for adjacent_parent_id in adjacent_parent_user_ids:
                    if user_id > adjacent_parent_id:
                        sum += edge_weights[(adjacent_parent_id, user_id)]
                    else:
                        sum += edge_weights[(user_id, adjacent_parent_id)]
                split_value = 1 + sum
            successor_nodes = adjacency_list[user_id]\
                .intersection(level_wise_list[index+1])
            sum = 0.0
            for successor_node in successor_nodes:
                sum += node_weights[successor_node]
            for successor_node in successor_nodes:
                edge_value = (float(node_weights[successor_node])/float(sum)) * float(split_value)
                if successor_node < user_id:
                    edge_weights[(successor_node, user_id)] = edge_value
                else:
                    edge_weights[(user_id, successor_node)] = edge_value
    return edge_weights

def calculate_node_weights(adjacency_list, level_wise_list):
    user_weights = {}
    levels = len(level_wise_list)
    if levels >= 2:
        for user_id in level_wise_list[1]:
            user_weights[user_id] = 1

    for current_level in range(2, levels):
        current_level_nodes = level_wise_list[current_level]
        parent_level_nodes = level_wise_list[current_level-1]
        for user_id in current_level_nodes:
            adjacent_parent_nodes = adjacency_list[user_id].intersection(
                parent_level_nodes)
            sum = 0
            for parent_node in adjacent_parent_nodes:
                sum += user_weights[parent_node]
            user_weights[user_id] = sum

    return user_weights

def bfs_traversal(adjacency_list, user_id, users_count):
    visited_nodes = [False] * (users_count+1)
    queue_1 = deque([user_id])
    visited_nodes[user_id] = True
    queue_2 = deque([])
    level_wise_list = []
    current_index = -1
    while queue_1 or queue_2:
        if queue_1:
            current_index = current_index + 1
            level_wise_list.append(set([]))

        while queue_1:
            popped_node = queue_1.popleft()
            level_wise_list[current_index].add(popped_node)
            for adjacent_user_id in adjacency_list[popped_node]:
                if not visited_nodes[adjacent_user_id]:
                    visited_nodes[adjacent_user_id] = True
                    queue_2.append(adjacent_user_id)
            #add not visited adjacent nodes to q2

        if queue_2:
            current_index = current_index+1
            level_wise_list.append(set([]))
        while queue_2:
            popped_node = queue_2.popleft()
            level_wise_list[current_index].add(popped_node)
            for adjacent_user_id in adjacency_list[popped_node]:
                if not visited_nodes[adjacent_user_id]:
                    visited_nodes[adjacent_user_id] = True
                    queue_1.append(adjacent_user_id)
            #add not visited adjacent nodes to q1
    return level_wise_list

def get_user_edges(user_movies, threshold):

    user_id_combinations = combinations(
        user_movies.keys().collect(), 2
    )

    user_movies = dict(user_movies.collect())
    similar_users = []

    for (user_id_1, user_id_2) in user_id_combinations:
        similar_movies = user_movies[user_id_1]\
                .intersection(user_movies[user_id_2])
        if len(similar_movies) >= threshold:
            similar_users.append(
                (user_id_1,
                user_id_2)
            )

    # for index1 in range(user_count):
    #     for index2 in range(index1 + 1, user_count):
    #         similar_movies = user_movies[index1][1]\
    #             .intersection(user_movies[index2][1])
    #         if len(similar_movies) >=threshold:
    #             x.append(user_movies[index1][0])
    #             x.append(user_movies[index2][0])
    #
    #             similar_users.append(
    #                 (user_movies[index1][0],
    #                  user_movies[index2][0]
    #                 )
    #             )

    return similar_users

def get_user_graph_adjacency_list(user_graph_edges):
    adjacency_list_part_1 = user_graph_edges\
        .groupByKey()\
        .mapValues(set)\
        .persist()

    adjacency_list_part_2 = user_graph_edges\
        .map(lambda (user_id_1, user_id_2): (user_id_2, user_id_1))\
        .groupByKey()\
        .mapValues(set)\
        .persist()


    return adjacency_list_part_1.union(adjacency_list_part_2)\
        .reduceByKey(lambda x,y: x.union(y))


def compute_betweeness(user_ids, user_ids_count, adjacency_list):
    combined_edge_weights = Counter({})
    for user_id in user_ids:
        level_wise_list = bfs_traversal(adjacency_list, user_id, user_ids_count)
        node_weights = calculate_node_weights(adjacency_list, level_wise_list)
        node_weights[user_id] = 1
        combined_edge_weights.update(Counter(calculate_edge_weights(
            adjacency_list,
            node_weights,
            list(reversed(level_wise_list))
        )))
    yield combined_edge_weights.items()

spark_context = SparkContext(appName='Betweenness', conf=SparkConf())
spark_context.setLogLevel("WARN")

ratings = spark_context.textFile(file_path)
ratings_file_header = ratings.first()
user_movies = ratings \
    .filter(lambda line: line != ratings_file_header) \
    .map(lambda line: user_movie_map(line))\
    .groupByKey()\
    .mapValues(set)\
    .persist()


user_graph_edges = spark_context.parallelize(
    get_user_edges(user_movies, threshold)
).persist()




user_graph_adjacency_list = get_user_graph_adjacency_list(
    user_graph_edges
)
user_ids_count = user_graph_adjacency_list.count()
# print user_graph_adjacency_list.count()
# user_graph_adjacency_dict= dict([
#         (6, set([2,3,9])),
#         (2, set([1,3,6])),
#         (3, set([1,2,6])),
#         (9, set([7,11,6])),
#         (1, set([2,3,4,5])),
#         (4, set([1,8,7])),
#         (7, set([9,4,10])),
#         (11,set([9,10])),
#         (5,set([1,8])),
#         (8,set([4,5,10])),
#         (10, set([8,7,11])),
#     ])

# y = spark_context.parallelize(user_graph_adjacency_dict)
# # print y.collect()
# x = y.mapPartitions(
#     lambda user_ids:compute_betweeness(
#         user_ids,11, user_graph_adjacency_dict
#     )).flatMap(list)\
#     .reduceByKey(add)\
#     .mapValues(lambda v: float(v)/float(2))\
#     .sortByKey(True)\
#     .persist()
# print x.collect()
# exit(1)
# level_wise_list = bfs_traversal(user_graph_adjacency_dict,1, 11)
# node_weights = calculate_node_weights(user_graph_adjacency_dict, level_wise_list)
#
# node_weights[1] = 1
# print(node_weights)
# print calculate_edge_weights(user_graph_adjacency_dict, node_weights, list(reversed(level_wise_list)))
# exit(1)


user_graph_adjacency_dict = dict(user_graph_adjacency_list.collect())
betweeness_edges = user_graph_adjacency_list.keys().mapPartitions(
    lambda user_ids: compute_betweeness(
        user_ids, user_ids_count, user_graph_adjacency_dict)
    ).flatMap(list)\
    .reduceByKey(add)\
    .mapValues(lambda v: float(v)/float(2))\
    .sortByKey(True)\
    .persist()
# user_id = 1

def toCSVLine(k, v):
    return '(' + str(k[0]) + ',' + str(k[1]) + ',' + str(v) + ')'


lines = betweeness_edges.map(lambda (k, v): toCSVLine(k, v)).collect()

file = open(output_file, 'w')
file_content = '\n'.join(lines)
file.write(file_content)

print "Time: %s sec" % (time.time() - t)


# node_weights[user_id] = 1
# print node_weights
# print level_wise_list
# print edge_weights
# s1 = spark_context.parallelize([(1,set([2,3,4])), (4,set([5,6]))])
# s2 = spark_context.parallelize([(3,set([1,2])),(1,set([5]))])
# print s1.union(s2).reduceByKey(lambda x,y: x.union(y)).collect()


#print "time taken", time.time()-t