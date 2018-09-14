from pyspark import SparkContext, SparkConf
import sys
from itertools import product, combinations
from operator import add, and_, itemgetter
from collections import deque, Counter
import snap
import networkx as nx
file_path = sys.argv[1]
output_file = "Piyush_Umate_Community.txt"
threshold = 9
import  time
t = time.time()
def user_movie_map(line):
    l = line.split(',')
    return int(l[0]), int(l[1])


def generate_graph(user_graph_edges, user_ids_count):
    graph = nx.Graph()
    for node_1, node_2 in user_graph_edges:
        graph.add_edge(node_1, node_2)
    return graph

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

def compute_bfs(user_id, user_graph_adjacency_map, visited_nodes):
    queue = deque([user_id])
    bfs = []
    while queue:
        popped_node = queue.popleft()
        bfs.append(popped_node)
        adjacent_nodes = user_graph_adjacency_map[popped_node]
        for adjacent_node in adjacent_nodes:
            if not visited_nodes[adjacent_node-1]:
                visited_nodes[adjacent_node-1] = True
                queue.append(adjacent_node)
    return bfs

def compute_communities(user_graph):
    # visited_nodes = [False] * user_count
    # communities = []
    #
    # while not all(visited_nodes):
    #     visited_id = visited_nodes.index(False)
    #     visited_nodes[visited_id] = True
    #     user_id = visited_id + 1
    #
    #     communities.append(
    #         compute_bfs(
    #             user_id, user_graph_adjacency_map, visited_nodes
    #         )
    #     )
    # return communities
    return list(nx.connected_components(user_graph))

def calculate_community_modularity(community, one_by_2m,user_graph_adjacency_dict, degree_matrix):
    sum = 0.0
    for node_1 in community:
        for node_2 in community:
            aij = user_graph_adjacency_dict[node_1][node_2]
            ki = degree_matrix[node_1]
            kj = degree_matrix[node_2]
            sum += (aij - (float(ki*kj*one_by_2m)))
    return sum

def compute_modularity(communities, user_graph_edge_count, user_graph_adjacency_dict, degree_matrix):
    sum = 0.0
    one_by_2m = 1.0 /float(2 * user_graph_edge_count)
    for community in communities:
        node_combinations = combinations(community, 2)
        for (node_1, node_2) in node_combinations:
            aij = user_graph_adjacency_dict[node_1][node_2]
            #replace this code by prepocessing len
            ki = degree_matrix[node_1]
            kj = degree_matrix[node_2]
            sum += (aij - (float(ki*kj*one_by_2m)))
    return sum * one_by_2m
    # return (spark_context.parallelize(communities).map(
    #     lambda community: calculate_community_modularity(community,one_by_2m,user_graph_adjacency_dict, degree_matrix)
    # ).sum()* one_by_2m)

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


user_graph_edge_count = user_graph_edges.count()

user_graph_adjacency_list = get_user_graph_adjacency_list(
    user_graph_edges
)
user_ids_count = user_graph_adjacency_list.count()


user_graph = generate_graph(user_graph_edges.collect(), user_ids_count)
# CmtyV = snap.TCnComV()
# modularity = snap.CommunityGirvanNewman(graph, CmtyV)
# for Cmty in CmtyV:
#     print "Community: "
#     for NI in Cmty:
#         print NI
# print "The modularity of the network is %f" % modularity
# comp = nx.algorithms.community.centrality.girvan_newman(user_graph)
# print tuple(sorted(c) for c in next(comp))


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

degree_matrix = dict(user_graph_adjacency_list.map(lambda (k,v): (k, len(v))).collect())

# user_graph_adjacency_dict = dict(user_graph_adjacency_list.collect())


def set_is_edge_list(user, adjacent_users, user_ids_count):
    is_edge = [0.0] * (user_ids_count + 1)
    for adjacent_user in adjacent_users:
        is_edge[adjacent_user] = 1.0
    return user, is_edge

user_graph_adjacency_dict = dict(user_graph_adjacency_list.collect())
user_graph_adjacency_map = user_graph_adjacency_dict
betweeness_edges = user_graph_adjacency_list.keys().mapPartitions(
    lambda user_ids: compute_betweeness(
        user_ids, user_ids_count, user_graph_adjacency_dict)
    ).flatMap(list)\
    .reduceByKey(add)\
    .mapValues(lambda v: float(v)/float(2))\
    .sortBy(lambda (k,v): v, ascending=False)\
    .keys()\
    .persist()

betweeness_edges_rdd = betweeness_edges



user_graph_adjacency_dict = dict(user_graph_adjacency_list.map(
    lambda (user, adjacent_users): set_is_edge_list(user, adjacent_users, user_ids_count)
).collect())


betweeness_edges = deque(betweeness_edges.collect())
#
# old_communities_size = 1
# old_communities = [
#     user_graph_adjacency_list.keys().collect()
# ]
# previous_modularity = compute_modularity(
#     old_communities,
#     user_graph_edge_count,
#     user_graph_adjacency_dict,
#     degree_matrix
# )
#
# max_modularity = previous_modularity
# max_communities = old_communities
# print "hola",previous_modularity
# exit(1)
# max_index = None

def compute_chunk_modularity(edges, all_edges, user_graph, user_graph_edge_count, user_graph_adjacency_dict):
    edges = list(edges)
    start_edge = edges[0]
    for edge in all_edges:
        if edge == start_edge:
            break
        else:
            user_graph.remove_edge(edge[0], edge[1])

    edges = deque(edges)
    popped_edge_node_1, popped_edge_node_2 = edges.popleft()
    user_graph.remove_edge(popped_edge_node_1, popped_edge_node_2)
    old_communities = compute_communities(user_graph)

    old_communities_size = len(old_communities)

    previous_modularity = compute_modularity(
        old_communities,
        user_graph_edge_count,
        user_graph_adjacency_dict,
        degree_matrix
    )

    max_modularity = previous_modularity
    max_communities = old_communities

    while edges:
        popped_edge_node_1, popped_edge_node_2 = edges.popleft()
        user_graph.remove_edge(popped_edge_node_1, popped_edge_node_2)
        communities = compute_communities(user_graph)
        current_community_size = len(communities)
        if current_community_size != old_communities_size:
            current_modularity = compute_modularity(
                communities,
                user_graph_edge_count,
                user_graph_adjacency_dict,
                degree_matrix
            )
            if current_modularity > max_modularity:
                max_modularity = current_modularity
                max_communities = communities
        old_communities_size = current_community_size
    yield max_communities, max_modularity


communities = betweeness_edges_rdd.mapPartitions(
    lambda edges: compute_chunk_modularity(edges, betweeness_edges, user_graph, user_graph_edge_count, user_graph_adjacency_dict)
).max(key=lambda x:x[1])[0]

def f(community):
    return sorted(list(community))

communities = map(f, communities)
communities.sort(key=itemgetter(0))

file = open(output_file, 'w')
length = len(communities)
for index, community in enumerate(communities):
    if index != length - 1:
        file.write(str(community).replace(', ',',') + '\n')
    else:
        file.write(str(community).replace(', ',','))
# while betweeness_edges:
#     popped_edge_node_1, popped_edge_node_2 = betweeness_edges.popleft()
#     user_graph.remove_edge(popped_edge_node_1, popped_edge_node_2)
#     #
#     communities = compute_communities(user_graph)
#
#
#     current_community_size = len(communities)
#     if current_community_size != old_communities_size:
#         current_modularity = compute_modularity(
#             communities,
#             user_graph_edge_count,
#             user_graph_adjacency_dict,
#             degree_matrix
#         )
#         if current_modularity > max_modularity:
#             max_modularity = current_modularity
#             max_communities = communities
#     old_communities_size = current_community_size


#
# print "max communities", max_communities
# print "length", len(max_communities)
# print "max modularity", max_modularity

# print "len", max_index
# betweeness_edges_map = dict(betweeness_edges.collect())
# print betweeness_edges_map
# user_id = 1

# def toCSVLine(k, v):
#     return '(' + str(k[0]) + ',' + str(k[1]) + ',' + str(v) + ')'
#
#
# lines = betweeness_edges.map(lambda (k, v): toCSVLine(k, v)).collect()
#
# file = open(output_file, 'w')
# file_content = '\n'.join(lines)
# file.write(file_content)
#
print "Time: %s sec" % (time.time() - t)


# node_weights[user_id] = 1
# print node_weights
# print level_wise_list
# print edge_weights
# s1 = spark_context.parallelize([(1,set([2,3,4])), (4,set([5,6]))])
# s2 = spark_context.parallelize([(3,set([1,2])),(1,set([5]))])
# print s1.union(s2).reduceByKey(lambda x,y: x.union(y)).collect()


#print "time taken", time.time()-t
