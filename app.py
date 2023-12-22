import json
import time
from flask import Flask, render_template
from flask import jsonify, request, Response
from collections import defaultdict
import heapq

# redis_client = redis.StrictRedis(host='r-bp1t5jikzfiac5go4lpd.redis.rds.aliyuncs.com',password="wasd8456@", port=6379, db=0)
app = Flask(__name__)

from pydocumentdb import document_client

# Azure Cosmos DB 连接信息
ENDPOINT = "https://tutorial-uta-cse6332.documents.azure.com:443/"
MASTERKEY = "fSDt8pk5P1EH0NlvfiolgZF332ILOkKhMdLY6iMS2yjVqdpWx4XtnVgBoJBCBaHA8PIHnAbFY4N9ACDbMdwaEw=="
DATABASE_ID = "tutorial"
COLLECTION_ID1 = "us_cities"
COLLECTION_ID2 = "reviews"

# 连接到 Azure Cosmos DB
client = document_client.DocumentClient(ENDPOINT, {'masterKey': MASTERKEY})

# 查询城市数据
def get_cities_data():
    query1 = "SELECT c.city, c.lat, c.lng FROM c"
    options = {"enableCrossPartitionQuery": True}  # 如果集合是分区集合，需要启用跨分区查询

    # 执行查询
    cities_data_q = list(client.QueryDocuments(f"dbs/{DATABASE_ID}/colls/{COLLECTION_ID1}", query1, options))

    cities_data = []
    for item in cities_data_q:
        cities_data.append({
            "city": item['city'],
            "lat": item['lat'],
            "lng": item['lng']
        })


    return cities_data
def get_reviews_data():
    query2 = "SELECT c.city, c.review FROM c"
    options = {"enableCrossPartitionQuery": True}  # 如果集合是分区集合，需要启用跨分区查询

    # 执行查询

    reviews_data_q = list(client.QueryDocuments(f"dbs/{DATABASE_ID}/colls/{COLLECTION_ID2}", query2, options))


    reviews_data = []
    for item in reviews_data_q:
        reviews_data.append({
            "city": item['city'],
            "review": item['review']
        })

    return reviews_data


@app.route('/', methods=['GET'])
def hello_world():  # put application's code here
    return render_template('a.html')

@app.route('/closest_cities', methods=['GET'])
def closest_cities():
    cities_data = get_cities_data()
    start_time = time.time()
    city_name = request.args.get('city')
    page_size = int(request.args.get('page_size', 50))
    page = int(request.args.get('page', 0))

    # Check if data is already in Redis
    # redis_key = f"closest_cities:{city_name}:page{page}"
    # redis_data = redis_client.get(redis_key)

    # if redis_data:
    #     return Response(redis_data, content_type='application/json')

    # Fetch data from Cosmos DB (replace this with actual Cosmos DB query)
    city_data = next((city for city in cities_data if city["city"] == city_name), None)

    if not city_data:
        return jsonify({"error": "City not found"}), 404

    # Process data and calculate Eular distances
    all_cities_distances = []
    for other_city in cities_data:
        if other_city["city"] != city_name:
            distance = calculate_eular_distance(city_data["lat"], city_data["lng"], other_city["lat"], other_city["lng"])
            all_cities_distances.append({"city": other_city["city"], "distance": distance})

    # Sort cities by distance
    sorted_cities = sorted(all_cities_distances, key=lambda x: x["distance"])

    # Paginate the result
    start_index = page * page_size
    end_index = (page + 1) * page_size
    if (end_index > len(sorted_cities)):
        end_index = len(sorted_cities)
    print(len(sorted_cities))
    paginated_result = sorted_cities[start_index:end_index]
    end_time = time.time()
    computing_time = int((end_time - start_time) * 1000)  # 转换为毫秒
    # Convert result to JSON format
    result_json = json.dumps({"result": paginated_result, "time_of_computing": computing_time})

    # Save result to Redis
    # redis_client.set(redis_key, result_json)

    # Return response
    return Response(result_json, content_type='application/json')

def calculate_eular_distance(x1, y1, x2, y2):
    x1, y1, x2, y2 = map(float, [x1, y1, x2, y2])
    return ((x1 - x2) ** 2 + (y1 - y2) ** 2) ** 0.5



# Endpoint for Q11
with open("static/stopwords.txt", "r", encoding="utf-8") as stopwords_file:
    stopwords = set(stopwords_file.read().splitlines())
print(stopwords)

@app.route('/knn_reviews', methods=['GET'])
def knn_reviews():
    reviews_data = get_reviews_data()
    start_time = time.time()

    classes = int(request.args.get('classes', 3))
    k = int(request.args.get('k', 2))
    words = int(request.args.get('words', 100))

    # Implement KNN algorithm and process data
    clusters = knn_algorithm(reviews_data, k, classes, words, stopwords)

    # Convert clusters to JSON format
    clusters_json = json.dumps(clusters)
    end_time = time.time()
    # Save result to Redis
    # redis_key = f"knn_reviews:{k}_{classes}_{words}"
    # redis_client.set(redis_key, clusters_json)

    # 计算实际的计算时间
    computing_time = int((end_time - start_time) * 1000)  # 转换为毫秒
    # Return response
    return jsonify({"result": clusters, "time_of_computing": computing_time})

def knn_algorithm(reviews_data, k, classes, words, stopwords):
    # Simplified KNN algorithm
    clusters = defaultdict(list)
    words_frequency = defaultdict(int)
    city_population = defaultdict(int)

    for review in reviews_data:
        city = review["city"]
        words_in_review = set([word.lower() for word in review["review"].split() if word.lower() not in stopwords])

        # Update words frequency
        for word in words_in_review:
            words_frequency[word] += 1

        # Update city population
        city_population[city] += 1

    # Find the k nearest neighbors for each city
    for review in reviews_data:
        city = review["city"]
        words_in_review = set([word.lower() for word in review["review"].split() if word.lower() not in stopwords])

        distances = []
        for other_city in city_population:
            if other_city != city:
                other_city_words = set([word for word in words_in_review if word in [w["review"].lower().split() for w in reviews_data if w["city"] == other_city]])
                distance = len(words_in_review.intersection(other_city_words))
                heapq.heappush(distances, (distance, other_city))

        # Select k nearest neighbors
        nearest_neighbors = [city for _, city in heapq.nlargest(k, distances)]

        # Add review to the cluster of the city with the nearest neighbors
        cluster_key = frozenset(sorted([city] + nearest_neighbors))
        clusters[cluster_key].append({"city": city, "review": review["review"]})

    # Calculate the weighted average score for each cluster
    result_clusters = []
    for cluster_key, reviews in clusters.items():
        center_city = list(cluster_key)[0]
        total_weighted_score = sum(city_population[city] for city in cluster_key)
        weighted_average_score = sum(city_population[city] * len(review["review"]) for review in reviews) / total_weighted_score

        result_clusters.append({
            "center_city": center_city,
            "cities": list(cluster_key),
            "words": {word: words_frequency[word] for word in words_frequency},
            "weighted_average_score": weighted_average_score
        })

    return result_clusters

if __name__ == '__main__':
    app.run()
