import json
import time
from flask import Flask, render_template
from flask import jsonify, request, Response
from collections import defaultdict
import heapq
import redis
import pandas as pd

# redis_client = redis.StrictRedis(host='r-bp1t5jikzfiac5go4lpd.redis.rds.aliyuncs.com',password="wasd8456@", port=6379, db=0)
app = Flask(__name__)
redis_client = redis.StrictRedis(host='r-bp1t5jikzfiac5go4lpd.redis.rds.aliyuncs.com',password="wasd8456@", port=6379, db=0)
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
    query2 = "SELECT c.city, c.score FROM c"
    options = {"enableCrossPartitionQuery": True}  # 如果集合是分区集合，需要启用跨分区查询

    # 执行查询

    reviews_data_q = list(client.QueryDocuments(f"dbs/{DATABASE_ID}/colls/{COLLECTION_ID2}", query2, options))[:1000]


    reviews_data = []
    for item in reviews_data_q:
        reviews_data.append({
            "city": item['city'],
            "score": item['score']
        })

    return reviews_data


@app.route('/', methods=['GET'])
def hello():  # put application's code here
    return render_template('b.html')

@app.route('/2', methods=['GET'])
def world():  # put application's code here
    return render_template('a.html')

@app.route('/closest_cities', methods=['GET'])
def closest_cities():
    city_name = request.args.get('city')
    page_size = 50
    page = int(request.args.get('page'))

    # 尝试从 Redis 中获取缓存数据
    start_redis_time = time.time()
    cached_result = redis_client.get(f'closest_cities:{city_name}:{page}')
    end_redis_time = time.time()
    if cached_result:
        # 如果缓存数据存在，直接返回
        print("从缓存中获取数据")
        redis_time = int((end_redis_time - start_redis_time) * 1000)  # 转换为毫秒
        return Response(cached_result, content_type='application/json')

    # 如果缓存数据不存在，执行计算和排序操作
    cities_data = get_cities_data()
    start_time = time.time()

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

    # 将结果缓存到 Redis 中，设置过期时间（假设设置为 1 小时）
    redis_client.setex(f'closest_cities:{city_name}:{page}', 3600, result_json)

    # Return response
    return Response(result_json, content_type='application/json')
def calculate_eular_distance(x1, y1, x2, y2):
    x1, y1, x2, y2 = map(float, [x1, y1, x2, y2])
    return ((x1 - x2) ** 2 + (y1 - y2) ** 2) ** 0.5

def get_reviews_data1():
    file = pd.read_csv('static/amazon-reviews.csv')
    file = file[['city', 'score']]
    return file.to_dict(orient='records')

@app.route('/average_review', methods=['GET'])
def average_review():
    city_name = request.args.get('city')
    page_size = 10
    page = int(request.args.get('page', 0))

    # 尝试从 Redis 中获取缓存数据
    cached_result = redis_client.get(f'average_review:{city_name}:{page}')

    if cached_result:
        # 如果缓存数据存在，直接返回
        print("从缓存中获取数据")
        return Response(cached_result, content_type='application/json')

    cities_data = get_cities_data()
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

    display_cities = [city["city"] for city in sorted_cities]

    reviews_data = get_reviews_data()

    result = []
    for city1 in display_cities:
        # Fetch reviews data from Cosmos DB for the city
        reviews_city = [review for review in reviews_data if review["city"] == city1]

        if len(reviews_city) != 0 :
            # Calculate the average review score
            total_score = sum(float(review["score"]) for review in reviews_city)
            average_score = total_score / len(reviews_city)
            result.append({"city": city1, "average_review_score": average_score})
    start_index = page * page_size
    end_index = (page + 1) * page_size
    result = result[start_index:end_index]

    print(result)
    result_json = json.dumps({"result": result})
    redis_client.setex(f'average_review:{city_name}:{page}', 3600, result_json)

    return Response(result_json, content_type='application/json')



if __name__ == '__main__':
    app.run()
