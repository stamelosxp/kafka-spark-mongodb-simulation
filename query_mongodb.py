from pymongo import MongoClient

def exec_queries():
    # Connect to MongoDB
    client = MongoClient('mongodb://127.0.0.1:27017/')
    db = client.big_data

    # Collection for processed data
    collection = db.processed_data

    # Question 1: Find the link with the smallest vehicle count
    min_vcount = collection.find().sort("vcount", 1).limit(1)
    print("Link with the smallest vehicle count:")
    for doc in min_vcount:
        print(doc)

    # Question 2: Find the link with the highest average speed
    max_vspeed = collection.find().sort("vspeed", -1).limit(1)
    print("Link with the highest average speed:")
    for doc in max_vspeed:
        print(doc)

    # Question 3: Find the highest spacing during a given time period
    # Assuming the time period is defined as a range of 'time' values
    start_time = "24/06/2024 19:01:04"
    end_time = "24/07/2024 19:01:04"
    max_spacing = collection.find({
        "time": {"$gte": start_time, "$lte": end_time}
    }).sort("spacing", -1).limit(1)
    print(f"Highest spacing between {start_time} and {end_time}:")
    for doc in max_spacing:
        print(doc)

    client.close()


if __name__ == '__main__':
    exec_queries()


