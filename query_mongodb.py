from pymongo import MongoClient
import pandas as pd
from datetime import datetime

def execQueries(db_name, start_time_str, end_time_str):
    # Connect to MongoDB
    client = MongoClient('mongodb://127.0.0.1:27017/')
    db = client[db_name]

    # Collection for processed data
    collection = db.processed_data

    # Convert time strings to datetime objects
    start_time = datetime.strptime(start_time_str, "%d/%m/%Y %H:%M:%S")
    end_time = datetime.strptime(end_time_str, "%d/%m/%Y %H:%M:%S")

    # Fetch data from the collection within the specified time range
    data = list(collection.find({
        "time": {
            "$gte": start_time.strftime("%d/%m/%Y %H:%M:%S"),
            "$lte": end_time.strftime("%d/%m/%Y %H:%M:%S")
        }
    }))

    if not data:
        raise ValueError("No data found in the collection within the specified time range.")

    # Print keys to debug the structure of the data
    print(f"Keys in the data: {data[0].keys()}")

    # Convert the data into a DataFrame
    df = pd.DataFrame(data)

    # Ensure necessary columns are present
    if 'vcount' not in df.columns or 'vspeed' not in df.columns or 'link' not in df.columns:
        raise ValueError("The necessary columns are not present in the data.")

    # Convert relevant fields to numeric types
    df['vcount'] = df['vcount']
    df['vspeed'] = df['vspeed']

    # Find the edge with the smallest vehicle count
    min_vcount_edge = df.loc[df['vcount'].idxmin()]

    # Find the edge with the highest average speed
    max_vspeed_edge = df.loc[df['vspeed'].idxmax()]

    result = {
        "min_vcount_edge": {
            "link": min_vcount_edge['link'],
            "vcount": min_vcount_edge['vcount']
        },
        "max_vspeed_edge": {
            "link": max_vspeed_edge['link'],
            "vspeed": max_vspeed_edge['vspeed']
        }
    }

    return result

# Example usage
db_name = "big_data"
start_time_str = "01/01/2024 00:00:00"
end_time_str = "28/06/2024 23:59:59"
result = execQueries(db_name, start_time_str, end_time_str)
print(result)