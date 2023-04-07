from datetime import datetime
from dateutil.rrule import rrule, DAILY, HOURLY, MONTHLY

import json

class Query():
    __slots__ = ["from_dt", "to_dt", "group_type"]

    def __init__( 
        self, from_dt: datetime, to_dt: datetime, group_type: str):

        self.from_dt = from_dt
        self.to_dt = to_dt
        self.group_type = group_type

def add_missed_values(query: Query, aggregation_result):
    PARSE_CONVERSION = {
        "month": MONTHLY,
        "day": DAILY,
        "hour": HOURLY,
    }

    dates = rrule(
        PARSE_CONVERSION[query.group_type],
        dtstart=query.from_dt, 
        until=query.to_dt
    )

    dataset_labels_pairs = list(zip(
        aggregation_result[0]["labels"],
        aggregation_result[0]["dataset"]
    ))
    
    for date in dates:
        if date not in aggregation_result[0]["labels"]:  
            dataset_labels_pairs.append((date, 0))

    dataset_labels_pairs = sorted(
        dataset_labels_pairs,
        key=lambda o: o[0]
    )

    result = {
        "dataset": [o[1] for o in dataset_labels_pairs],
        "labels":  [o[0] for o in dataset_labels_pairs]
    }

    return result

def aggregate_salaries(query: Query, mongo_collection):
    GROUPTYPES = {
        "month": "%Y-%m",
        "day": "%Y-%m-%d",
        "hour": "%Y-%m-%d-%H",
    }

    type = GROUPTYPES[query.group_type]
    
    pipeline = [
        {"$match": # match all records between from and to dates 
         {"$and": [
          {"dt": {"$gte": query.from_dt}},
          {"dt": {"$lte": query.to_dt}}]}},

         {"$group": { # group by type and collect sums and dates
             "_id": {"$dateToString": {
                "format": type,
                "date": "$dt"
             }},

             "date": {"$min": "$dt"},
             "datasets": {"$sum": "$value"},
           }
         },

         {"$project": { # truncate time to type
             "datasets": True,
             "date": {"$dateTrunc": {
                "date": "$date",
                "unit": query.group_type
             }},
           }
         },

         {"$sort": {"date": 1}}, # sort records by dates

         {"$group": {   # group by final sets 
             "_id": "null",
             "dataset": {"$push": "$datasets"},
             "labels": {"$push": "$date"},
           }
         },

         {"$project": { # exclude _id field
             "_id": False,
             "dataset": True,
             "labels": True,
           }
         },
    ]

    aggregation_result = list(mongo_collection.aggregate(pipeline)) 

    return add_missed_values(query, aggregation_result)

def query_from_json_string(input: str) -> Query:
    parsed = json.loads(input)

    from_date  = datetime.fromisoformat(parsed["dt_from"])
    to_date    = datetime.fromisoformat(parsed["dt_upto"])
    group_type = parsed["group_type"] 

    return Query(from_date, to_date, group_type)
