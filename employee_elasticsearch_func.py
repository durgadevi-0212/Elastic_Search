from elasticsearch import Elasticsearch, helpers
import csv

es = Elasticsearch(["http://localhost:8989"])

def create_Collection(p_collection_name):
    if not es.indices.exists(index=p_collection_name):
        es.indices.create(index=p_collection_name)
        print(f"Collection '{p_collection_name}' created.")
    else:
        print(f"Collection '{p_collection_name}' already exists.")

def index_Data(p_collection_name, p_exclude_column):
    with open('employee_data.csv', mode='r') as file:
        reader = csv.DictReader(file)
        actions = []
        
        for row in reader:
            row.pop(p_exclude_column, None)
            actions.append({
                "_index": p_collection_name,
                "_source": row
            })
        
        helpers.bulk(es, actions)
        print(f"Data indexed into '{p_collection_name}' excluding column '{p_exclude_column}'.")

def search_By_Column(p_collection_name, p_column_name, p_column_value):
    query = {
        "query": {
            "match": {
                p_column_name: p_column_value
            }
        }
    }
    results = es.search(index=p_collection_name, body=query)
    print(f"Search results for {p_column_name}='{p_column_value}':")
    for hit in results['hits']['hits']:
        print(hit['_source'])

def get_Emp_Count(p_collection_name):
    count = es.count(index=p_collection_name)['count']
    print(f"Total employee count in '{p_collection_name}': {count}")

def del_Emp_ById(p_collection_name, p_employee_id):
    query = {
        "query": {
            "match": {
                "employee_id": p_employee_id
            }
        }
    }
    es.delete_by_query(index=p_collection_name, body=query)
    print(f"Employee with ID '{p_employee_id}' deleted from '{p_collection_name}'.")

def get_Dep_Facet(p_collection_name):
    query = {
        "size": 0,
        "aggs": {
            "department_counts": {
                "terms": {
                    "field": "department.keyword"
                }
            }
        }
    }
    results = es.search(index=p_collection_name, body=query)
    print("Employee count by department:")
    for bucket in results['aggregations']['department_counts']['buckets']:
        print(f"Department: {bucket['key']}, Count: {bucket['doc_count']}")

