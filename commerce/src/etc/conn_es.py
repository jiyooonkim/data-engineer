from elasticsearch import Elasticsearch


def searchAPI(query):
    es = Elasticsearch('http://127.0.0.1:9200')
    index = 'search-2021*'

    index_name = "my_index"
    index_body = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "properties": {
                "title": {"type": "text"},
                "content": {"type": "text"}
            }
        }
    }


    es.indices.create(index=index_name, body=index_body)
    res = es.get(index=index_name)
    return res


if __name__ == "__main__":
    es = Elasticsearch('http://127.0.0.1:9200')
    es.info()
    data = es.cat.indices()
    print(data)
    query={'match':{'target_word':'0t장판'}}
    
    results = es.search(index='compound', query=query)
    print( results)
    for result in results['hits']['hits']:
        print('score:', result)