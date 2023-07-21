import typesense
from flask import Flask, request, abort
from create_collection import create_collection_from_tab, clean_string, all_responses_combined, filtered_sort_data
import time
import math

client = typesense.Client({
    'nodes': [{
        'host': 'localhost',
        'port': '8081',
        'protocol': 'http'
    }],
    'api_key': 'abcd'
})

folder_path = './tab_data/'
create_collection_from_tab(folder_path, client)

app = Flask(__name__)
@app.route('/search', methods=['GET'])
def search():
    query = request.args.get('q')
    query_string = clean_string(query)
    search_type = request.args.get('type')
    exhaustive_search = request.args.get('exhaustive_search')
    matched_string_all = request.args.get('matched_string_all')
    if query_string:
        search_params = {
            'q': query_string,
            'query_by': 'string_search',
            'sort_by': 'norm_count:desc',
            "exhaustive_search": bool(exhaustive_search=='true' and len(query_string)>1),
            "per_page": 250,
            'page':1,
            'group_by': 'code,name,norm_count',
            'group_limit': 20,
        }
        if bool(matched_string_all=='true'):
            search_params['group_limit'] = 50
        start_time = time.time()
        first_response = client.collections[search_type].documents.search(search_params)
        all_page_response = all_responses_combined(client,search_type,search_params, math.ceil(first_response['found']/250), first_response)
        typesense_time = time.time()
        filtered_arr = filtered_sort_data(query_string, all_page_response)
        end_time = time.time()
        return {
            'search_time_ms': int((end_time - start_time)*1000),
            'found': len(all_page_response),
            'hits': filtered_arr,
            'typesense_search_time' : int((typesense_time - start_time)*1000),
            'post_processing_time':int((end_time - typesense_time)*1000),
            'exhaustive_search':search_params['exhaustive_search'],
        }
    else:
        return abort(403, "String Cannot be Empty")

if __name__ == '__main__':
    app.run(port=8083)
