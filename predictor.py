# from whoosh.analysis import NgramWordAnalyzer, StemmingAnalyzer, StandardAnalyzer
from whoosh.qparser import QueryParser,FuzzyTermPlugin
from whoosh.index import open_dir
from whoosh import columns, fields, index, sorting
from whoosh.fields import *
import time
from create_collection import filteration,clean_string,create_indexed_data
from flask import Flask, request, jsonify, abort
from collections import OrderedDict

folder_path = './tab_data/'
y_n = input('Do you want to index the data. Enter y or n:')
if y_n.lower()=='y':
    create_indexed_data(folder_path)
index_data = {
    'icd': open_dir(f"indexed_data/icd_index"),
    'cpt': open_dir(f"indexed_data/cpt_index"),
    'med': open_dir(f"indexed_data/med_index")
}


app = Flask(__name__)
@app.route('/search', methods=['GET'])
def search():
    query = request.args.get('q')
    query_string = clean_string(query)
    type_of_data = request.args.get('type')
    if query_string:
        wildcard = '* '.join(query_string.split())+'*'
        fuzzy = '~2 '.join(query_string.split())+'~2'
        start_time = time.time()
        index = index_data[type_of_data]
        parser = QueryParser("string_search", schema=index.schema)
        parser.add_plugin(FuzzyTermPlugin())
        query = parser.parse(wildcard)
        type_of_search = 'wildcard'
        with index.searcher() as searcher:
            results = searcher.search(query, limit=80)
            if results.is_empty():
                query = parser.parse(fuzzy)
                results = searcher.search(query, limit=80)
                type_of_search = 'fuzzy'
            
            # merging the strings with same code
            top_results=OrderedDict()
            for result in results:
                if result['code'] not in top_results:
                    top_results[result['code']] = {
                        'code': result['code'],
                        'name': result['name'],
                        'norm_count': result['norm_count'],
                        'string_search': [],
                    }
                top_results[result['code']]['string_search'].append(result['string_search'])

        list_of_values = list(top_results.values())
        end_time = time.time()
                
        return {   
            'search_time_ms': int((end_time - start_time)*1000),
            'found': len(list_of_values),
            'type_of_search': type_of_search + ' (This field is added just for testing purposes)',
            'hits': list_of_values,
        }
    else:
        return abort(403, "String Cannot be Empty")

if __name__ == '__main__':
    app.run(port=8083)