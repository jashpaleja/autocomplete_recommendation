from tqdm import tqdm
import ujson
from glob import glob
import os
from collections import OrderedDict
import re

def create_and_save_inv_data(input_file_path: str, type_of_data: str ,get_all_lists: list, get_all_strings: list, weight: str):
    oldData = open(input_file_path, 'r')
    old_data = ujson.load(oldData)
    inv_data = {}
    for key, value in tqdm(old_data.items()):
        if key[:3] == type_of_data:
            includesArr = []
            for get_list in get_all_lists:
                includesArr += value.get(get_list, [])
            for get_string in get_all_strings:
                string_Val = value.get(get_string,'')
                if string_Val:
                    includesArr += [string_Val]
            includesArr = list(set(includesArr))
            for strings in includesArr:
                valSet = inv_data.setdefault(strings,{})
                valSet[key] = value.get(weight,0)

    document_array = []
    for index,(i, j) in enumerate(inv_data.items()):
        icd_code = max(inv_data[i], key=inv_data[i].get)
        document_array.append(
        {
            'id': str(index + 1),
            'string_search': i,
            'code': icd_code,
            'norm_count': inv_data[i][icd_code],
            'name': old_data[icd_code].get('name', '')
        }
        )
    return document_array


def create_collection_from_tab(folder_path: str, client):
    json_to_combine = {
        'icd':{
            'key': 'ICD',
            'get_all_lists':['includes', 'old_includes', 'old_graph_includes'],
            'get_all_strings': ['simple_dx', 'name'],
            'weight': 'norm_count'
        },
        'cpt':{
            'key': 'CPT',
            'get_all_lists':['includes', 'old_graph_includes'],
            'get_all_strings': ['simple_cpt', 'name'],
            'weight': 'norm_count'
        },
        'med':{
            'key': 'NDC',
            'get_all_lists':['includes', 'mgpi_display_hierachy', 'old_graph_includes_by_mgpi'],
            'get_all_strings': ['mgpi_display', 'name'],
            'weight': 'norm_count'
        }
    }

    inverse_results = {}
    for f in glob(os.path.join(folder_path, '*.json')):
        file_name = os.path.basename(f)
        code_type = file_name.split("_")[0].lower()
        print(f"Indexing {code_type} data from file:- {file_name}")
        v = json_to_combine[code_type]
        inverse_results[code_type] = create_and_save_inv_data(
            folder_path+file_name, 
            v['key'],
            v['get_all_lists'], 
            v['get_all_strings'], 
            v['weight'])
        
    for _ in list(inverse_results.keys()):
        try:
            client.collections[_].delete()
        except Exception as e:
            print(f"Collection {_} doesn't exist", e)
            # pass

        schema = {
        'name': _,
        'fields': [
            {
            'name'  :  'string_search',
            'type'  :  'string',
            "index": True
            },
            {
            'name'  :  'code',
            'type'  :  'string',
            'facet': True,
            },
            {
            'name'  :  'norm_count',
            'type'  :  'float',
            'facet': True,
            },
            {
            'name'  :  'name',
            'type'  :  'string',
            'facet': True,
            }
        ],
        'default_sorting_field': 'norm_count'
        }
        print(f'Indexing {_} data')
        client.collections.create(schema)
        data = inverse_results[_]
        client.collections[_].documents.import_(data, {'action': 'create'})



def clean_string(string):
    cleaned_string = re.sub(r"[^\w\s]", "", string)
    return cleaned_string.strip()

def all_responses_combined(client, search_type, search_params, max_pages, first_response):
    response = first_response['grouped_hits']
    i = 2
    while i<=max_pages: #max_pages
        page1_response = client.collections[search_type].documents.search(search_params)
        response.extend(page1_response['grouped_hits'])
        i+=1
    search_params['page']=i
    return response


def match_regex_contains(query, string, ordered, query_length):
    if query_length>1:
        if ordered: # it will check if the the query is present in the sentence in ordered format else check if unordered format
            pattern = r'\b.*?' + r'.*?'.join(re.escape(word) for word in query.split()) + r'.*?\b'
            return bool(re.search(pattern, string, flags=re.IGNORECASE))
        words = set(query.lower().split())
        return all(word in string.lower() for word in words)
    else:
        return bool(re.search(r'^'+query, string, flags=re.IGNORECASE)) if ordered else bool(re.search(r''+query, string, flags=re.IGNORECASE)) # it will check if the the sentence start with the query else it will check if the sentence contains the query


def filtered_sort_data(query_string , response):
    priority1 = [] # store if any string from the code has a exact match
    priority2 = [] # store if any string from the code has a all the words present in the string from the query and the order of the words remain the same
    priority3 = [] # store if any string from the code has a all the words present in the string from the query and are not in the same order
    priority4 = [] # store string from the code even if the word has 1 word present in the string and also store fuzzy matches.
    for groups in response:
        data_to_append={
            'code':groups['group_key'][0],
            'name':groups['group_key'][1],
            'norm_count':groups['group_key'][2],
        }
        response = []
        exact = []
        all_contains_ordered=[]
        all_contains_unordered=[]
        others = []
        query_length = len(query_string.split())
        score_obj = {'1':0, '2':0, '3':0, '4':0}
        for document in groups['hits']:
            matched_string = document['document']['string_search']
            if query_string.lower() == matched_string.lower():
                exact.append(matched_string)
                score_obj['1']+=1
            else:
                if match_regex_contains(query_string, matched_string, False, query_length):
                    if match_regex_contains(query_string, matched_string, True, query_length):
                        all_contains_ordered.append(matched_string)
                        score_obj['2']+=1
                    else:    
                        all_contains_unordered.append(matched_string)
                        score_obj['3']+=1
                else:
                    others.append(matched_string)
                    score_obj['4']+=1
        
        data_to_append['matched_strings'] = exact+all_contains_ordered+all_contains_unordered+others
        if score_obj['1']>0:
            priority1.append(data_to_append)
        elif score_obj['2']>0:
            priority2.append(data_to_append)
        elif score_obj['3']>0:
            priority3.append(data_to_append)
        else:
            priority4.append(data_to_append)

    return priority1+priority2+priority3+priority4