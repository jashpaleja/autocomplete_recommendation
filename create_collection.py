# from whoosh.analysis import NgramWordAnalyzer, StemmingAnalyzer, StandardAnalyzer
from whoosh.index import open_dir,create_in
from whoosh.fields import *
from collections import OrderedDict
import os
import shutil
import re
from tqdm import tqdm
import ujson
from glob import glob
import os

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
                valSet[key] = value.get(weight,float(0.0))

    document_array = []
    for index,(i, j) in enumerate(inv_data.items()):
        icd_code = max(inv_data[i], key=inv_data[i].get)
        document_array.append(
        {
            'string_search': i,
            'code': icd_code,
            'norm_count': inv_data[i][icd_code],
            'name': old_data[icd_code].get('name', '')
        }
        )
    return document_array

def indexing_data(inverse_results):
    # Cleaning preindexed data
    for type_of_data, values in inverse_results.items():
        try:
            shutil.rmtree(f"indexed_data/{type_of_data}_index")
            print(f"Folder 'indexed_data/{type_of_data}_index' deleted successfully.")
        except OSError as e:
            pass
            # print(f"Error: {e}")

    # Indexing data
    for type_of_data, values in inverse_results.items():
        schema = Schema(name=STORED, string_search=TEXT(stored=True), code=STORED, norm_count=STORED)
        if not os.path.exists(f"indexed_data"):
            os.mkdir(f"indexed_data")
        if not os.path.exists(f"indexed_data/{type_of_data}"):
            os.mkdir(f"indexed_data/{type_of_data}_index")
        index = create_in(f"indexed_data/{type_of_data}_index", schema)
        writer = index.writer()
        print(f'Creating index for {type_of_data} data')
        for obj in tqdm(values):
            if obj['name']:
                writer.add_document(name=obj['name'], string_search=u''+obj['string_search'],
                            code=obj['code'], norm_count=float(obj['norm_count']), _boost=float(obj['norm_count']))
        writer.commit()

def create_indexed_data(folder_path:str):
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
        print(f"Preprocessing {file_name}")
        v = json_to_combine[code_type]
        inverse_results[code_type] = create_and_save_inv_data(
            folder_path+file_name, 
            v['key'],
            v['get_all_lists'], 
            v['get_all_strings'], 
            v['weight'])
        
    indexing_data(inverse_results)

def clean_string(string):
    cleaned_string = re.sub(r"[^\w\s]", "", string)
    return re.sub(' +', ' ', cleaned_string)

def filteration(results):
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
    return list(top_results.values())