# autocomplete_recommendation

run:- pip -r requirements.txt

Make sure the TAB data is stored in a tab_data folder in the same repository.

run:- python predictor.py

The server will be running on port http://127.0.0.1:8083/

You can search with below example.
http://127.0.0.1:8083/search?q={write your search query here}&type={'icd', 'cpt', 'med'}
