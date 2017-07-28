

* Download the last available version of the AACT data from [here](https://aact-prod.herokuapp.com/pipe_files)
* pip install -r requirements.txt
* edit the `DATA_DIR` variable in parser.py to point to the directory storing the  downlaoded data uncompressed
* edit the `ELASTICSEARCH_URL` variable in parser.py to point to your Elasticsearch cluster
* run
```
python parser.py
```