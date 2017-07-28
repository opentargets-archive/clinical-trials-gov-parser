import logging
import os

import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk, streaming_bulk
from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim
from pandas.io import json
from tqdm import tqdm

geolocator = Nominatim()

INDEX_NAME = 'clinical-trials-gov-complete'
DOC_TYPE = 'clinical-trial'
DATA_DIR = os.path.join('data', '20170416_pipe-delimited-export')
ELASTICSEARCH_URL = 'http://localhost:9200'

geo_cache = {}


def parse_reference_line(line):
    dot_split = line.split('. ')
    if len(dot_split) < 2:
        return [], []
    author_string = dot_split[0]

    authors = [a.strip() for a in author_string.split(',')]
    title = dot_split[1].strip()  # this might truncate titles with a dot inside
    return authors, title



def get_coords_for_string(loc_string):
    if loc_string not in geo_cache:
        try:
            resolved_loc = geolocator.geocode(loc_string)
            if resolved_loc is not None:
                location = {
                    "lat": resolved_loc.latitude,
                    "lon": resolved_loc.longitude
                }
                geo_cache[loc_string] = location
                return location
        except GeocoderTimedOut:
            pass
    else:
        return geo_cache[loc_string]
    return {}


def data_iterator():
    data_dir = DATA_DIR

    base_file = 'studies'
    additional_files = ['brief_summaries',
                        'study_references',
                        'interventions',
                        'conditions',
                        'keywords',
                        'detailed_descriptions',
                        'drop_withdrawals',
                        'sponsors',
                        'outcomes',
                        'outcome_counts',
                        # 'outcome_analyses',
                        # 'outcome_measurements',
                        'milestones',
                        'facilities',
                        'facility_contacts',
                        'facility_investigators',
                        # 'eligibilities',
                        'designs',
                        # 'design_group_interventions'
                        'design_groups',
                        'design_outcomes',
                        'countries',
                        'baseline_counts',
                        # 'baseline_measurements',
                        'calculated_values',
                        'central_contacts',
                        'links'

                        ]

    base_df = pd.read_csv(os.path.join(data_dir, base_file + '.txt'), sep='|', dtype={'last_known_status': str})
    additional_data = {}
    for file_name in additional_files:
        print('parsing file: ' + file_name + '.txt')
        additional_data[file_name] = pd.read_csv(os.path.join(data_dir, file_name + '.txt'),
                                                 sep='|',
                                                 error_bad_lines=False,
                                                 # low_memory=False
                                                 )
        grouped = additional_data[file_name].groupby('nct_id', sort=False, as_index=False)
        grouped_df = pd.DataFrame(({'nct_id': nct_id, file_name:data.transpose().to_dict().values()} for nct_id, data in grouped))
        base_df = base_df.merge(grouped_df,on='nct_id', how='left')

    for i in tqdm(base_df.index,
                  desc='parsing clinical trials'):
        data = base_df.loc[i].to_dict()
        try:

            if isinstance(data['study_references'], list):
                for r, ref in enumerate(data['study_references']):
                    if 'citation' in ref:
                        authors, title = parse_reference_line(ref['citation'])
                        data['study_references'][r]['authors'] = authors
                        data['study_references'][r]['title'] = title
                        # del data['study_references'][r]['citation']
            if isinstance(data['countries'], list):
                for country in data['countries']:
                    country['location'] = get_coords_for_string(country['name'])
            doc = json.dumps(data)
            yield {
                '_index': INDEX_NAME,
                '_type': DOC_TYPE,
                '_id': data['nct_id'],
                '_source': doc
            }
        except:
            logging.exception('cannot parse entry %s ' % i)


es = Elasticsearch(hosts=[ELASTICSEARCH_URL])

print('deleting',INDEX_NAME,es.indices.delete(index=INDEX_NAME,ignore=404, timeout='300s'))
print('creating',INDEX_NAME,es.indices.create(index=INDEX_NAME, ignore=400, timeout='30s', body=json.load(open(
'mappings.json'))))

success, failed = 0, 0
for ok, item in streaming_bulk(es,
                              data_iterator(),
                              raise_on_error=False,
                              chunk_size=1000):
    if not ok:
        print('failed', ok, item)
        failed += 1
    else:
        success += 1


# complete = pd.merge(complete, references, on='nct_id')
# print(complete.groupby('nct_id').head())
