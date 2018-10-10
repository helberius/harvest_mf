import requests
from elasticsearch import Elasticsearch

import json
from math import ceil
import sys
import datetime
import gzip
import os

es = Elasticsearch([{'host': 'IP_ELASTIC', 'port': '9200'}])
SYNOP_REPO='/home/helbert/Documents/projects/SYNOP_REPO'

def process_request(period):
    if period=='all':
        get_all()
    else:
        ls_group_observations=get_one_specific_period(period)
        save_docs_to_es(ls_group_observations,'mf_synop')


def list_indexes():
    """
        An index is a collection of documents that have somewhat similar characteristics.
    For example, you can have an index for customer data, another index for a product catalog,
    and yet another index for order data. An index is identified by a name (that must be all lowercase)
    and this name is used to refer to the index when performing indexing, search, update, and delete
    operations against the documents in it.
    """
    ls_indices = es.indices.get_alias("*")
    return ls_indices

def save_docs_to_es(ls_docs, index):
    ls_indexes=list_indexes()
    if index in ls_indexes:
        print('the index already exist')
        query_count = {"query": {"match_all": {}}}
        count = es.count(index=index, body=query_count)
        print('number of features in ' + index + ' :' + str(count['count']) )
    else:
        print('the index does not exist, i will create one')
        es.indices.create(index=index, ignore=400)



    for d in ls_docs:
        es.index(index=index, doc_type='group_obs', id=d['id'], body=d)

    query_count = {"query": {"match_all": {}}}
    count = es.count(index=index, body=query_count)
    print('after insertion, number of features in ' + index + ' :' + str(count['count']) )


def get_one_specific_period(period):
    url='https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/Archive/synop.'+period+'.csv.gz'
    r=requests.get(url)
    file_name=os.path.join(SYNOP_REPO, ('synop_'+ period+'.txt'))
    file=open(file_name,'wb')
    file.write(r.content)
    file.close()

    ls_group_observations = []

    with open(file_name) as f:
        content=f.readlines()

        header=content[0]

        ls_header=header.replace('\n','').split(';')


        x=1

        while x<len(content):
            l=content[x]
            x=x+1
            #print(l)
            ls_info=l.replace('\n','').split(';')

            dict_obs_group={}
            index=0
            for k in ls_header:
                if k != '' and ls_info[index] != 'mq':
                    if k in ['numer_sta', 'date']:
                        dict_obs_group[k] = ls_info[index]
                    elif k in ['pmer','tend', 'cod_tend', 'u','ww','w1','w2','nbas','hbas','cl','cm','ch','pres','niv_bar','geop','tend24','sw','etat_sol']:
                        dict_obs_group[k] = int(ls_info[index])
                    else:
                        dict_obs_group[k] =float(ls_info[index])



                index=index+1

            obs_date=dict_obs_group['date']
            obs_year=obs_date[0:4]
            obs_month=obs_date[4:6]
            obs_day = obs_date[6:8]
            obs_hour=obs_date[8:10]
            obs_minute = obs_date[10:12]


            format = '%Y%m%d%H%M%S'
            datestring = obs_year+obs_month+obs_day+obs_hour+obs_minute+'00'
            d = datetime.datetime.strptime(datestring, format)

            dict_obs_group['date_iso']=d.isoformat()
            dict_obs_group['timestamp']=d.timestamp()

            dict_obs_group['id']=dict_obs_group['numer_sta'] + '_' + dict_obs_group['date']
            ls_group_observations.append(dict_obs_group)
    return ls_group_observations





    # with open(c) as f:
    #     content=f.readlines()
    # ls_lines=content.split('\n')
    #
    # print(len(ls_lines))

def get_all():
    now=datetime.datetime.now()
    init_year=2017
    end_year=now.year
    end_month=now.month
    print ('get_all')
    print (init_year, end_year, end_month)
    for y in range(init_year, (end_year+1)):
        print(y)
        if y !=end_year:
            for m in range(1,13):
                if m <10:
                    st_m='0'+ str(m)
                else:
                    st_m=str(m)
                print(y, st_m)
                period=str(y)+st_m
                ls_group_observations = get_one_specific_period(period)
                save_docs_to_es(ls_group_observations, 'mf_synop')


        else:
            for m in range(1,end_month):
                if m <10:
                    st_m='0'+ str(m)
                else:
                    st_m=str(m)
                print(y,st_m)
                period=str(y)+st_m
                ls_group_observations = get_one_specific_period(period)
                save_docs_to_es(ls_group_observations, 'mf_synop')

if __name__ == '__main__':
    print (sys.argv)
    period=sys.argv[1]
    print('selected period: ',period)
    process_request(period)

    print('work done')
