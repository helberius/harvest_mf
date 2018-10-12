import requests
from elasticsearch import Elasticsearch

import json
from math import ceil
import sys
import datetime
import gzip
import os

es = Elasticsearch([{'host': 'IP_ELASTICSEARCH', 'port': '9200'}])
SYNOP_REPO='/home/helbert/Documents/projects/SYNOP_REPO'

def process_request(period):

    if period=='all':
        """ If this option is called, the app will retrieve the monthly files for synop starting on January 1996, until current month"""
        get_all()
    elif period=='current_month':
        """This option will calculate the current month, cal request this file only """
        get_current_month()
    else:
        """ This option assumes the parameter period, refers to a specific month , then it will use this parameter to identify the file and request it."""
        ls_group_observations=get_one_specific_period(period)
        save_docs_to_es(ls_group_observations,'mf_synop')

def get_current_month():
    """This option will calculate the current month, cal request this file only """
    now=datetime.datetime.now()
    current_year=now.year
    current_month=now.month
    st_month=''
    if current_month<10:
        st_month='0'+str(current_month)
    else:
        st_month=str(current_month)
    period=str(current_year) + st_month
    ls_group_observations = get_one_specific_period(period)
    save_docs_to_es(ls_group_observations, 'mf_synop')


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
    """ This method, will save the list of docs ls_docs into the collection of documents identified by the parameter index
        the documents are python dictionaries, the dictionaries need to have a key 'id', i am indexing using this key

    """
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
    """ This option assumes the parameter period, refers to a specific month, formatted YYYYYMM , then it will use this parameter to identify the file and request it."""

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


            """ 
                Each line of the retrieved text files corresponds to a group of observations 
                (different variables, observed at the same time)
                In the original dataset the time is formatted as 20081001000000
                I am parsing this string, and calculating the timestamp
                I am also adding a field with the time formatted as ''date_iso'
                I am also adding a key 'id' that is the concatenation of the numer_sta and the date
                'id' is unique for each group of observations
                I am adding to the original data, the timestamp
            """

            format = '%Y%m%d%H%M%S'
            datestring = obs_year+obs_month+obs_day+obs_hour+obs_minute+'00'
            d = datetime.datetime.strptime(datestring, format)

            dict_obs_group['date_iso']=d.isoformat()
            dict_obs_group['timestamp']=d.timestamp()

            dict_obs_group['id']=dict_obs_group['numer_sta'] + '_' + dict_obs_group['date']
            ls_group_observations.append(dict_obs_group)
    return ls_group_observations


def get_all():
    """ If this option is called, the app will retrieve the monthly files for synop starting on January 1996, until current month"""
    now=datetime.datetime.now()
    init_year=1996
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
    """ To run the scrip 
    python main [all, current_month, (period, for instance 200810)]
    """
    period=sys.argv[1]
    print('selected period: ',period)
    process_request(period)
    print('work done')
