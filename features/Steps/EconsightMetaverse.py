from pathlib import Path
from behave import *
import pandas as pd
import numpy as np
import requests
import jsondiff
from io import BytesIO
from datetime import datetime
import json
import logging
from pandas.testing import assert_frame_equal
from pandas.testing import assert_series_equal
from urllib.parse import urlencode
from rtutility.dataop.fetch.database import ImportFromDatabase

from logging import basicConfig, DEBUG, info, debug

basicConfig(level=DEBUG, format="%(levelname)s %(asctime)s : %(message)s")


def convert(values):
    try:
        return int(values)
    except:
        return ''


@given('Fetch the EconsightMetaverse data from {EconsightMetaverse}')
def fetch_data(context,EconsightMetaverse:str):
    with open(Path(EconsightMetaverse)) as fp:
        context.EconsightMetaverse_query = fp.read()

@given('Fetch the EconsightMetaverse from api {base_url}')
def base_url(context,base_url:str):
    context.Econ_base_url = base_url

@when('Read the Following parameters')
def with_params(context):
    context.Eco_with_params = (f"{context.Econ_base_url}?{urlencode(dict(context.table))}")


@then('Make api requests')
def api_request(context):
    context.api_request = requests.get(context.Eco_with_params,verify=False)
    if context.api_request.status_code == 200:
        context.api_request_api = context.api_request.content
        logging.info(context.api_request.status_code)
    else:
        raise ValueError("NO response")

@then('save the api reponse')
def save_response(context):
    context.Api_Data_ECO=pd.read_csv(BytesIO(context.api_request_api),na_filter=False,sep='|')


@when('Fetch the Data for EconsightMetaverse from DB at {Cutoff_date}')
def Econ_data(context,Cutoff_date:str):
    k = "','".join(context.Api_Data_ECO['internalId'].to_list())
    k= "'"+k+"'"
    context.ECO_DB = context.EconsightMetaverse_query.format(datetime.strptime(Cutoff_date,'%Y-%m-%d').strftime('%Y%m%d'),
                                                             datetime.strptime(Cutoff_date,'%Y-%m-%d').strftime('%Y%m%d'),
                                                             datetime.strptime(Cutoff_date,'%Y-%m-%d').strftime('%Y%m%d'))
    context.Brutus_SIDEXT = ImportFromDatabase("brutus1.bat.ci.dom","SIDExternal")
    context.Brutus_SIDEXT.query = context.ECO_DB
    context.ECO_DB_Data = context.Brutus_SIDEXT.read()
    context.df = context.ECO_DB_Data.copy()
    
    context.df['worldClassPatentsInTechnology'] = context.df['worldClassPatentsInTechnology'].apply(convert)
    # context.df['totalPatentsInTechnology'] = pd.to_numeric(context.df['totalPatentsInTechnology'],errors='coerce').fillna(context.df['totalPatentsInTechnology']).astype('int64')
    # context.df['environment'] = pd.to_numeric(context.df['environment'],errors='coerce').fillna(context.df['environment']).astype('int64')
    # context.df['environmentWorldClass'] = pd.to_numeric(context.df['environmentWorldClass'],errors='coerce').fillna(context.df['environmentWorldClass']).astype('int64')




    context.result = context.df[['stoxxId','vendor','technologyName','technologyCode','totalPatentsInTechnology','worldClassPatentsInTechnology','specialisation']].groupby(['stoxxId','vendor']).apply(lambda x:x.drop(columns=['stoxxId']).astype(str).to_dict(orient ='records')).reset_index(name='technologydetails')
    # context.result['technologydetails'] = [[{key: value if value not in ('none','None','nan') else '' for key, value in d.items()} for d in sublist] for sublist in context.result['technologydetails']]
    context.result['technologydetails']=context.result['technologydetails'].apply(lambda x:str(pd.DataFrame(x).drop('vendor',axis=1).map(lambda y : '' if str(y).lower() in ['nan','none'] else y).fillna('').to_dict(orient='records')))
    context.result['technologydetails']=context.result['technologydetails'].str.replace("'",'"')
    dff = context.ECO_DB_Data.merge(context.result,on=['stoxxId','vendor'],how='left')
    dff.drop_duplicates(subset=['vendor','stoxxId'],inplace =True)
    w=dff[['vendor','stoxxId','owner','totalPatents','environment','environmentWorldClass','technologydetails']]
    df_pivoted = w.pivot_table(index='stoxxId', columns='vendor', values=['owner','totalPatents','environment','environmentWorldClass','technologydetails'],aggfunc='first')
    df_pivoted.columns = [f'{col[0]}@{col[1]}' for col in df_pivoted.columns]
    df_pivoted.reset_index(inplace=True)
    context.QA_df = df_pivoted.copy()
    if 'owner@EconsightMetaverse' not in context.QA_df.columns:
        context.QA_df['owner@EconsightMetaverse'] = ''

@then('validate the data')
def validate_data(context):
    # context.Api_Data_ECO = context.Api_Data_ECO.sort_values(by='internalId').reset_index(drop=True)
    # context.QA_df = context.QA_df.sort_values(by='stoxxId').reset_index(drop=True)
    context.Final_result = context.Api_Data_ECO.merge(context.QA_df,how='inner',left_on='internalId',right_on='stoxxId',suffixes=('_DEV','_QA'))
    context.QA_df.rename(columns={'stoxxId':'internalId'},inplace =True)
    # # context.QA_df['environment@EconsightMetaverse'] = context.QA_df['environment@EconsightMetaverse'].fillna('')
    # selected_columns = ['internalId','environment@EconsightMetaverse','environmentWorldClass@EconsightMetaverse','owner@EconsightMetaverse','technologydetails@EconsightMetaverse','totalPatents@EconsightMetaverse','environment@EconsightEnergyPatent','environmentWorldClass@EconsightEnergyPatent','owner@EconsightEnergyPatent','technologydetails@EconsightEnergyPatent','totalPatents@EconsightEnergyPatent','environment@EconsightLithiumBatteries','environmentWorldClass@EconsightLithiumBatteries','owner@EconsightLithiumBatteries','technologydetails@EconsightLithiumBatteries','totalPatents@EconsightLithiumBatteries']
    # for i in selected_columns:
    #     if (i.split("@")[0]!='technologydetails') and (i!='internalId'):
    #         context.Api_Data_ECO[i]= pd.to_numeric(context.Api_Data_ECO[i],errors ='coerce').fillna(context.Api_Data_ECO[i])
    #         context.QA_df[i] = context.QA_df[i].fillna('')


    # assert_frame_equal(context.Api_Data_ECO[(context.Api_Data_ECO['internalId'].isin(context.Final_result['internalId']))][selected_columns].sort_values(by='internalId').reset_index(drop=True),
    #                 context.QA_df[(context.QA_df['internalId'].isin(context.Final_result['internalId']))][selected_columns].sort_values(by='internalId').reset_index(drop=True),check_like=True,check_dtype=False)
    
    api_df=context.Api_Data_ECO[(context.Api_Data_ECO['internalId'].isin(context.Final_result['internalId']))]
    qa_df=context.QA_df[(context.Api_Data_ECO['internalId'].isin(context.Final_result['internalId']))]
    api_df['internalId'].sort_values()
    qa_df['internalId'].sort_values()
    context.QA_dff=context.Final_result
    context.QA_dff=context.Final_result[['internalId']+[i  for i in context.Final_result.columns if "_QA" in i]]
    context.Api_dff=context.Final_result[['internalId']+[i  for i in context.Final_result.columns if "_DEV" in i]]
    context.QA_dff.columns=[i.replace("_QA","") for i in context.QA_dff.columns]
    context.Api_dff.columns=[i.replace("_DEV","") for i in context.Api_dff.columns]
    # context.QA_dff['environment@EconsightEnergyPatent'] = pd.to_numeric(context.QA_dff['environment@EconsightEnergyPatent'],errors='coerce').fillna(context.QA_dff['environment@EconsightEnergyPatent'])
    # context.Api_dff['environment@EconsightEnergyPatent'] = pd.to_numeric(context.Api_dff['environment@EconsightEnergyPatent'],errors='coerce').fillna(context.Api_dff['environment@EconsightEnergyPatent'])
    # context.QA_dff['environment@EconsightEnergyPatent'] =context.QA_dff['environment@EconsightEnergyPatent'].fillna('')
    # context.Api_dff['environment@EconsightEnergyPatent'] =context.Api_dff['environment@EconsightEnergyPatent'].fillna('')
    # context.QA_dff['environment@EconsightLithiumBatteries'] =context.QA_dff['environment@EconsightLithiumBatteries'].fillna('')
    # context.Api_dff['environment@EconsightLithiumBatteries'] =context.Api_dff['environment@EconsightLithiumBatteries'].fillna('')
    # context.QA_dff['environment@EconsightLithiumBatteries'] = pd.to_numeric(context.QA_dff['environment@EconsightLithiumBatteries'],errors='coerce').fillna(context.QA_dff['environment@EconsightLithiumBatteries'])
    # context.Api_dff['environment@EconsightLithiumBatteries'] = pd.to_numeric(context.Api_dff['environment@EconsightLithiumBatteries'],errors='coerce').fillna(context.Api_dff['environment@EconsightLithiumBatteries'])
    # context.QA_dff['environment@EconsightMetaverse'] = pd.to_numeric(context.QA_dff['environment@EconsightMetaverse'],errors='coerce').fillna(context.QA_dff['environment@EconsightMetaverse'])
    # context.Api_dff['environment@EconsightMetaverse'] = pd.to_numeric(context.Api_dff['environment@EconsightMetaverse'],errors='coerce').fillna(context.Api_dff['environment@EconsightMetaverse'])
    # context.QA_dff['environment@EconsightMetaverse'] =context.QA_dff['environment@EconsightMetaverse'].fillna('')
    # context.Api_dff['environment@EconsightMetaverse'] =context.Api_dff['environment@EconsightMetaverse'].fillna('')
    # # context.QA_dff['environmentWorldClass@EconsightEnergyPatent'] =pd.to_numeric(context.QA_dff['environmentWorldClass@EconsightEnergyPatent']).fillna(context.QA_dff['environmentWorldClass@EconsightEnergyPatent'])
    # # context.Api_dff['environmentWorldClass@EconsightEnergyPatent'] =pd.to_numeric(context.Api_dff['environmentWorldClass@EconsightEnergyPatent']).fillna(context.Api_dff['environmentWorldClass@EconsightEnergyPatent'])
    # # context.QA_dff['environmentWorldClass@EconsightLithiumBatteries'] =context.QA_dff['environmentWorldClass@EconsightLithiumBatteries'].fillna('')
    # context.Api_dff['environmentWorldClass@EconsightLithiumBatteries'] =context.Api_dff['environmentWorldClass@EconsightLithiumBatteries'].fillna('')
    # context.QA_dff['environmentWorldClass@EconsightLithiumBatteries'] =pd.to_numeric(context.QA_dff['environmentWorldClass@EconsightLithiumBatteries']).fillna(context.QA_dff['environmentWorldClass@EconsightLithiumBatteries'])
    # context.Api_dff['environmentWorldClass@EconsightLithiumBatteries'] =pd.to_numeric(context.Api_dff['environmentWorldClass@EconsightLithiumBatteries']).fillna(context.Api_dff['environmentWorldClass@EconsightLithiumBatteries'])
    # context.QA_dff['environmentWorldClass@EconsightLithiumBatteries'] =context.QA_dff['environmentWorldClass@EconsightLithiumBatteries'].fillna('')
    # context.Api_dff['environmentWorldClass@EconsightLithiumBatteries'] =context.Api_dff['environmentWorldClass@EconsightLithiumBatteries'].fillna('')
    # context.QA_dff['environmentWorldClass@EconsightMetaverse'] =pd.to_numeric(context.QA_dff['environmentWorldClass@EconsightMetaverse']).fillna(context.QA_dff['environmentWorldClass@EconsightMetaverse'])
    # context.Api_dff['environmentWorldClass@EconsightMetaverse'] =pd.to_numeric(context.Api_dff['environmentWorldClass@EconsightMetaverse']).fillna(context.Api_dff['environmentWorldClass@EconsightMetaverse'])
    # context.QA_dff['environmentWorldClass@EconsightMetaverse'] =context.QA_dff['environmentWorldClass@EconsightMetaverse'].fillna('')
    # context.Api_dff['environmentWorldClass@EconsightMetaverse'] =context.Api_dff['environmentWorldClass@EconsightMetaverse'].fillna('')
    # context.QA_dff['owner@EconsightEnergyPatent'] =context.QA_dff['owner@EconsightEnergyPatent'].fillna('')
    # # context.Api_dff['owner@EconsightEnergyPatent'] =context.Api_dff['owner@EconsightEnergyPatent'].fillna('')
    # context.QA_dff['owner@EconsightLithiumBatteries'] =context.QA_dff['owner@EconsightLithiumBatteries'].fillna('')
    # context.Api_dff['owner@EconsightLithiumBatteries'] =context.Api_dff['owner@EconsightLithiumBatteries'].fillna('')
    # context.QA_dff['environmentWorldClass@EconsightEnergyPatent'] =context.QA_dff['environmentWorldClass@EconsightEnergyPatent'].fillna('')

    strip_col = [i for i in context.QA_dff if i.split("@")[0]=='technologydetails']
    for i in strip_col:
            context.Api_dff[i]= context.Api_dff[i].str.replace("'",'"')
            context.Api_dff[i]=context.Api_dff[i].fillna('').apply(lambda x:json.loads(x) if x else None)
            context.QA_dff[i]=context.QA_dff[i].fillna('').apply(lambda x:json.loads(x) if x else None)
            # m = context.Api_dff[i]
            # o = context.QA_dff[i]
            # set1 = {frozenset(d.items()) for d in m}
            # set2 = {frozenset(d.items()) for d in o}
            # assert set1 == set2
            
    a=pd.json_normalize(context.Api_dff.to_dict(orient='records'),record_path=strip_col[0],meta=['internalId'])
    q=pd.json_normalize(context.QA_dff.to_dict(orient='records'),record_path=strip_col[0],meta=['internalId'])
    a=a.sort_values(by=['internalId','technologyName']).reset_index(drop=True)
    q=q.sort_values(by=['internalId','technologyName']).reset_index(drop=True)
    a.specialisation=pd.to_numeric(a.specialisation,errors='coerce')
    q.specialisation=pd.to_numeric(q.specialisation,errors='coerce')
    a.totalPatentsInTechnology=pd.to_numeric(q.totalPatentsInTechnology,errors='coerce')
    q.totalPatentsInTechnology=pd.to_numeric(q.totalPatentsInTechnology,errors='coerce')


    assert_frame_equal(a,q,check_dtype=False)

    # assert_frame_equal(context.Api_dff,context.QA_dff,check_dtype=False,check_like=True)


