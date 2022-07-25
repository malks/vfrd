#!/usr/bin/python3
# -*- coding: latin-1 -*-
##########DEPENDENCIAS##############################
#  apt-get install python3-pip
#  python3 -m pip install mysql-connector-python
#  apt-get install python3-demjson
####################################################

import requests
from mysql_connection import run_select,run_sql,run_select_array_ret,new_conn
import re
import json
#DATE_ADD(NOW(), INTERVAL 86400 SECOND)

url="https://api.rd.services/platform/conversions?api_key="
headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
}

def string_and_strip(what):
    if what==None:
        return ''
    return re.sub("'|\"|-|`","",str(what))

def is_access_token_valid():
    check=run_select("SELECT if(expires_in>=now(),1,0) as `check` FROM lepard_magento.vfrd_apidata where id=1",main_conn)
    ret=False
    if check["check"]==1:
        ret=True
    return ret


if __name__ == "__main__":
    main_conn=new_conn()
    api_key=run_select("SELECT api_key FROM lepard_magento.vfrd_apidata where id=1",main_conn)
    
    if len(api_key)<=0:
        quit()

    api_key=api_key[0]["api_key"]
    url+=api_key
    
    contacts=run_select("SELECT email,concat(name,' ',lastname) as `name`,phone as `mobile_phone`,city,ifnull(if(length(state)=2,state,te.uf),'') as state,dob as `cf_data_nascimento` FROM lepard_magento.vfrd_sync LEFT JOIN lepard_magento.tb_estados te ON te.nome=state WHERE status=0",main_conn)

    payload={}
    payload["event_type"]="CONVERSION"
    payload["event_family"]="CDP"

    for c in contacts:
        payload["payload"]={string_and_strip(key): c[key] for key in c}
        payload["payload"]["available_for_mailing"]=True
        payload["payload"]["conversion_identifier"]="cadastro de leads"
        payload["payload"]["legal_bases"]=[]
        payload["payload"]["legal_bases"].append({})
        payload["payload"]["legal_bases"][0]["category"]="communications"
        payload["payload"]["legal_bases"][0]["type"]="consent"
        payload["payload"]["legal_bases"][0]["status"]="granted"

        jpayload=json.dumps(payload,ensure_ascii=False)
        response = requests.request("POST", url, json=payload, headers=headers)
        run_sql("UPDATE lepard_magento.vfrd_sync SET status=1 WHERE email='"+c["email"]+"'",main_conn)