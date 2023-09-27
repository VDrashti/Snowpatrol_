from snowflake.snowpark.session import Session
import snowflake.snowpark.functions as F
from datetime import datetime, timedelta
import sys
import os
import pandas as pd
import streamlit as st

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from python.snowpatrol import constants

class SnowpatrolDataModel:
    st.markdown('<style>div.block-container{padding-top:2rem;}</style>',unsafe_allow_html=True) 
    def __init__(self, session: Session) -> None:
            self._session = session
            self._role = ""
            self._db = ""
            self._schema = "" 
            self._wh = ""

    def get_available_roles(self):
       try:
        rows=self._session.sql("show roles").collect()
        res=[row['name'] for row in rows]
       except:
           res=[]
       return res
            
    def get_available_databases(self, role:str):
        try:
            self._session.use_role(role)
            rows=self._session.sql("show databases").collect()
            res=[row['name'] for row in rows]
        except:
            res=[]

        return res

    def get_available_schemas(self, role, database):
        try:
            self._session.use_role(role)
            self._session.use_database(database)
            rows=self._session.sql("show schemas").collect()
            res = [row['name'] for row in rows]
        except:
            res=[]

        return res

    def get_available_warehouses(self,role):
        try:
            self._session.use_role(role)
            rows=self._session.sql("show warehouses").collect()
            res = [row['name'] for row in rows]
        except:
            res=[]

        return res  
       
    @property
    def role(self):
        return self._role
    
    @role.setter
    def role(self, val):
        self._role = val
        if len(val) > 0: 
            self._session.use_role(self._role)

    @property
    def db(self):
        return self._db

    @db.setter
    def db(self, val):
        self._db = val
        if len(val) > 0:
            self._session.use_database(self._db)

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, val):
        self._schema = val
        if len(val) > 0:
            self._session.use_schema(self._schema)

    @property
    def wh(self):
        return self._wh

    @wh.setter
    def wh(self, val):
        self._wh = val
        if len(val) > 0:
            self._session.use_warehouse(self._wh)

    def get_active_licenses(self) -> pd.DataFrame:
        try:
            active_licenses = self._session.sql(f""" 
                                    with 
                                        combined_logs as (
                                        select app_id, session_user from {constants.TBL_APP_LOGS}
                                        union 
                                        select app_id, session_user from {constants.TBL_OKTA_USERS}
                                        ),
                                        logs_with_metadata as (
                                            select ma.app_name, cl.app_id, cl.session_user, em.title, em.department, em.division
                                            from {constants.TBL_EMPLOYEE_METADATA} em 
                                            join combined_logs cl on (em.session_user = cl.session_user)
                                            join {constants.TBL_MONITORED_APPS} ma on (cl.app_id = ma.app_id)
                                        )
                                    select app_name, app_id, division, department, title, count(distinct session_user) as active_licenses from logs_with_metadata group by all
                                """).to_pandas()
        except:
            active_licenses = pd.DataFrame()

        return active_licenses
        
    def run_model_today(self, **kwargs) -> dict:
        response = self._session.call("run_model_today"
                        ,kwargs["app_id"]
                        ,kwargs["cutoff_days"]
                        ,kwargs["probability_no_login_revocation_threshold"]
                        ,kwargs['include_dept'],kwargs['include_div'],kwargs['include_title'],
                        kwargs['save_model']
                    )

        return response

    def get_revocation_recommendations(self, app_id:int, run_id:str = None) -> pd.DataFrame:
        try:
            result = self._session.table(f"{constants.TBL_LICENSE_REVOCATION_RECOMMENDATION}")\
            .filter((F.col("app_id") == F.lit(app_id)) & F.iff(F.lit(run_id).isNotNull(), F.col("run_id") == F.lit(run_id), F.lit(True)))\
            .select("*")\
            .to_pandas()
        except:
            result = pd.DataFrame()
        return result
    
    

