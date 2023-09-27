
from datetime import datetime, timedelta
from snowflake.snowpark import Session
import snowflake.connector
import plotly.graph_objects as go
from app_data_model import SnowpatrolDataModel
import json 
import streamlit as st
from streamlit_extras.colored_header import colored_header
from streamlit_extras.metric_cards import style_metric_cards
# from streamlit_toggle import st_toggle_switch
from streamlit_option_menu import option_menu
from streamlit_extras.stylable_container import stylable_container
from PIL import Image
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
#from sqlalchemy import URL
import pandas as pd
import Revocations
import Export_Data
import Summary
from st_clickable_images import clickable_images




def build_snowpark_session(kwargs) -> Session:
    try:
        res=Session.builder.configs({
        "account": kwargs["account"],
        "user": kwargs["username"],
        "password": kwargs["password"],
        "warehouse": kwargs.get("warehouse", ""),
        "database": kwargs.get("database", ""),
        "schema": kwargs.get("schema", ""),
        "role": kwargs.get("role", "")
            }).create() 
    except:
        st.error(":warning: Incorrect login credentials")
        res = None
    return res

def connect_to_snowflake(**kwargs):
    if 'SNOWPARK_SESSION' not in st.session_state:
        if (kwargs["account"].strip() != "") & (kwargs["username"].strip() != "") & (kwargs["password"].strip() is not None):
            SNOWPARK_SESSION=build_snowpark_session(kwargs)
            st.session_state['SNOWPARK_SESSION']=SNOWPARK_SESSION
            st.info(f":+1: Connected to {SNOWPARK_SESSION.get_current_account()} as your default role - {SNOWPARK_SESSION.get_current_role()}")
        else:
            st.error(":warning: Missing fields")

@st.cache_data
def get_available_roles_for_user():
    return st.session_state['sdm'].get_available_roles()

@st.cache_data
def get_available_databases(role):
    return st.session_state['sdm'].get_available_databases(role)

@st.cache_data
def get_available_schemas(role, db):
    return st.session_state['sdm'].get_available_schemas(role, db)

@st.cache_data
def get_available_warehouses(role):
    return st.session_state['sdm'].get_available_warehouses(role)


#################################
#
#   UI Builder
#
#################################
# st.set_page_config(
#      page_title="Snowpatrol - License Optimization",
#      page_icon=":rotating_light:",
#      layout="wide",
#      initial_sidebar_state="expanded"
# )
#st.header(":rotating_light: Snowpatrol - License Optimization")
#     # st.write("")
#     st.header(":blue[OVERVIEW]")

# colored_header(
#                         label='',
#                         description="",
#                         color_name="blue-70",
#                         )

def init_session() -> Session:
    with st.form("snowflake_connection"):
        colored_header(
                label="Connect to Snowflake's Snowpatrol data source",
                description="Provide your Snowflake credentials to login",
                color_name="red-70",
        )
        account = st.text_input('Snowflake Account Identifier* : ')
        mandatory_fields_col1, mandatory_fields_col2 = st.columns(2, gap="small")        
        with mandatory_fields_col1:
            username = st.text_input('Username* : ')
        with mandatory_fields_col2:
            password = st.text_input('Password* : ', type="password")
        
        """__*must fill__"""
        connect = st.form_submit_button("Connect")

        if connect:
            connect_to_snowflake(account=account , username=username , password=password)
            session_sdm = SnowpatrolDataModel(st.session_state['SNOWPARK_SESSION'])
            st.session_state['sdm']=session_sdm



def build_UI():
    image = Image.open("SnowPatrol.png")
    with st.container() as mp:
        st.markdown('<style>div.block-container{padding-bottom :0px; padding-right :10px; padding-top :30px;padding-left :10px; margin:10px; }</style>',unsafe_allow_html=True)
        m1,m2=st.columns(2,gap="small")
        with m1:
            st.image(image,width=250 )
        with m2:
            st.markdown(
            "<div style='text-align: right; font-size: 20px; font-family: Arial; color:rgb(0,0,100); font-weight: bold; '>Overview</div>",
            unsafe_allow_html=True,
            )
    st.markdown('<hr style="margin-top: 0px; margin-bottom: 0px;">', unsafe_allow_html=True)            
    if 'sdm' in st.session_state:
        session_sdm=st.session_state['sdm']
        available_roles=get_available_roles_for_user()

        with st.container() as execution_context:
            execution_context_col1,execution_context_col2, execution_context_col3, execution_context_col4 = st.columns(4, gap="small")
            selected_role = selected_db = selected_scm = selected_wh = ""
            
            with execution_context_col1:
                if available_roles:
                    selected_role = st.selectbox("Role: ", options=available_roles)
                    if selected_role != session_sdm.role:
                        session_sdm.role = selected_role 
            with execution_context_col2:
                available_databases=get_available_databases(session_sdm.role)       
                if available_databases:
                    selected_db = st.selectbox('Database : ', options=available_databases)
                    if selected_db != session_sdm.db:
                        session_sdm.db = selected_db
            with execution_context_col3:
                available_schemas=get_available_schemas(session_sdm.role, session_sdm.db)
                if available_schemas:
                    selected_scm = st.selectbox('Schema : ', options=available_schemas)
                    if selected_scm != session_sdm.schema:
                        session_sdm.schema = selected_scm
            with execution_context_col4:
                available_warehouses=get_available_warehouses(session_sdm.role)
                if available_warehouses:
                    selected_wh = st.selectbox('Warehouse : ', options=available_warehouses)
                    if selected_wh != session_sdm.wh:
                        session_sdm.wh = selected_wh
                
        active_licenses=session_sdm.get_active_licenses()

        with st.container() as login_history_section:
            
            log_col_1,log_col_2=st.columns((1,4))
            with log_col_1:
                    
                st.write("")
                st.write("")

                colored_header(
                    label="Login history",
                    description="",
                    color_name="blue-70",
                    )

                
                st.write("")
                al = 0 if active_licenses.empty else active_licenses['ACTIVE_LICENSES'].sum()
                
                #log = Image.open("Track.png")

                st.metric(label="**🔐 Total Licenses**", value=al)
                
                st.write("")
                apps_tracked= 0 if active_licenses.empty else active_licenses['APP_NAME'].nunique()
                st.metric(label="**📈 Applications Tracked**", value=apps_tracked)
                style_metric_cards()
                
                
                        
            with log_col_2:
                with st.container() as charts_section:
                    if not active_licenses.empty:
                        # """**License usage by Department**"""
                        _z = active_licenses.groupby(['APP_NAME', 'DEPARTMENT'])['ACTIVE_LICENSES'].sum().reset_index()
                        temp = _z.pivot(index='APP_NAME', columns='DEPARTMENT')['ACTIVE_LICENSES'].fillna(0)
                        fig_active_license_heatgrid = go.Figure(data=go.Heatmap(
                                                    z=temp.values,
                                                    x=temp.columns,
                                                    y=temp.index,
                                                    hoverongaps = True
                                                    ))
                        fig_active_license_heatgrid = fig_active_license_heatgrid.update_traces(
                            text=temp.values, texttemplate="%{text}",
                        )
                        fig_active_license_heatgrid.update_layout(title={
                                                'text': "License usage by Department",
                                                'font' : {'size' : 26},
                                                'y':0.9,
                                                'x':0.5,
                                                'xanchor': 'center',
                                                'yanchor': 'top'})
                                                
                        st.plotly_chart(fig_active_license_heatgrid, use_container_width=True, theme="streamlit")
                            
            
if __name__ == '__main__':
    if 'SNOWPARK_SESSION' not in st.session_state:
        init_session()
    e1,e,e2=st.columns([5,2,93])
    with e1:
        
        st.write("")
        st.write("")
        st.write("")
        st.write("")
        clicked = clickable_images(
        [
            "Overview-selected.png",
            "Reco-selected.png",
            "Export-selected.png"
                
        ],
        titles=["Overview","Summary","Revocations","Export_Data"],
        img_style={"margin": "15px", "height": "50px"},
        )
        
    with e:
        st.markdown("<span style='display: inline-block;border-left: 1px solid #ccc;margin: 0 10px;height: 1000px; padding: 10px;'></span>",unsafe_allow_html=True)
    with e2:
        if clicked==0:
            build_UI()
        elif clicked==1:
            Revocations.build_UI()
        elif clicked==2:
            Export_Data.build_UI()
        else:
            build_UI()
    # selected=option_menu(

    # menu_title=None,

    # options=['Overview','Revocations','Export_Data'],

    # #options=['',''],

    # icons=['columns-gap','file-earmark-text'],

    # default_index=0,
    # orientation="horizontal")
    # if selected=='Overview':
    #     build_UI()
    # elif selected=='Revocations':
    #     Revocations.build_UI()
    # elif selected=='Export_Data':
    #     Export_Data.build_UI()
