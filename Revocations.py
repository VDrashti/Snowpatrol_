from datetime import datetime, timedelta
from snowflake.snowpark import Session
import snowflake.connector
import plotly.graph_objects as go
from app_data_model import SnowpatrolDataModel
import json 
import streamlit as st
from streamlit_extras.colored_header import colored_header
from streamlit_extras.metric_cards import style_metric_cards
from streamlit_toggle import st_toggle_switch
from streamlit_option_menu import option_menu
from PIL import Image
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
#from sqlalchemy import URL
import pandas as pd
import base64



#################################
#
#   Utility functions
#
#################################
# def build_snowpark_session(kwargs) -> Session:
#     try:
#         res=Session.builder.configs({
#         "account": kwargs["account"],
#         "user": kwargs["username"],
#         "password": kwargs["password"],
#         "warehouse": kwargs.get("warehouse", ""),
#         "database": kwargs.get("database", ""),
#         "schema": kwargs.get("schema", ""),
#         "role": kwargs.get("role", "")
#             }).create() 
#     except:
#         st.error(":warning: Incorrect login credentials")
#         res = None
#     return res

# def connect_to_snowflake(**kwargs):
#     if 'SNOWPARK_SESSION' not in st.session_state:
#         if (kwargs["account"].strip() != "") & (kwargs["username"].strip() != "") & (kwargs["password"].strip() is not None):
#             SNOWPARK_SESSION=build_snowpark_session(kwargs)
#             st.session_state['SNOWPARK_SESSION']=SNOWPARK_SESSION
#             st.info(f":+1: Connected to {SNOWPARK_SESSION.get_current_account()} as your default role - {SNOWPARK_SESSION.get_current_role()}")
#         else:
#             st.error(":warning: Missing fields")

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


# #################################
# #
# #   UI Builder
# #
# #################################
# # st.set_page_config(
# #      page_title="Snowpatrol - License Optimization",
# #      page_icon=":rotating_light:",
# #      layout="wide",
# #      initial_sidebar_state="expanded"
# # )
# #st.header(":rotating_light: Snowpatrol - License Optimization")
# image = Image.open("SnowPatrol.png")
# with st.container() as mp:
#     st.markdown('<style>div.block-container{padding-bottom :0px; padding-right :10px; padding-top :30px;padding-left :10px; margin:10px; }</style>',unsafe_allow_html=True)
#     m1,m2=st.columns(2,gap="small")
#     with m1:
#         st.image(image,width=250 )
#     with m2:
#         st.markdown(
#         "<div style='text-align: right; font-size: 20px; font-family: Arial; color:rgb(0,0,100); font-weight: bold; '>Revocation Recommendations</div>",
#         unsafe_allow_html=True,
#         )
# st.markdown('<hr style="margin-top: 0px; margin-bottom: 0px;">', unsafe_allow_html=True)
# #st.markdown("<hr>",unsafe_allow_html=True)
# # st.divider()       
# # st.markdown('<style>div.block-container{padding-top:0rem;}</style>',unsafe_allow_html=True)
# def init_session() -> Session:
#     with st.form("snowflake_connection"):
#         colored_header(
#                 label="Connect to Snowflake's Snowpatrol data source",
#                 description="Provide your Snowflake credentials to login",
#                 color_name="red-70",
#         )
#         account = st.text_input('Snowflake Account Identifier* : ',value="cgbqnol-oz22455")
#         mandatory_fields_col1, mandatory_fields_col2 = st.columns(2, gap="small")        
#         with mandatory_fields_col1:
#             username = st.text_input('Username* : ',value="harika")
#         with mandatory_fields_col2:
#             password = st.text_input('Password* : ', type="password",value="Harika@445")
        
#         """__*must fill__"""
#         connect = st.form_submit_button("Connect")

#         if connect:
#             connect_to_snowflake(account=account , username=username , password=password)
#             session_sdm = SnowpatrolDataModel(st.session_state['SNOWPARK_SESSION'])
#             st.session_state['sdm']=session_sdm



def build_UI():
    image = Image.open("SnowPatrol.png")
    with st.container() as mp:
        st.markdown('<style>div.block-container{padding-bottom :0px; padding-right :10px; padding-top :30px;padding-left :10px; margin:10px; }</style>',unsafe_allow_html=True)
        m1,m2=st.columns(2,gap="small")
        with m1:
            st.image(image,width=250 )
        with m2:
            st.markdown(
            "<div style='text-align: right; font-size: 20px; font-family: Arial; color:rgb(0,0,100); font-weight: bold; '>Revocation Recommendations</div>",
            unsafe_allow_html=True,
            )
    st.markdown('<hr style="margin-top: 0px; margin-bottom: 0px;">', unsafe_allow_html=True)          
    if 'sdm' in st.session_state:
        session_sdm=st.session_state['sdm']
        available_roles=get_available_roles_for_user()
        session_sdm.role=selected_role = "ACCOUNTADMIN"
        session_sdm.db=selected_db = "SNOWPATROL"
        session_sdm.schema=selected_scm = "MAIN"
        session_sdm.wh=selected_wh = "COMPUTE_WH"
        active_licenses=session_sdm.get_active_licenses()
        with st.container() as execution_context:
            execution_context_col1,ee,execution_context_col2=st.columns([20,5,75])
            if not active_licenses.empty:
                with execution_context_col1:
                    app_list = active_licenses['APP_NAME'].unique()
                    app_name = st.selectbox(label=" # App Name", options=app_list)
                    
                    app_id = int(active_licenses[active_licenses.APP_NAME == app_name].reset_index().at[0,'APP_ID'])
                    if(app_id==1):
                        k=10 #Udemy
                    elif(app_id==2):
                        k=6 #Power BI
                    elif(app_id==3):
                        k=10 #DevOps
                    elif(app_id==4):
                        k=8 #Office 365
                        st.write("")
                    cutoff_days = st.text_input("Cutoff Day (0-365 Days)",60)
                    
                    
                    probability_no_login_revocation_threshold=st.text_input("Probability Threshold (0.00-0.1.00)",0.50)
                    
                    # probability_no_login_revocation_threshold=0.5
                    st.write("")
                    with st.expander("**Pull recommendations from an older run:**"):                            
                        runs_df=session_sdm.get_revocation_recommendations(app_id)
                        run_list = runs_df['RUN_ID'].unique().tolist() if not runs_df.empty else []
                        run_id= st.selectbox("Select a run id :- ", options=run_list)

                        get_older_recommendations = st.button("Get", disabled=True if runs_df.empty else False)
                    st.write("")
                    with st.expander("**Retrain & get fresh recommendations:**"):
                        include_dept = st.selectbox(
                            label="Select Department",
                            options=["All","Account","Operations", "Delivery","Management", "General","Innovations","Line Of Business","Sales & Marketing","Sales"],  # Replace with your department options
                            index=0  # Set the default selected department
                        )
                        # include_div = st.selectbox(
                        #     label="Select Division",
                        #     options=["none","hackathon", "project"," project"],  # Replace with your division options
                        #     index=0  # Set the default selected division
                        # )
                        include_div="none"
                        generate_new_recommendations = st.button("Generate")
                with ee:
                    st.markdown("<span style='display: inline-block;border-left: 1px solid #ccc;margin: 0 10px;height: 1000px; padding: 10px;'></span>",unsafe_allow_html=True)
                with execution_context_col2:
                    
                    if get_older_recommendations | generate_new_recommendations:

                                response={'status':''}

                                if get_older_recommendations:
                                    response = {"status": "SUCCESS"}
                                    recommendations_df = runs_df[runs_df.RUN_ID == run_id]
                                    run_date = recommendations_df['TRAINING_DATE'].unique()[0]
                                    probability_threshold = recommendations_df['THRESHOLD_PROBABILITY'].unique()[0]
                                    with st.container() as st1:
                                        if 'ERROR' in response['status']:
                                            st.error(f"**{response['status']}**", icon="☠️")
                                        else:
                                            _total_active = int(active_licenses[active_licenses.APP_ID == app_id]['ACTIVE_LICENSES'].sum())
                                            _revocable = int(recommendations_df[recommendations_df.REVOKE == 1]["SESSION_USER"].nunique())
                                            image_path = 'j.png'  # Replace with your image file's name
                                            with open(image_path, 'rb') as f:
                                                image_data = f.read()
                                            image_base64 = base64.b64encode(image_data).decode()
                                            image_pat = 's.png'
                                            with open(image_pat, 'rb') as f:
                                                image_ = f.read()
                                            image_base = base64.b64encode(image_).decode()
            
                                            with st.container() as metrics_section:
                                                image_path = 'j.png'  # Replace with your image file's name
                                                with open(image_path, 'rb') as f:
                                                    image_data = f.read()
                                                image_base64 = base64.b64encode(image_data).decode()
                                                image_pat = 's.png'
                                                with open(image_pat, 'rb') as f:
                                                    image_ = f.read()
                                                image_base = base64.b64encode(image_).decode()
                                                apps_tracked="$" + str(_revocable*k)
                    
                                                st.markdown(f'''<div style='width: 100%;'>
                                                            <div style='width: 48%; height: 100px; float: left; padding-left: 10px; border: 2px solid lightgrey; border-radius: 10px; padding-top:10px;'>
                                                            <div style='width:10%; height: 30px; float: left; padding-top: 20px;'><img src='data:image/png;base64,{image_base64}' style='max-width: 100%; height: auto;'>
                                                            </div>
                                                            <div style='width:90%; height: 30px; float: left; padding-top:10px;padding-left:10px'>
                                                            <b>Total Revocations</b><h3 style='padding-top: 0px;'>{_revocable}</h3>
                                                            </div> </div><div style='margin-left: 50%; height: 100px; border: 2px solid lightgrey; border-radius: 10px; padding-left:10px;padding-top:10px;'>
                                                            <div style='width:10%; height: 30px; float: left; padding-top: 20px;'><img src='data:image/png;base64,{image_base}' style='max-width: 100%; height: auto;'>
                                                            </div>
                                                            <div style='width:90%; height: 30px; float: left; padding-top:10px;padding-left:10px'>
                                                            <b>Potential Savings for {include_dept} Department</b><h3 style='padding-top: 0px;'>{apps_tracked}</h3>
                                                            </div>
                                                            </div>''',unsafe_allow_html=True)
                                            #     metrics_section_col1, metrics_section_col2 = st.columns(2, gap="small")
                                            #     with metrics_section_col1:
                                            #         al = _revocable
                                            #         st.metric(label="**Inctive Licenses**", value=al)
                                            #     with metrics_section_col2:
                                            #         apps_tracked="$" + str(_revocable*k)
                                            #         st.metric(label=f"Potential Savings for {include_dept} Department", value=apps_tracked)
                                            #     style_metric_cards()
                                            # recomm_results_col0_spacer1, recomm_results_c1, recomm_results_col0_spacer2 = st.columns((5, 25, 5))
                                            department_revocations_df = recommendations_df[recommendations_df['REVOKE'] == 1].groupby('DEPARTMENT').size().reset_index(name='NUMBER_OF_REVOCATIONS')
                                            # Create a bar chart for department-wise revocations
                                            fig_department_revocations = go.Figure(go.Bar(
                                                x=department_revocations_df['DEPARTMENT'],
                                                y=department_revocations_df['NUMBER_OF_REVOCATIONS'],
                                                marker=dict(color='royalblue')
                                            ))
                                            fig_department_revocations.update_layout(
                                                title={
                                                        'text': f"Department-Wise Revocations",
                                                        'font': {'color': 'red'}  # Change color to your desired color
                                                    },
                                                xaxis_title="Department",
                                                yaxis_title="Number of Revocations",
                                                xaxis=dict(tickangle=-45)
                                            )

                                            st.plotly_chart(fig_department_revocations, use_container_width=True, theme="streamlit")
                                            
                                            # col1, col2 = st.columns(2)
                                            # Calculate cost based on app_id
                                            if app_id == 1:
                                                cost = 10
                                            elif app_id == 2:
                                                cost = 6
                                            elif app_id == 3:
                                                cost = 10
                                            elif app_id == 4:
                                                cost = 8

                                            _revocable = int(recommendations_df[recommendations_df.REVOKE == 1]["SESSION_USER"].nunique())

                                            hi = cost * _revocable

                                            # Create the first pie chart in the first column
                                            with st.container() as l:
                                                # st.markdown('<style>div.block-container{ width: 48%; height: 600px; float: left; padding-left: 10px; border: 2px solid lightgrey; border-radius: 10px; padding-top:10px; }</style>',unsafe_allow_html=True) 
                                                
                                                if app_id == 1:
                                                    cost = 10
                                                elif app_id == 2:
                                                    cost = 6
                                                elif app_id == 3:
                                                    cost = 10
                                                elif app_id == 4:
                                                    cost = 8
                                                
                                                

                                                _revocable = int(recommendations_df[recommendations_df.REVOKE == 1]["SESSION_USER"].nunique())
                                                active = int(active_licenses[active_licenses.APP_ID == app_id]['ACTIVE_LICENSES'].sum())
                                                

                                                hi = cost * _revocable
                                                e=cost*active

                                                # Create the first pie chart in the first column
                                            
                                                fig_savings_1 = go.Figure(data=go.Pie(values=[hi,_revocable],
                                                                                    labels=['Active Licences', 'Inactive Licences'], hole=0.4))

                                                fig_savings_1.update_traces(hoverinfo='label+value',
                                                                            textinfo='percent', textfont_size=20,
                                                                            marker=dict(colors=['yellowgreen','silver']))

                                                fig_savings_1.add_annotation(x=0.5, y=0.47,
                                                                            text=f"{'$'}{hi}/{'App'}",
                                                                            font=dict(size=15, family='Verdana',
                                                                                    color='black'),
                                                                            showarrow=False)

                                                fig_savings_1.update_layout(title={
                                                    'text': f"Active vs. Inactive Licenses for {app_name}",
                                                    'font': {'size' : 20, 'color': 'red'},
                                                    'y': 0.9,
                                                    'x': 0.45,
                                                    'xanchor': 'center',
                                                    'yanchor': 'top'})

                                                st.plotly_chart(fig_savings_1, use_container_width=True, theme="streamlit")
                                elif generate_new_recommendations:
                                    run_date = datetime.now().date()
                                    probability_threshold = probability_no_login_revocation_threshold
                                    response = session_sdm.run_model_today(app_id=app_id
                                                        , cutoff_days=cutoff_days
                                                        ,probability_no_login_revocation_threshold=probability_no_login_revocation_threshold
                                                        ,include_dept=True,include_div=True,include_title=False
                                                        ,save_model=False)
                                    response = json.loads(response)
                                    recommendations_df = session_sdm.get_revocation_recommendations(app_id, response['run_id'])
                                    with st.container() as st1:
                                        if 'ERROR' in response['status']:
                                            st.error(f"**{response['status']}**", icon="☠️")
                                        else:
                                            if(include_div== "none"):
                                                if include_dept=="All_dept":
                                                    
                                                    # recomm_results_col0_spacer1, recomm_results_c1, recomm_results_col0_spacer2 = st.columns((5, 25, 5))
                                                    _total_active = int(active_licenses[active_licenses.APP_ID == app_id]['ACTIVE_LICENSES'].sum())
                                                    _revocable = int(recommendations_df[recommendations_df.REVOKE == 1]["SESSION_USER"].nunique())
                                                    with st.container() as metrics_section:
                                                        image_path = 'j.png'  # Replace with your image file's name
                                                        with open(image_path, 'rb') as f:
                                                            image_data = f.read()
                                                        image_base64 = base64.b64encode(image_data).decode()
                                                        image_pat = 's.png'
                                                        with open(image_pat, 'rb') as f:
                                                            image_ = f.read()
                                                        image_base = base64.b64encode(image_).decode()
                                                        apps_tracked="$" + str(_revocable*k)
                            
                                                        st.markdown(f'''<div style='width: 100%;'>
                                                                    <div style='width: 48%; height: 100px; float: left; padding-left: 10px; border: 2px solid lightgrey; border-radius: 10px; padding-top:10px;'>
                                                                    <div style='width:10%; height: 30px; float: left; padding-top: 20px;'><img src='data:image/png;base64,{image_base64}' style='max-width: 100%; height: auto;'>
                                                                    </div>
                                                                    <div style='width:90%; height: 30px; float: left; padding-top:10px;padding-left:10px'>
                                                                    <b>Total Revocations</b><h3 style='padding-top: 0px;'>{_revocable}</h3>
                                                                    </div> </div><div style='margin-left: 50%; height: 100px; border: 2px solid lightgrey; border-radius: 10px; padding-left:10px;padding-top:10px;'>
                                                                    <div style='width:10%; height: 30px; float: left; padding-top: 20px;'><img src='data:image/png;base64,{image_base}' style='max-width: 100%; height: auto;'>
                                                                    </div>
                                                                    <div style='width:90%; height: 30px; float: left; padding-top:10px;padding-left:10px'>
                                                                    <b>Potential Savings for {include_dept} Department</b><h3 style='padding-top: 0px;'>{apps_tracked}</h3>
                                                                    </div>
                                                                    </div>''',unsafe_allow_html=True)
                                                    fig_active_vs_revocable = go.Figure(data=go.Pie(values=[(_total_active - _revocable),_revocable],
                                                                                        labels=['ACTIVE', 'REVOCABLE'], hole=0.4)
                                                                                )
                                                    fig_active_vs_revocable.update_traces(hoverinfo='label+value',
                                                                                textinfo='percent', textfont_size=20, marker=dict(colors=['yellowgreen', 'silver'])
                                                                                )
                                                    fig_active_vs_revocable.add_annotation(x=0.5, y=0.47,
                                                                                text=f"{_revocable} / {_total_active}",
                                                                                font=dict(size=15, family='Verdana',
                                                                                            color='black'),
                                                                                showarrow=False)
                                                    
                                                    fig_active_vs_revocable.update_layout(title={
                                                                'text': "Active vs. Revocable Licenses",
                                                                'font' : {'size' : 20},
                                                                'y':0.9,
                                                                'x':0.45,
                                                                'xanchor': 'center',
                                                                'yanchor': 'top'})

                                                    st.plotly_chart(fig_active_vs_revocable, use_container_width=True, theme="streamlit")
                                                    
                                                if include_dept== "All":
                                                    _total_active = int(active_licenses[active_licenses.APP_ID == app_id]['ACTIVE_LICENSES'].sum())
                                                    _revocable = int(recommendations_df[recommendations_df.REVOKE == 1]["SESSION_USER"].nunique())
                                                    with st.container() as metrics_section:
                                                        image_path = 'j.png'  # Replace with your image file's name
                                                        with open(image_path, 'rb') as f:
                                                            image_data = f.read()
                                                        image_base64 = base64.b64encode(image_data).decode()
                                                        image_pat = 's.png'
                                                        with open(image_pat, 'rb') as f:
                                                            image_ = f.read()
                                                        image_base = base64.b64encode(image_).decode()
                                                        apps_tracked="$" + str(_revocable*k)
                            
                                                        st.markdown(f'''<div style='width: 100%;'>
                                                                    <div style='width: 48%; height: 100px; float: left; padding-left: 10px; border: 2px solid lightgrey; border-radius: 10px; padding-top:10px;'>
                                                                    <div style='width:10%; height: 30px; float: left; padding-top: 20px;'><img src='data:image/png;base64,{image_base64}' style='max-width: 100%; height: auto;'>
                                                                    </div>
                                                                    <div style='width:90%; height: 30px; float: left; padding-top:10px;padding-left:10px'>
                                                                    <b>Total Revocations</b><h3 style='padding-top: 0px;'>{_revocable}</h3>
                                                                    </div> </div><div style='margin-left: 50%; height: 100px; border: 2px solid lightgrey; border-radius: 10px; padding-left:10px;padding-top:10px;'>
                                                                    <div style='width:10%; height: 30px; float: left; padding-top: 20px;'><img src='data:image/png;base64,{image_base}' style='max-width: 100%; height: auto;'>
                                                                    </div>
                                                                    <div style='width:90%; height: 30px; float: left; padding-top:10px;padding-left:10px'>
                                                                    <b>Potential Savings for {include_dept} Department</b><h3 style='padding-top: 0px;'>{apps_tracked}</h3>
                                                                    </div>
                                                                    </div>''',unsafe_allow_html=True)
                                                    # recomm_results_col0_spacer1, recomm_results_c1, recomm_results_col0_spacer2 = st.columns((5, 25, 5))
                                                    department_revocations_df = recommendations_df[recommendations_df['REVOKE'] == 1].groupby('DEPARTMENT').size().reset_index(name='NUMBER_OF_REVOCATIONS')
                                                    # Create a bar chart for department-wise revocations
                                                    fig_department_revocations = go.Figure(go.Bar(
                                                        x=department_revocations_df['DEPARTMENT'],
                                                        y=department_revocations_df['NUMBER_OF_REVOCATIONS'],
                                                        marker=dict(color='royalblue')
                                                    ))
                                                    fig_department_revocations.update_layout(
                                                        title="Department-Wise Revocations",
                                                        xaxis_title="Department",
                                                        yaxis_title="Number of Revocations",
                                                        xaxis=dict(tickangle=-45)
                                                    )

                                                    st.plotly_chart(fig_department_revocations, use_container_width=True, theme="streamlit")

                                                if include_dept != "All" and include_dept != "All departments":
                                                    # recomm_results_col0_spacer1, recomm_results_c1, recomm_results_col0_spacer2 = st.columns((5, 25, 5))
                                                    department_df = recommendations_df[(recommendations_df['DEPARTMENT'] == include_dept) & (recommendations_df['REVOKE'] == 1)]
                                                    # Calculate the total number of revocations in 'Dept1'
                                                    total_revocations_dept = int(department_df[department_df['REVOKE'] == 1]["SESSION_USER"].nunique())
                                                    
                            

                                                    with st.container() as metrics_section:
                                                        image_path = 'j.png'  # Replace with your image file's name
                                                        with open(image_path, 'rb') as f:
                                                            image_data = f.read()
                                                        image_base64 = base64.b64encode(image_data).decode()
                                                        image_pat = 's.png'
                                                        with open(image_pat, 'rb') as f:
                                                            image_ = f.read()
                                                        image_base = base64.b64encode(image_).decode()
                                                        apps_tracked="$" + str(total_revocations_dept*k)
                            
                                                        st.markdown(f'''<div style='width: 100%;'>
                                                                    <div style='width: 48%; height: 100px; float: left; padding-left: 10px; border: 2px solid lightgrey; border-radius: 10px; padding-top:10px;'>
                                                                    <div style='width:10%; height: 30px; float: left; padding-top: 20px;'><img src='data:image/png;base64,{image_base64}' style='max-width: 100%; height: auto;'>
                                                                    </div>
                                                                    <div style='width:90%; height: 30px; float: left; padding-top:10px;padding-left:10px'>
                                                                    <b>Total Revocations</b><h3 style='padding-top: 0px;'>{total_revocations_dept}</h3>
                                                                    </div> </div><div style='margin-left: 50%; height: 100px; border: 2px solid lightgrey; border-radius: 10px; padding-left:10px;padding-top:10px;'>
                                                                    <div style='width:10%; height: 30px; float: left; padding-top: 20px;'><img src='data:image/png;base64,{image_base}' style='max-width: 100%; height: auto;'>
                                                                    </div>
                                                                    <div style='width:90%; height: 30px; float: left; padding-top:10px;padding-left:10px'>
                                                                    <b>Potential Savings for {include_dept} Department</b><h3 style='padding-top: 0px;'>{apps_tracked}</h3>
                                                                    </div>
                                                                    </div>''',unsafe_allow_html=True)
                                                    

                                                    # If there are any records for 'Dept1' with 'REVOKE' equal to 1
                                                    if not department_df.empty:
                                                        # Calculate the number of revocations per department and title
                                                        department_title_revocations_df = department_df.groupby(['DEPARTMENT', 'TITLE']).size().reset_index(name='NUMBER_OF_REVOCATIONS')

                                                        # Get a list of unique departments
                                                        unique_departments = department_title_revocations_df['DEPARTMENT'].unique()

                                                        # Create and display a bar chart for each department
                                                        for department in unique_departments:
                                                            department_data = department_title_revocations_df[department_title_revocations_df['DEPARTMENT'] == department]
                                                            fig_department_title_revocations = go.Figure()

                                                            # Add clustered bars for titles
                                                            fig_department_title_revocations.add_trace(go.Bar(
                                                                x=department_data['TITLE'],
                                                                y=department_data['NUMBER_OF_REVOCATIONS'],
                                                                name=department
                                                            ))

                                                            # Update the layout
                                                            fig_department_title_revocations.update_layout(
                                                                barmode='group',  # 'group' for clustered bars
                                                                title={
                                                                    'text': f"Revocations for Department: {department}",
                                                                    'font': {'color': 'red'}  # Change color to your desired color
                                                                },
                                                                xaxis_title="",
                                                                yaxis_title="Number of Revocations",
                                                                xaxis=dict(tickangle=-45),
                                                                width=1200
                                                                
                                                                  # Adjust the chart width as needed
                                                            )

                                                            st.plotly_chart(fig_department_title_revocations, use_container_width=True, theme="streamlit")
                                                    else:
                                                        st.write("No data available for this department with 'REVOKE' equal to 1.")
                                                
                                            else:
                                                # recomm_results_col0_spacer1, recomm_results_c1, recomm_results_col0_spacer2 = st.columns((5, 25, 5))
                                                recommendations_df1 = recommendations_df[recommendations_df['DIVISION'] == include_div]
                                                if include_dept=="All_dept":
                                                    
                                                    # st.success(f"**{response['status']}**", icon="👍")
                                                    
                                                
                                                    _total_active = int(active_licenses[active_licenses.APP_ID == app_id]['ACTIVE_LICENSES'].sum())
                                                    _revocable = int(recommendations_df1[recommendations_df1.REVOKE == 1]["SESSION_USER"].nunique())
                                                    with st.container() as metrics_section:
                                                        image_path = 'j.png'  # Replace with your image file's name
                                                        with open(image_path, 'rb') as f:
                                                            image_data = f.read()
                                                        image_base64 = base64.b64encode(image_data).decode()
                                                        image_pat = 's.png'
                                                        with open(image_pat, 'rb') as f:
                                                            image_ = f.read()
                                                        image_base = base64.b64encode(image_).decode()
                                                        apps_tracked="$" + str(_revocable*k)
                            
                                                        st.markdown(f'''<div style='width: 100%;'>
                                                                    <div style='width: 48%; height: 100px; float: left; padding-left: 10px; border: 2px solid lightgrey; border-radius: 10px; padding-top:10px;'>
                                                                    <div style='width:10%; height: 30px; float: left; padding-top: 20px;'><img src='data:image/png;base64,{image_base64}' style='max-width: 100%; height: auto;'>
                                                                    </div>
                                                                    <div style='width:90%; height: 30px; float: left; padding-top:10px;padding-left:10px'>
                                                                    <b>Total Revocations</b><h3 style='padding-top: 0px;'>{_revocable}</h3>
                                                                    </div> </div><div style='margin-left: 50%; height: 100px; border: 2px solid lightgrey; border-radius: 10px; padding-left:10px;padding-top:10px;'>
                                                                    <div style='width:10%; height: 30px; float: left; padding-top: 20px;'><img src='data:image/png;base64,{image_base}' style='max-width: 100%; height: auto;'>
                                                                    </div>
                                                                    <div style='width:90%; height: 30px; float: left; padding-top:10px;padding-left:10px'>
                                                                    <b>Potential Savings for {include_dept} Department</b><h3 style='padding-top: 0px;'>{apps_tracked}</h3>
                                                                    </div>
                                                                    </div>''',unsafe_allow_html=True)
                                                    fig_active_vs_revocable = go.Figure(data=go.Pie(values=[(_total_active - _revocable),_revocable],
                                                                                        labels=['ACTIVE', 'REVOCABLE'], hole=0.4)
                                                                                )
                                                    fig_active_vs_revocable.update_traces(hoverinfo='label+value',
                                                                                textinfo='percent', textfont_size=20, marker=dict(colors=['yellowgreen', 'silver'])
                                                                                )
                                                    fig_active_vs_revocable.add_annotation(x=0.5, y=0.47,
                                                                                text=f"{_revocable} / {_total_active}",
                                                                                font=dict(size=15, family='Verdana',
                                                                                            color='black'),
                                                                                showarrow=False)
                                                    
                                                    fig_active_vs_revocable.update_layout(title={
                                                                'text': "Active vs. Revocable Licenses",
                                                                'font' : {'size' : 20},
                                                                'y':0.9,
                                                                'x':0.45,
                                                                'xanchor': 'center',
                                                                'yanchor': 'top'})

                                                    st.plotly_chart(fig_active_vs_revocable, use_container_width=True, theme="streamlit")
                                                if include_dept== "All":
                                                    _total_active = int(active_licenses[active_licenses.APP_ID == app_id]['ACTIVE_LICENSES'].sum())
                                                    _revocable = int(recommendations_df1[recommendations_df1.REVOKE == 1]["SESSION_USER"].nunique())
                                                    with st.container() as metrics_section:
                                                        image_path = 'j.png'  # Replace with your image file's name
                                                        with open(image_path, 'rb') as f:
                                                            image_data = f.read()
                                                        image_base64 = base64.b64encode(image_data).decode()
                                                        image_pat = 's.png'
                                                        with open(image_pat, 'rb') as f:
                                                            image_ = f.read()
                                                        image_base = base64.b64encode(image_).decode()
                                                        apps_tracked="$" + str(_revocable*k)
                            
                                                        st.markdown(f'''<div style='width: 100%;'>
                                                                    <div style='width: 48%; height: 100px; float: left; padding-left: 10px; border: 2px solid lightgrey; border-radius: 10px; padding-top:10px;'>
                                                                    <div style='width:10%; height: 30px; float: left; padding-top: 20px;'><img src='data:image/png;base64,{image_base64}' style='max-width: 100%; height: auto;'>
                                                                    </div>
                                                                    <div style='width:90%; height: 30px; float: left; padding-top:10px;padding-left:10px'>
                                                                    <b>Total Revocations</b><h3 style='padding-top: 0px;'>{_revocable}</h3>
                                                                    </div> </div><div style='margin-left: 50%; height: 100px; border: 2px solid lightgrey; border-radius: 10px; padding-left:10px;padding-top:10px;'>
                                                                    <div style='width:10%; height: 30px; float: left; padding-top: 20px;'><img src='data:image/png;base64,{image_base}' style='max-width: 100%; height: auto;'>
                                                                    </div>
                                                                    <div style='width:90%; height: 30px; float: left; padding-top:10px;padding-left:10px'>
                                                                    <b>Potential Savings for {include_dept} Department</b><h3 style='padding-top: 0px;'>{apps_tracked}</h3>
                                                                    </div>
                                                                    </div>''',unsafe_allow_html=True)
                                                    #recomm_results_col0_spacer1, recomm_results_c1, recomm_results_col0_spacer2 = st.columns((5, 25, 5))
                                                    department_revocations_df = recommendations_df1[recommendations_df1['REVOKE'] == 1].groupby('DEPARTMENT').size().reset_index(name='NUMBER_OF_REVOCATIONS')
                                                    # Create a bar chart for department-wise revocations
                                                    fig_department_revocations = go.Figure(go.Bar(
                                                        x=department_revocations_df['DEPARTMENT'],
                                                        y=department_revocations_df['NUMBER_OF_REVOCATIONS'],
                                                        marker=dict(color='royalblue')
                                                    ))
                                                    fig_department_revocations.update_layout(
                                                        title="Department-Wise Revocations",
                                                        xaxis_title="Department",
                                                        yaxis_title="Number of Revocations",
                                                        xaxis=dict(tickangle=-45)
                                                    )

                                                    st.plotly_chart(fig_department_revocations, use_container_width=True, theme="streamlit")

                                                if include_dept != "All" and include_dept != "All departments":
                                                    # recomm_results_col0_spacer1, recomm_results_c1, recomm_results_col0_spacer2 = st.columns((5, 25, 5))
                                                    department_df = recommendations_df1[(recommendations_df1['DEPARTMENT'] == include_dept) & (recommendations_df1['REVOKE'] == 1)]
                                                    # Calculate the total number of revocations in 'Dept1'
                                                    total_revocations_dept = int(department_df[department_df['REVOKE'] == 1]["SESSION_USER"].nunique())
                                                    
                                                    with st.container() as metrics_section:
                                                        image_path = 'j.png'  # Replace with your image file's name
                                                        with open(image_path, 'rb') as f:
                                                            image_data = f.read()
                                                        image_base64 = base64.b64encode(image_data).decode()
                                                        image_pat = 's.png'
                                                        with open(image_pat, 'rb') as f:
                                                            image_ = f.read()
                                                        image_base = base64.b64encode(image_).decode()
                                                        apps_tracked="$" + str(total_revocations_dept*k)
                            
                                                        st.markdown(f'''<div style='width: 100%;'>
                                                                    <div style='width: 48%; height: 100px; float: left; padding-left: 10px; border: 2px solid lightgrey; border-radius: 10px; padding-top:10px;'>
                                                                    <div style='width:10%; height: 30px; float: left; padding-top: 20px;'><img src='data:image/png;base64,{image_base64}' style='max-width: 100%; height: auto;'>
                                                                    </div>
                                                                    <div style='width:90%; height: 30px; float: left; padding-top:10px;padding-left:10px'>
                                                                    <b>Total Revocations</b><h3 style='padding-top: 0px;'>{total_revocations_dept}</h3>
                                                                    </div> </div><div style='margin-left: 50%; height: 100px; border: 2px solid lightgrey; border-radius: 10px; padding-left:10px;padding-top:10px;'>
                                                                    <div style='width:10%; height: 30px; float: left; padding-top: 20px;'><img src='data:image/png;base64,{image_base}' style='max-width: 100%; height: auto;'>
                                                                    </div>
                                                                    <div style='width:90%; height: 30px; float: left; padding-top:10px;padding-left:10px'>
                                                                    <b>Potential Savings for {include_dept} Department</b><h3 style='padding-top: 0px;'>{apps_tracked}</h3>
                                                                    </div>
                                                                    </div>''',unsafe_allow_html=True)
                                                    

                                                    # If there are any records for 'Dept1' with 'REVOKE' equal to 1
                                                    if not department_df.empty:
                                                        # Calculate the number of revocations per department and title
                                                        department_title_revocations_df = department_df.groupby(['DEPARTMENT', 'TITLE']).size().reset_index(name='NUMBER_OF_REVOCATIONS')

                                                        # Get a list of unique departments
                                                        unique_departments = department_title_revocations_df['DEPARTMENT'].unique()

                                                        # Create and display a bar chart for each department
                                                        for department in unique_departments:
                                                            department_data = department_title_revocations_df[department_title_revocations_df['DEPARTMENT'] == department]
                                                            fig_department_title_revocations = go.Figure()

                                                            # Add clustered bars for titles
                                                            fig_department_title_revocations.add_trace(go.Bar(
                                                                x=department_data['TITLE'],
                                                                y=department_data['NUMBER_OF_REVOCATIONS'],
                                                                name=department
                                                            ))

                                                            # Update the layout
                                                            fig_department_title_revocations.update_layout(
                                                                barmode='group',  # 'group' for clustered bars
                                                                title={
                                                                    'text': f"Revocations for Department: {department}",
                                                                    'font': {'color': 'red'}  # Change color to your desired color
                                                                },
                                                                xaxis_title="",
                                                                yaxis_title="Number of Revocations",
                                                                xaxis=dict(tickangle=-45),
                                                                width=1200  # Adjust the chart width as needed
                                                            )

                                                            st.plotly_chart(fig_department_title_revocations, use_container_width=True, theme="streamlit")
                                                    else:
                                                        st.write("No data available for this department with 'REVOKE' equal to 1.")



                                            # with recomm_results_col0_spacer2:
                                            #     st.markdown("""""")
                                            # col1, col2 = st.columns(2)
                                            # # Calculate cost based on app_id
                                            if include_dept=="All" and include_div=="none":
                                        
                                                if app_id == 1:
                                                    cost = 10
                                                elif app_id == 2:
                                                    cost = 6
                                                elif app_id == 3:
                                                    cost = 10
                                                elif app_id == 4:
                                                    cost = 8
                                                
                                                

                                                _revocable = int(recommendations_df[recommendations_df.REVOKE == 1]["SESSION_USER"].nunique())
                                                # active = int(active_licenses[active_licenses.APP_ID == app_id]['ACTIVE_LICENSES'].sum())
                                                # active1=active-_revocable
                                                

                                                hi = cost * _revocable
                                                # e=cost*active

                                                # Create the first pie chart in the first column
                                            
                                                fig_savings_1 = go.Figure(data=go.Pie(values=[hi, _revocable],
                                                                                    labels=['Active Licences', 'Inactive Licences'], hole=0.4))

                                                fig_savings_1.update_traces(hoverinfo='label+value',
                                                                            textinfo='percent', textfont_size=20,
                                                                            marker=dict(colors=['yellowgreen','silver']))

                                                fig_savings_1.add_annotation(x=0.5, y=0.47,
                                                                            text=f"{'$'}{hi}/{app_name}",
                                                                            font=dict(size=15, family='Verdana',
                                                                                    color='black'),
                                                                            showarrow=False)

                                                fig_savings_1.update_layout(title={
                                                    'text': f"Active vs. Inactive Licenses for {app_name}",
                                                    'font': {'size' : 20, 'color': 'red'},
                                                    'y': 0.9,
                                                    'x': 0.45,
                                                    'xanchor': 'center',
                                                    'yanchor': 'top'})

                                                st.plotly_chart(fig_savings_1, use_container_width=True, theme="streamlit")
                                            elif(include_dept=="All" and include_div!="none"):

                                            
                                                if app_id == 1:
                                                    cost = 10
                                                elif app_id == 2:
                                                    cost = 6
                                                elif app_id == 3:
                                                    cost = 10
                                                elif app_id == 4:
                                                    cost = 8
                                                
                                                

                                                _revocable = int(recommendations_df[recommendations_df.REVOKE == 1][recommendations_df.DIVISION == include_div]["SESSION_USER"].nunique())
                                                active = int(active_licenses[active_licenses.APP_ID == app_id][active_licenses.DIVISION == include_div]['ACTIVE_LICENSES'].sum())
                                                active1=active-_revocable
                                                

                                                hi = cost * _revocable
                                                e=cost*active

                                                # Create the first pie chart in the first column
                                            
                                                fig_savings_1 = go.Figure(data=go.Pie(values=[hi,_revocable],
                                                                                    labels=['Active Licences', 'Inactive Licences'], hole=0.4))

                                                fig_savings_1.update_traces(hoverinfo='label+value',
                                                                            textinfo='percent', textfont_size=20,
                                                                            marker=dict(colors=['yellowgreen','silver']))

                                                fig_savings_1.add_annotation(x=0.5, y=0.47,
                                                                            text=f"{'$'}{hi}/{'App'}",
                                                                            font=dict(size=15, family='Verdana',
                                                                                    color='black'),
                                                                            showarrow=False)

                                                fig_savings_1.update_layout(title={
                                                    'text': f"Active vs. Inactive Licenses for {app_name}",
                                                    'font': {'size' : 20, 'color': 'red'},
                                                    'y': 0.9,
                                                    'x': 0.45,
                                                    'xanchor': 'center',
                                                    'yanchor': 'top'})

                                                st.plotly_chart(fig_savings_1, use_container_width=True, theme="streamlit")
                                            elif(include_dept!="All" and include_div=="none"):
                                                if app_id == 1:
                                                    cost = 10
                                                elif app_id == 2:
                                                    cost = 6
                                                elif app_id == 3:
                                                    cost = 10
                                                elif app_id == 4:
                                                    cost = 8
                                                
                                                

                                                _revocable = int(recommendations_df[recommendations_df.REVOKE == 1][recommendations_df.DEPARTMENT==include_dept]["SESSION_USER"].nunique())
                                                active = int(active_licenses[active_licenses.APP_ID == app_id][active_licenses.DEPARTMENT==include_dept]['ACTIVE_LICENSES'].sum())
                                                active1=active-_revocable
                                                

                                                hi = cost * _revocable
                                                e=cost*active

                                                # Create the first pie chart in the first column
                                            
                                                fig_savings_1 = go.Figure(data=go.Pie(values=[hi,_revocable],
                                                                                    labels=['Active Licences', 'Inactive Licences'], hole=0.4))

                                                fig_savings_1.update_traces(hoverinfo='label+value',
                                                                            textinfo='percent', textfont_size=20,
                                                                            marker=dict(colors=['yellowgreen','silver']))

                                                fig_savings_1.add_annotation(x=0.5, y=0.47,
                                                                            text=f"{'$'}{hi}/{'App'}",
                                                                            font=dict(size=15, family='Verdana',
                                                                                    color='black'),
                                                                            showarrow=False)

                                                fig_savings_1.update_layout(title={
                                                    'text': f"Active vs. Inactive Licenses for {app_name}",
                                                    'font': {'size' : 20, 'color': 'red'},
                                                    'y': 0.9,
                                                    'x': 0.45,
                                                    'xanchor': 'center',
                                                    'yanchor': 'top'})

                                                st.plotly_chart(fig_savings_1, use_container_width=True, theme="streamlit")
                                            elif(include_div!="none" and include_dept!="All"):
                                                if app_id == 1:
                                                    cost = 10
                                                elif app_id == 2:
                                                    cost = 6
                                                elif app_id == 3:
                                                    cost = 10
                                                elif app_id == 4:
                                                    cost = 8
                                                
                                                

                                                _revocable = int(recommendations_df[recommendations_df.REVOKE == 1][recommendations_df.DEPARTMENT==include_dept][recommendations_df.DIVISION == include_div]["SESSION_USER"].nunique())
                                                active = int(active_licenses[active_licenses.APP_ID == app_id][active_licenses.DEPARTMENT==include_dept][active_licenses.DIVISION == include_div]['ACTIVE_LICENSES'].sum())
                                                active1=active-_revocable
                                                

                                                hi = cost * _revocable
                                                e=cost*active

                                                # Create the first pie chart in the first column
                                            
                                                fig_savings_1 = go.Figure(data=go.Pie(values=[hi,_revocable],
                                                                                    labels=['Active Licences', 'Inactive Licences'], hole=0.4))

                                                fig_savings_1.update_traces(hoverinfo='label+value',
                                                                            textinfo='percent', textfont_size=20,
                                                                            marker=dict(colors=['yellowgreen','silver']))

                                                fig_savings_1.add_annotation(x=0.5, y=0.47,
                                                                            text=f"{'$'}{hi}/{'App'}",
                                                                            font=dict(size=15, family='Verdana',
                                                                                    color='black'),
                                                                            showarrow=False)

                                                fig_savings_1.update_layout(title={
                                                    'text': f"Active vs. Inactive Licenses for {app_name}",
                                                    'font': {'size' : 20, 'color': 'red'},
                                                    'y': 0.9,
                                                    'x': 0.45,
                                                    'xanchor': 'center',
                                                    'yanchor': 'top'})

                                                st.plotly_chart(fig_savings_1, use_container_width=True, theme="streamlit")


                                            # Create the second pie chart in the second column
                                            # with col2:
                                            #     # Create the second pie chart here using the same approach as above
                                            #     _total_active = int(active_licenses[active_licenses.APP_ID == app_id]['ACTIVE_LICENSES'].sum())
                                            #     _revocable = int(recommendations_df[recommendations_df.REVOKE == 1]["SESSION_USER"].nunique())
                                            #     fig_active_vs_revocable = go.Figure(data=go.Pie(values=[(_total_active - _revocable),_revocable],
                                            #                                         labels=['ACTIVE', 'REVOCABLE'], hole=0.4)
                                            #                                 )
                                            #     fig_active_vs_revocable.update_traces(hoverinfo='label+value',
                                            #                                 textinfo='percent', textfont_size=20, marker=dict(colors=['yellowgreen','silver'])
                                            #                                 )
                                            #     fig_active_vs_revocable.add_annotation(x=0.5, y=0.47,
                                            #                                 text=f"{_revocable} / {_total_active}",
                                            #                                 font=dict(size=15, family='Verdana',
                                            #                                             color='black'),
                                            #                                 showarrow=False)
                                                
                                            #     fig_active_vs_revocable.update_layout(title={
                                            #                 'text': "Active vs. Revocable Licenses",
                                            #                 'font' : {'size' : 20, 'color': 'red'},
                                            #                 'y':0.9,
                                            #                 'x':0.45,
                                            #                 'xanchor': 'center',
                                            #                 'yanchor': 'top'})

                                            #     st.plotly_chart(fig_active_vs_revocable, use_container_width=True, theme="streamlit")
                

                                    


   
