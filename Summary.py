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
                    cutoff_days = st.text_input("Cutoff Day (0-365 Days)",60)
                    
                    
                    probability_no_login_revocation_threshold=st.text_input("Probability Threshold (0.00-0.1.00)",0.50)
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
                    recommendations_df = pd.DataFrame()
                    
                    app_id=1
                    response = session_sdm.run_model_today(app_id=app_id
                                                        , cutoff_days=cutoff_days
                                                        ,probability_no_login_revocation_threshold=probability_no_login_revocation_threshold
                                                        ,include_dept=True,include_div=True,include_title=False
                                                        ,save_model=False)
                    response = json.loads(response)
                    recommendations_df1 =session_sdm.get_revocation_recommendations(app_id, response['run_id'])
                    recommendations_df1["cost"]=10
                    
                    app_id=2
                    response = session_sdm.run_model_today(app_id=app_id
                                                        , cutoff_days=cutoff_days
                                                        ,probability_no_login_revocation_threshold=probability_no_login_revocation_threshold
                                                        ,include_dept=True,include_div=True,include_title=False
                                                        ,save_model=False)
                    response = json.loads(response)
        
                    recommendations_df2 =session_sdm.get_revocation_recommendations(app_id, response['run_id'])
                    recommendations_df2["cost"]=6
                    
                    app_id=3
                    response = session_sdm.run_model_today(app_id=app_id
                                                        , cutoff_days=cutoff_days
                                                        ,probability_no_login_revocation_threshold=probability_no_login_revocation_threshold
                                                        ,include_dept=True,include_div=True,include_title=False
                                                        ,save_model=False)
                    response = json.loads(response)
                    recommendations_df3 =session_sdm.get_revocation_recommendations(app_id, response['run_id'])

                    recommendations_df3["cost"]=10
                    app_id=4
                    response = session_sdm.run_model_today(app_id=app_id
                                                        , cutoff_days=cutoff_days
                                                        ,probability_no_login_revocation_threshold=probability_no_login_revocation_threshold
                                                        ,include_dept=True,include_div=True,include_title=False
                                                        ,save_model=False)
                    response = json.loads(response)
                    recommendations_df4 =session_sdm.get_revocation_recommendations(app_id, response['run_id'])
                    recommendations_df4["cost"]=8
                    recommendations_df=recommendations_df1
                    recommendations_df = recommendations_df.append(recommendations_df2, ignore_index=True)
                    recommendations_df = recommendations_df.append(recommendations_df3, ignore_index=True)
                    recommendations_df = recommendations_df.append(recommendations_df4, ignore_index=True)
                    conn_params={
                    "user":"harika",
                    "password":"Harika@445",
                    "account":"kz58877.ca-central-1.aws",
                    "warehouse":"COMPUTE_WH",
                    "database":"SNOWPATROL",
                    "schema":"MAIN"
                    }
                    
                    conn = snowflake.connector.connect(**conn_params)
                    cursor= conn.cursor()
                    cursor1 = conn.cursor()
                    cursor2 = conn.cursor()
                    cursor3= conn.cursor()
                    
                    if(generate_new_recommendations):
 
                        if include_dept== "All":
                            _total_active = int(active_licenses['ACTIVE_LICENSES'].sum())
                            _revocable = int(len(recommendations_df[recommendations_df.REVOKE == 1]))
                            with st.container() as metrics_section:
                                image_path = 'j.png'  # Replace with your image file's name
                                with open(image_path, 'rb') as f:
                                    image_data = f.read()
                                image_base64 = base64.b64encode(image_data).decode()
                                image_pat = 's.png'
                                with open(image_pat, 'rb') as f:
                                    image_ = f.read()
                                image_base = base64.b64encode(image_).decode()
                                _rev = recommendations_df[recommendations_df.REVOKE == 1]
                                
                                apps_tracked="$" + str(_rev["cost"].sum())
    
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
                            total_revocations_dept = int(len(department_df[department_df['REVOKE'] == 1]))
                            _rev = department_df[department_df.REVOKE == 1]
                                
                            apps_tracked="$" + str(_rev["cost"].sum())
                            
    

                            with st.container() as metrics_section:
                                image_path = 'j.png'  # Replace with your image file's name
                                with open(image_path, 'rb') as f:
                                    image_data = f.read()
                                image_base64 = base64.b64encode(image_data).decode()
                                image_pat = 's.png'
                                with open(image_pat, 'rb') as f:
                                    image_ = f.read()
                                image_base = base64.b64encode(image_).decode()
                                #apps_tracked="$" + str(total_revocations_dept*k)
    
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

                            
                        