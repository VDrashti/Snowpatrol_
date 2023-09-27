import streamlit as st
from PIL import Image
import re

# from streamlit_extras.badges import badge

#################################
#
#   Utility functions
#
#################################
def insert_img(img_file: str, caption:str):
    image = Image.open(img_file)
    st.image(image, caption=caption)


#################################
#
#   UI Builder
#
#################################
st.set_page_config(
     page_title="Snowpatrol - License Optimization",
     page_icon=":rotating_light:",
     layout="wide",
     initial_sidebar_state="expanded"
)
# badge(type="github", name="snowflakecorp/gsi-se-snowpatrol-demo")
st.header(":rotating_light: Snowpatrol - License Optimization")

st.divider()

with open("About.md") as markdown_content:
    for line in markdown_content.readlines():
        if line.startswith('insert_img'):
            res = re.search(r'insert_img\((.*),(.*)\)',line)
            insert_img(res.group(1), res.group(2))
        else:
            st.write(line)
