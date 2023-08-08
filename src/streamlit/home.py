import streamlit as st
import pandas as pd 
import plotly.express as px 
import numpy as np 
from PIL import Image
import matplotlib


st.set_page_config(page_title = "Home",
                    page_icon='🏠')

st.sidebar.write('#')
st.sidebar.write('#')
                    
#st.sidebar.image('resources\Department-of-Agriculture-Land-Reform-and-Rural-Development-DALRRD.webp')

st.sidebar.write('#')
st.sidebar.write('#')

st.sidebar.image('resources/EAI_square_navy.png')
st.title('WELCOME TO THE DALRRD DASHBOARD')

st.markdown(
    '''
## Project description
The aim of this project is to provide an interactive web based dashboard to help farmers gain insights into the various commodity prices of products provided
on the AMIS system. The dashboard contains three pages of valuable visuals organised in terms of produce cartegory. The 
products are: ''')

#horizontal divider for the page
st.write('---')

#declare two columns to divide the page
left_column, right_column = st.columns((1,1))

#restrict text to the left column
with left_column:
    st.markdown(
        '''
    * ### Grains:
        contains relevant information about grains such as:\n
        * The average bid price per market.
        * The average last price per market.
        * Trends of the bid and last price over time. 
    ''')

#paste an image on the right column
with right_column:
    st.image('resources/market.jpg')

#divide the page
st.write('---')

left, right = st.columns((1,1))

#restrict text to the left side
with left:
     st.markdown(
        '''
    * ### Horticulture:
        Contains relevant information about horticulture such as:\n
        * The average closing price per market.
        * The closing price per market.
        * Trading volumes of products per market.
        * Variety of filtering for options for the otherwise large dataset.  ''')

with right:
    st.image('resources/vegetables-g73d70fde1_1280.jpg')

st.divider()

left, right = st.columns((1,1))

with left:
     st.markdown(
        '''
    * ### Livestock:
        Contains relevant information and graphs about Livestock markets such as:\n
        * The average purchase price per market and class.
        * The average unit mass of products per class.
        * Average unit count per market and class.
        * Variety of filtering for options for the otherwise large dataset.  ''')

with right:
    st.image('resources/man-gce6e0f165_1280.jpg')
('---')
st.markdown('''
## Aims and objectives
* Developing a web scraping tool that automatically collects data from different websites and formats.
* Transforming the collected data into a dashboard that applies solid exploratory data analysis (EDA) principles to ensure relevant, useful figures and statistics are presented.
* Testing the dashboard and ensuring that it is user-friendly and easily interpretable by buyers and sellers.
* Evaluating the impact of the automated data scraping and transformation system on the agricultural sector in South Africa.
* Exploring the potential applications of the project in other regions and sectors, providing insights into the benefits of automating data collection and transformation for decision-making.

    '''
)
st.divider()
st.markdown('''
## Results
The project has achieved its objectives by identifying the different sources and formats of commodity price data in horticulture, grain, and livestock categories
 in South Africa, developing a web scraping tool that automatically collects data from different websites and formats, transforming the collected data, and applying 
 solid exploratory data analysis (EDA) principles to achieve insights. The insights are the driving force of this dashboard which provides a link
 between users and the very valuable but hard to interpret data.

'''
)