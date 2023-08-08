#importing relevant libraries
# ------------------------------------------------------------------------

import streamlit as st 
import pandas as pd 
import numpy as np
import plotly.express as px
import os
import plotly.graph_objects as go
import psycopg2

from datetime import datetime, timedelta
# ------------------------------------------------------------------------
st.set_page_config(page_icon = '🌽',
page_title = 'Grains',
layout='wide')

st.title('GRAIN MARKET ANALYSIS')

st.sidebar.write('#')
st.sidebar.write('#')
                    
#st.sidebar.image('resources\Department-of-Agriculture-Land-Reform-and-Rural-Development-DALRRD.webp')

st.sidebar.write('#')
st.sidebar.write('#')

st.sidebar.image('resources/EAI_square_navy.png')

# initialize connection to postgres db
# use st.cache_resource to run the function only once
@st.cache_resource
def init_connection():
    return psycopg2.connect(**st.secrets['postgres'])

conn = init_connection()

#query the database
#use st.cache_data to cache the imported data into app memory
@st.cache_data
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        #store the data in a python variable (called list because the query returns a list)
        list = cur.fetchall()

        #fetch the column names 
        col_names = []
        for elt in cur.description:
            col_names.append(elt[0])

        #return a pandas dataframe with specified column names
        return pd.DataFrame(list, columns=col_names)

#query the database to get a pandas dataframe
grains = run_query("SELECT * from grain_prices;")

grains[['Day', 'Month', 'Year', "Grain_Type"]] = grains['contract_type'].str.split(' ', expand=True)
grains['Month_Code'] = grains['Month'].replace(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'], ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12' ])
grains['Year_Full_Format'] = '20' + grains['Year']
grains['Contract_Date'] = grains['Month_Code']+'/'+grains['Day']+'/'+ grains['Year_Full_Format']
grains['Contract_Date'] = pd.to_datetime(grains.Contract_Date)
grains['upload_date'] = pd.to_datetime(grains.upload_date)
Transaction_Volume = grains.groupby(['Grain_Type']).size().rename('Count').reset_index()


# ------------------------------------------------------------------------


grains['Market'] = grains['market_name']+"  "+grains['market_code']


#declaring a placeholder to hold the KPIs
placeholder = st.empty()

date_range = pd.to_datetime((datetime.now()-timedelta(days =  365)))
grains = grains.loc[grains['upload_date']>date_range, ]

# organising the filters
# ------------------------------------------------------------------------ 

left, center, right = st.columns(3)

with left:
    markets = list(grains.Market.unique())
    Markets_filter = st.selectbox('Select a market', markets)

with center:
    grain_type = list(grains.Grain_Type.unique())
    grain_type_filter = st.selectbox('Select a grain type', grain_type)

with right:
    dates = sorted(list(grains.upload_date.unique()), reverse = True)
    date_filter = st.selectbox('Select a date', dates)

# ------------------------------------------------------------------------


#plotting the last price of commodities
# ------------------------------------------------------------------------

left_column, right_column = st.columns((1,1))

with left_column:
    markets_df = grains[grains['Market'] == Markets_filter]
    grain_df = markets_df.loc[markets_df['Grain_Type'] == grain_type_filter]
    date_df = grain_df.loc[grain_df['upload_date'] == date_filter]
    fig = px.bar(date_df, x='Contract_Date', y = 'last', width=450, height=450, title='Last price vs Contract date')
    st.plotly_chart(fig, theme = 'streamlit')

    Max_Price = grain_df['last'].loc[grain_df['last'].idxmax()]
    min_price = grain_df['last'].loc[grain_df['last'].idxmax()]

    max_date = '{:%grain_df %d, %Y}'.format(grain_df['upload_date'].loc[grain_df['upload_date'].idxmax()])
    min_date = '{:%grain_df %d, %Y}'.format(grain_df['upload_date'].loc[grain_df['upload_date'].idxmin()])

    fig = px.line(grain_df.sort_values(by = 'upload_date'), x='upload_date', y='last', color='contract_type')
    fig.update_layout(barmode ='group',
    title = f'Last Price of {grain_type_filter} contracts', width=450, height = 450)
    st.plotly_chart(fig, theme='streamlit')

with right_column:
    markets_df = grains.loc[grains['Market'] == Markets_filter]
    grain_df = markets_df.loc[markets_df['Grain_Type'] == grain_type_filter]
    date_range_from = pd.to_datetime(datetime.now())
    dates_df = grain_df.loc[grain_df['Contract_Date']>date_range_from]
    grouped = dates_df.groupby('Contract_Date')[['last', 'bid']].mean().reset_index()

    fig = go.Figure(data=[
        go.Bar(name = 'Last Price (AVG)', x = grouped['Contract_Date'],
        y = grouped['last']),
        go.Bar(name = 'Bid Price (AVG)', x = grouped['Contract_Date'], 
        y = grouped['bid'])
    ])

    fig.update_layout(barmode = 'group', title = f'Average price(Last/bid) over one year for unsettled {grain_type_filter} contacts', width=550, height=450)
    st.plotly_chart(fig)


avg_last = np.mean(date_df['last'])
avg_bid = np.mean(date_df['bid'])
highest_bid = np.max(date_df['bid'])

# ------------------------------------------------------------------------


# plotting the bid rice and last price
# ------------------------------------------------------------------------

with right_column:
    fig = go.Figure(data = [
                    go.Bar(name = 'Last', x = date_df['Contract_Date'], y = date_df["last"]),
                    go.Bar(name = 'Bid', x = date_df['Contract_Date'], y = date_df['bid'])

    ])
    fig.update_layout(width=550, height= 450,barmode = 'group', title='Grain prices (Last/Bid price) versus contract dates')
    st.plotly_chart(fig, theme = 'streamlit')



#adding relevant KPI's to the place holders
# ------------------------------------------------------------------------

with placeholder:

    kpi1, kpi2, kpi3 = st.columns(3)

    kpi1.metric(label='Average last price (ZAR)', value = round(avg_last))
    kpi2.metric(label = 'Highest bid price (ZAR)', value = highest_bid)
    kpi3.metric(label = 'Average Bid Price (ZAR)', value = round(avg_bid))

# ------------------------------------------------------------------------

