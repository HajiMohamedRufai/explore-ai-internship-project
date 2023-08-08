import streamlit as st
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2






st.set_page_config(page_icon = '🌷',
page_title = 'Horticulture',
layout='wide')

st.title('HORTICULTURE MARKET ANALYSIS')

st.sidebar.write('#')
st.sidebar.write('#')
                    
#st.sidebar.image('resources\Department-of-Agriculture-Land-Reform-and-Rural-Development-DALRRD.webp')

st.sidebar.write('#')
st.sidebar.write('#')

st.sidebar.image('resources/EAI_square_navy.png')

# use st.cache_resource to store the function in cached memory
@st.cache_resource
def init_connection():
    return psycopg2.connect(**st.secrets['postgres'])

#initialize the connection
conn = init_connection()

#use st.cache_data to cache the dataset in app memory
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        list = cur.fetchall()

        #fetch the column names 
        col_names = []
        for elt in cur.description:
            col_names.append(elt[0])

        return pd.DataFrame(list, columns=col_names)

horticulture = run_query('SELECT * FROM horticulture_prices')
horticulture[['series', 'product']] = horticulture['product'].str.split('.', expand = True)
horticulture['product'].str.replace(' ', '')
hortuculture = horticulture.sort_values(by = 'date')
horticulture['Veg_Class'] = horticulture['class']


placeholder = st.empty()


left, center, right = st.columns(3)

with left:
    market_list = list(horticulture['market_name'].unique())
    market_filter = st.selectbox('Select a market', market_list)

    market_df = horticulture.loc[horticulture['market_name'] ==market_filter]
    product_list = list(market_df['product'].unique())
    product_filter = st.selectbox('Select a product', product_list)
    product_df = market_df.loc[market_df['product'] == product_filter]
with center:
    variety_list = list(product_df['variety'].unique())
    variety_filter = st.selectbox('Select a variety', variety_list)
    variety_df = product_df.loc[product_df['variety'] == variety_filter]

    class_filter = st.selectbox('Select a class',list(variety_df['Veg_Class'].unique()))
    class_df = variety_df.loc[variety_df['Veg_Class']==class_filter]

with right:
    
    size_filter = st.selectbox('Select a size',list(class_df['size'].unique()))
    size_df = class_df.loc[class_df['size']==size_filter]

    package_filter = st.selectbox('select a package type', list(size_df['package'].unique()))
    package_df = size_df.loc[size_df['package']==package_filter]
left, right = st.columns(2)
with left:
    fig = px.bar(package_df, x='date', y = 'closing_price')
    fig.update_layout(title = f'Closing price for {product_filter}', height = 450, width = 450)
    st.plotly_chart(fig)

pie_products = horticulture.loc[horticulture['product']== product_filter]
pie_products.reset_index(inplace = True)
with right:
    fig = px.pie(pie_products, values = pie_products['market_name'].value_counts().values, names = pie_products['market_name'].unique())
    fig.update_traces(hoverinfo='label+percent+name', textinfo = 'value', hole = .4)
    annotations = [dict(text = f'{product_filter}', x=0.5, y=0.5, font_size = 13, showarrow=False)]
    fig.update_layout(title=f'Volume of trading activities for {product_filter} across various markets')
    st.plotly_chart(fig)

avg_closing = round(np.mean(package_df['closing_price']), 0)
avg_price = round(np.mean(package_df['average_price']), 0)
closing_stock = round(np.sum(package_df['closing_stock']), 0)

with placeholder:
    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric(label= 'Average closing price (ZAR)', value = avg_closing)
    kpi2.metric(label='Average price (ZAR)', value= avg_price)
    kpi3.metric(label = 'Closing stock(sum of units)', value = closing_stock)