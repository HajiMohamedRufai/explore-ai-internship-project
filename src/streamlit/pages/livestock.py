import streamlit as st 
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2

st.set_page_config(page_icon = '🐐',
page_title = 'Livestock',
layout='wide')

st.title("LIVESTOCK MARKET ANALYSIS")

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

#query the datasets from the database
livestock_cattle = run_query("SELECT * FROM livestock_cattle_prices")
livestock_sheep = run_query("SELECT * FROM livestock_sheep_prices")
livestock_pork = run_query("SELECT * FROM livestock_pork_prices")

#change the column name 'class' to 'df_class' because 'class' is reserved

livestock_cattle['cattle_class'] = livestock_cattle['class']



cattle_class_list = list(livestock_cattle.cattle_class.unique())


livestock_cattle = livestock_cattle.sort_values(by=['date_to']).reset_index(drop = True)

st.write('##  cattle')
st.divider()
placeholder = st.empty()



#create columns for the filters
left, center, right = st.columns((1,1,1))

with left:
    class1_filter = st.selectbox('select a cattle class',cattle_class_list)

with right:
    class2_filter = st.selectbox('select a pairwise comparison class', cattle_class_list)

left, right = st.columns(2)
with left:
    class1_dataset = livestock_cattle[livestock_cattle['cattle_class'] == class1_filter]
    fig = px.line(class1_dataset, x='date_to', y='avg_purch', color='cattle_class')

    fig.update_layout(barmode = 'group', 
    title= "Avg Purchase Price versus Dates", width=450, height=450)
    st.plotly_chart(fig)
    
def pair_plot(df, column, class1=None, class2 = None):
    options = [class1, class2]
    class_df = df[df[column].isin(options)]
    fig = px.line(class_df, x='date_to', y = 'avg_purch', color=column)
    fig.update_layout(barmode = 'group', title =f'Comparison between pairs of classes',
    width=450, height=450)

    return st.plotly_chart(fig)

with right:
    pair_plot(livestock_cattle, 'cattle_class', class1=class1_filter, class2=class2_filter)

#generate relevant kpis
avg_purch = round(np.mean(class1_dataset['avg_purch'], 0))
avg_mass = round(np.mean(class1_dataset['avg_mass']))
avg_selling = round(np.mean(class1_dataset['avg_selling']))

#add the kpis to the placeholder
with placeholder:
    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric(label = f'Avg purchase price (ZAR) for class {class1_filter}', value = avg_purch)
    kpi2.metric(label = f'Avg mass(Kg) for class {class1_filter}', value = avg_mass )
    kpi3.metric(label =f'Avg selling price (ZAR) for class{class1_filter}', value = avg_selling)

#visuals for the sheep dataset
st.divider()
st.write('## sheep')

placeholder = st.empty()

livestock_sheep['Sheep_class'] = livestock_sheep['class']
sheep_class_list = list(livestock_sheep.Sheep_class.unique())

left, right = st.columns(2)

with left:
    Sheep_Class_filter = st.selectbox('Select a sheep class', sheep_class_list)

with right:
    Sheep_Class_filter_2 = st.selectbox('Select a pairwise comparison class', sheep_class_list)

left, right = st.columns(2)
with left:
    class1_dataset = livestock_sheep[livestock_sheep['Sheep_class'] == Sheep_Class_filter]
    fig = px.line(class1_dataset.sort_values(by = 'date_to'), x='date_to', y='avg_purch', color='Sheep_class')

    fig.update_layout(barmode = 'group', 
    title= "Avg Purchase Price versus Dates", width=450, height=450)
    st.plotly_chart(fig)

with right:
    pair_plot(livestock_sheep.sort_values(by='date_to'), 'Sheep_class', class1=Sheep_Class_filter, class2=Sheep_Class_filter_2)

avg_purch = round(np.mean(class1_dataset['avg_purch']), 0)
avg_mass = round(np.mean(class1_dataset['avg_mass']), 0)
avg_selling = round(np.mean(class1_dataset['avg_selling']))

with placeholder:
    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric(label= f'Average Purchase price (ZAR) for class {Sheep_Class_filter} (ZAR)', value= avg_purch)
    kpi2.metric(label = f'Average mass (Kg) for class {Sheep_Class_filter} (Kgs)', value = avg_mass)
    kpi3.metric(label = f'average selling price (ZAR) for class {Sheep_Class_filter}', value = avg_selling)
#visuals for the pigs dataset
st.divider()
st.write('## Pigs')

placeholder = st.empty()

livestock_pork['pig_class'] = livestock_pork['class']

livestock_pork = livestock_pork.sort_values(by='date_to').reset_index(drop=True)

weight_list1 = list(livestock_pork.weight.unique())
class_list2 = list(livestock_pork.pig_class.unique())

left, right =st.columns(2)

with right:
    weight_filter =st.selectbox('Select weight class', weight_list1)

with left:
    class_filter =st.selectbox('select a pig class', class_list2)

#filtering the dataset according to the widgets
weight_df = livestock_pork.loc[livestock_pork['weight'] == weight_filter]
class_df = weight_df.loc[weight_df['pig_class'] == class_filter]

left, right = st.columns(2)
#plot the graphs 

with left:
    fig = px.bar(class_df, x= 'date_to', y ='units')
    fig.update_layout(title = 'Number of units vs date_to', width=450, height = 450)
    st.plotly_chart(fig)

with right:
    fig = px.line(class_df, x = 'date_to', y = 'avg_purch')
    fig.update_layout(title = 'Average purchase price', height= 450, width= 450)
    st.plotly_chart(fig)

avg_purch = round(np.mean(class_df['avg_purch']),0)
avg_units = round(np.mean(class_df['units']),0)
avg_mass = round(np.mean(class_df['avg_mass']))

with placeholder:
    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric(label=f'Average purchase price for class {class_filter} (ZAR)', value= avg_purch)

    kpi2.metric(label= f'Average units for class {class_filter}', value = avg_units)

    kpi3.metric(label =f'Average selling price for class {class_filter}', value = avg_mass )
