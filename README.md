# Phonepe-Pulse-data-visualisation
PhonePe Pulse Data Visualisation
References:

Data Link:https://github.com/PhonePe/pulse#readme

choropleth:https://plotly.com/python/choropleth-maps/

Streamlit: https://docs.streamlit.io/library/api-reference

Problem Statement: The Phonepe pulse Github repository contains a large amount of data related to various metrics and statistics. The goal is to extract this data and process it to obtain insights and information that can be visualized in a user-friendly manner.

Workflow:

1.Data creation: this function will read the phonepe Pulse date which is cloned from repository by provided data path.

2.data extraction: this function will validate the data and drops the nul value if any. created two csv file which will contains State names and state code from downloaded geojson file. based on the state name the state code column has been inserted to dataframe. the data has been preprocessed by removing whitespaces and special characters in the data.

3.data preprocessing: we created one database inside mySQL database and inserted as tables with the preprocessed data.

4.data fetching: here we fetching the tables from MySQL phonepe pulse database using mysql connector library and converting to pandas dataframe.

5.data_selection: Here we using the streamlit library to create user friendly interface with multiple dropdown options. the user has to select the data file , year, quarter and data type.

6.data geojson: in this function we read a geojson file to plot the data on geo map. we groupby the data with the year and quarter column to visualize the data as year and quarter wise.

data visualization: in this function we using plotly.express library to visualize the data as state wise. the choropleth map has been used to visualize the data with state and transaction count and transaction amount details. in streamlit interface we can see the total transaction count and transaction amount based on data selection by user.
