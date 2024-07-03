import json
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
import mysql.connector

st.set_page_config(page_title='Phonepe_pulse', layout='wide')
st.header('PHONEPE_PULSE_DATA-VISUALIZATION')  # title
col0, col1, col2, col3 = st.columns([3, 1, 1, 2])


def data_fetching():  # fetching data from mySQL database
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Baskar@123",
        database="phonepe_pulse")

    mycursor = mydb.cursor()
    mycursor.execute("SELECT * FROM phonepe_pulse.aggregated_transaction")
    myresult = mycursor.fetchall()
    at = pd.DataFrame(myresult,
                      columns=['State', 'State_code', 'Year', 'Quarter', 'Transacion_type', 'Transacion_count', 'Transacion_amount'])

    mycursor1 = mydb.cursor()
    mycursor1.execute("SELECT * FROM phonepe_pulse.aggregated_user")
    myresult1 = mycursor1.fetchall()
    au = pd.DataFrame(myresult1, columns=['State', 'State_code', 'Year', 'Quarter', 'Usersby_device', 'Transacion_count', 'percentage'])

    mycursor2 = mydb.cursor()
    mycursor2.execute("SELECT * FROM phonepe_pulse.map_transaction")
    myresult2 = mycursor2.fetchall()
    mt = pd.DataFrame(myresult2, columns=['State', 'State_code', 'Year', 'Quarter', 'Usersby_region', 'Transacion_count', 'Transacion_amount'])

    mycursor3 = mydb.cursor()
    mycursor3.execute("SELECT * FROM phonepe_pulse.map_user")
    myresult3 = mycursor3.fetchall()
    mu = pd.DataFrame(myresult3, columns=['State', 'State_code', 'Year', 'Quarter', 'district', 'registeredUsers', 'appOpens'])

    mycursor4 = mydb.cursor()
    mycursor4.execute("SELECT * FROM phonepe_pulse.top_transaction")
    myresult4 = mycursor4.fetchall()
    tt = pd.DataFrame(myresult4, columns=['State', 'State_code', 'Year', 'Quarter', 'entityName_pincodes', 'Transacion_count', 'Transacion_amount'])

    mycursor5 = mydb.cursor()
    mycursor5.execute("SELECT * FROM phonepe_pulse.top_user")
    myresult5 = mycursor5.fetchall()
    tu = pd.DataFrame(myresult5, columns=['State', 'State_code', 'Year', 'Quarter', 'district_pincodes', 'registeredUsers'])

    mydb.close()
    
    return at, au, mt, mu, tt, tu


at, au, mt, mu, tt, tu = data_fetching()


def data_selection():  # data selection from users
    year = 0
    quarter = 0
    with col2:
        a = st.subheader('PhonePe Data')
        a = st.selectbox('Select the Data', ('AGGRE_TRAN', 'AGGRE_USER', 'MAP_TRAN', 'MAP_USER', 'TOP_TRAN', 'TOP_USER'))

    with col2:
        b = st.selectbox('Select the Year', ('2018', '2019', '2020', '2021', '2022'))
        if b == '2018':
            year = 2018
        elif b == '2019':
            year = 2019
        elif b == '2020':
            year = 2020
        elif b == '2021':
            year = 2021
        elif b == '2022':
            year = 2022
    with col2:
        c = st.selectbox('Select the Quarter', ('Q1(JAN - MAR)', 'Q2(APR - JUN)', 'Q3(JUL - SEP)', 'Q4(OCT - DEC)'))

        if c == 'Q1(JAN - MAR)':
            quarter = 1
        elif c == 'Q2(APR - JUN)':
            quarter = 2
        elif c == 'Q3(JUL - SEP)':
            quarter = 3
        elif c == 'Q4(OCT - DEC)':
            quarter = 4

        return a, year, quarter


a, year, quarter = data_selection()


def data_geojson(): # this function will returns the data based on user inputs

    india_states = json.load(open("states_india.geojson", "r"))
    state_id_map = {}
    for feature in india_states["features"]:
        feature["id"] = feature["properties"]["state_code"]
        state_id_map[feature["properties"]["st_nm"]] = feature["id"]
    DA = []
    data = 0
    if a == 'AGGRE_TRAN':
        data = at
    elif a == 'AGGRE_USER':
        data = au
    elif a == 'MAP_TRAN':
        data = mt
    elif a == 'MAP_USER':
        data = mu
    elif a == 'TOP_TRAN':
        data = tt
    elif a == 'TOP_USER':
        data = tu
    data['Year'] = pd.to_datetime(data['Year'], format='%Y')
    groups = data.groupby([pd.Grouper(key='Year', freq='Y'), 'Quarter'])
    for name, group in groups:
        DA.append(group)

    return india_states, DA


india_states, DA = data_geojson()


def data_visualization():  # data visualization based on selesction of  year, Quarter and data types.

    da = 0
    if year == 2018 and quarter == 1:
        da = DA[0]
    if year == 2018 and quarter == 2:
        da = DA[1]
    if year == 2018 and quarter == 3:
        da = DA[2]
    if year == 2018 and quarter == 4:
        da = DA[3]
    if year == 2019 and quarter == 1:
        da = DA[4]
    if year == 2019 and quarter == 2:
        da = DA[5]
    if year == 2019 and quarter == 3:
        da = DA[6]
    if year == 2019 and quarter == 4:
        da = DA[7]
    if year == 2020 and quarter == 1:
        da = DA[8]
    if year == 2020 and quarter == 2:
        da = DA[9]
    if year == 2020 and quarter == 3:
        da = DA[10]
    if year == 2020 and quarter == 4:
        da = DA[11]
    if year == 2021 and quarter == 1:
        da = DA[12]
    if year == 2021 and quarter == 2:
        da = DA[13]
    if year == 2021 and quarter == 3:
        da = DA[14]
    if year == 2021 and quarter == 4:
        da = DA[15]
    if year == 2022 and quarter == 1:
        da = DA[16]
    if year == 2022 and quarter == 2:
        da = DA[17]
    if year == 2022 and quarter == 3:
        da = DA[18]
    if year == 2022 and quarter == 4:
        da = DA[19]

    if a == 'AGGRE_TRAN':
        with col2:
            d = st.selectbox('Select the DATA type', ('Trans_count', 'Trans_amount'))
        with col0:
            if d == 'Trans_count':
                da["Scale"] = np.log2(da["Transacion_count"])
                fig = px.choropleth(da,
                                    locations="State_code",
                                    geojson=india_states,
                                    color="Scale",
                                    hover_name="State",
                                    hover_data=["Transacion_count"],
                                    width=800, height=600,
                                    )

                fig.update_geos(fitbounds="locations", visible=False)

                state_trans_count = da.groupby("Transacion_type")["Transacion_count"].sum()
                col3.subheader('Transaction counts')
                col3.write(state_trans_count)
                return st.plotly_chart(fig)

        with col0:
            if d == 'Trans_amount':
                da["Scale_1"] = np.log2(da["Transacion_amount"])
            fig = px.choropleth(da,
                                locations="State_code",
                                geojson=india_states,
                                color="Scale_1",
                                hover_name="State",
                                hover_data=["Transacion_amount"],
                                width=800, height=600,
                                )

            fig.update_geos(fitbounds="locations", visible=False)
            state_trans_amount = da.groupby("Transacion_type")["Transacion_amount"].sum()
            col3.header('Transaction amounts')
            col3.write(state_trans_amount)
            return st.plotly_chart(fig)

    if a == 'AGGRE_USER':
        with col2:
            d = st.selectbox('Select the DATA type', ('Trans_count', 'Percentage'))
        with col0:
            if d == 'Trans_count':
                da["Scale"] = np.log2(da["Transacion_count"])

                fig = px.choropleth(da,
                                    locations="State_code",
                                    geojson=india_states,
                                    color="Scale",
                                    hover_name="State",
                                    hover_data=["Transacion_count"],
                                    width=800, height=600,
                                    )
                fig.update_geos(fitbounds="locations", visible=False)
                state_trans_count = da.groupby("Usersby_device")["Transacion_count"].sum()
                col3.subheader('User device Trans count')
                col3.write(state_trans_count)
                return st.plotly_chart(fig)

        with col0:
            if d == 'Percentage':
                da["Scale_1"] = np.log2(da["percentage"])
                fig = px.choropleth(da,
                                    locations="State_code",
                                    geojson=india_states,
                                    color="Scale_1",
                                    hover_name="State",
                                    hover_data=["percentage"],
                                    width=800, height=600,
                                    )
                fig.update_geos(fitbounds="locations", visible=False)
                state_trans_count = da.groupby("Usersby_device")["percentage"].sum()
                col3.subheader('User device Percentage')
                col3.write(state_trans_count)
                return st.plotly_chart(fig)

    if a == 'MAP_TRAN':
        with col2:
            d = st.selectbox('Select the DATA type', ('Trans_count', 'Trans_amount'))
        with col0:
            if d == 'Trans_count':
                da["Scale"] = np.log2(da["Transacion_count"])
                fig = px.choropleth(da,
                                    locations="State_code",
                                    geojson=india_states,
                                    color="Scale",
                                    hover_name="State",
                                    hover_data=["Transacion_count"],
                                    width=800, height=600,
                                    )

                fig.update_geos(fitbounds="locations", visible=False)
                state_trans_count = da.groupby("Usersby_region")["Transacion_count"].sum()
                col3.subheader('Transaction count')
                col3.write(state_trans_count)
                return st.plotly_chart(fig)

        with col0:
            if d == 'Trans_amount':
                da["Scale_1"] = np.log2(da["Transacion_amount"])
            fig = px.choropleth(da,
                                locations="State_code",
                                geojson=india_states,
                                color="Scale_1",
                                hover_name="State",
                                hover_data=["Transacion_amount"],
                                width=800, height=600,
                                )

            fig.update_geos(fitbounds="locations", visible=False)
            state_trans_amount = da.groupby("Usersby_region")["Transacion_amount"].sum()
            col3.header('Transaction amount')
            col3.write(state_trans_amount)
            return st.plotly_chart(fig)

    if a == 'MAP_USER':
        with col2:
            d = st.selectbox('Select the DATA type', ('Registered_Users', 'App_Opens'))
        with col0:
            if d == 'Registered_Users':
                da["Scale"] = np.log2(da["registeredUsers"])
                fig = px.choropleth(da,
                                    locations="State_code",
                                    geojson=india_states,
                                    color="Scale",
                                    hover_name="State",
                                    hover_data=["registeredUsers"],
                                    width=800, height=600,
                                    )

                fig.update_geos(fitbounds="locations", visible=False)
                state_trans_count = da.groupby("district")["registeredUsers"].sum()
                col3.subheader('Registered users by district')
                col3.write(state_trans_count)
                return st.plotly_chart(fig)

        with col0:
            if d == 'App_Opens':
                da["Scale_1"] = np.log2(da["appOpens"])
            fig = px.choropleth(da,
                                locations="State_code",
                                geojson=india_states,
                                color="Scale_1",
                                hover_name="State",
                                hover_data=["appOpens"],
                                width=800, height=600,
                                )

            fig.update_geos(fitbounds="locations", visible=False)
            state_trans_amount = da.groupby("district")["appOpens"].sum()
            col3.header('App opens count by district')
            col3.write(state_trans_amount)
            return st.plotly_chart(fig)

    if a == 'TOP_TRAN':
        with col2:
            d = st.selectbox('Select the DATA type', ('Trans_count', 'Trans_amount'))
        with col0:
            if d == 'Trans_count':
                da["Scale"] = np.log2(da["Transacion_count"])
                fig = px.choropleth(da,
                                    locations="State_code",
                                    geojson=india_states,
                                    color="Scale",
                                    hover_name="State",
                                    hover_data=["Transacion_count"],
                                    width=800, height=600,
                                    )

                fig.update_geos(fitbounds="locations", visible=False)
                state_trans_count = da.groupby("entityName_pincodes")["Transacion_count"].sum()
                col3.subheader('Transaction count')
                col3.write(state_trans_count)
                return st.plotly_chart(fig)

        with col0:
            if d == 'Trans_amount':
                da["Scale_1"] = np.log2(da["Transacion_amount"])
            fig = px.choropleth(da,
                                locations="State_code",
                                geojson=india_states,
                                color="Scale_1",
                                hover_name="State",
                                hover_data=["Transacion_amount"],
                                width=800, height=600,
                                )

            fig.update_geos(fitbounds="locations", visible=False)
            state_trans_amount = da.groupby("entityName_pincodes")["Transacion_amount"].sum()
            col3.header('Transaction amount')
            col3.write(state_trans_amount)
            return st.plotly_chart(fig)

    if a == 'TOP_USER':
        with col0:
            da["Scale"] = np.log2(da["registeredUsers"])
            fig = px.choropleth(da,
                                locations="State_code",
                                geojson=india_states,
                                color="Scale",
                                hover_name="State",
                                hover_data=["registeredUsers"],
                                width=800, height=600,
                                )

            fig.update_geos(fitbounds="locations", visible=False)
            state_trans_count = da.groupby("district_pincodes")["registeredUsers"].sum()
            col3.subheader('Registered users')
            col3.write(state_trans_count)
            return st.plotly_chart(fig)


data_visualization()
