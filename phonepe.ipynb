import pandas as pd
import os, csv
import mysql.connector


def data_extraction():  # reading cloned data from phonepe.
    path1 = 'C:/Users/Nandhini/pulse/data/'
    path_file = []
    for root, dirs, files in os.walk(path1):
        if root.endswith('state'):
            a = "".join(["/" if wor == "\\" else wor for wor in root])
            path_file.append(a)

    return path_file


directory = data_extraction()

Statename = pd.read_csv('Statecode.csv')
Code = pd.read_csv('St_code.csv')


def data_extraction():  # preprocessing & creating dataframe and converting to csv file
    path = directory[0]
    data_file = os.listdir(path)
    data = {'State': [], 'Year': [], 'Quater': [], 'Transacion_type': [], 'Transacion_count': [], 'Transacion_amount': []}

    for x in data_file:
        a = path+"/"+x+"/"
        data_year = os.listdir(a)
        for j in data_year:
            b = a+j+"/"
            data_year_val = os.listdir(b)
            for k in data_year_val:
                c = b+k
                df = pd.read_json(c)
                df = df.dropna()
                for i in df['data']['transactionData']:
                    name = i['name']
                    count = i['paymentInstruments'][0]['count']
                    amount = i['paymentInstruments'][0]['amount']
                    data['Transacion_type'].append(name)
                    data['Transacion_count'].append(count)
                    data['Transacion_amount'].append(amount)
                    data['State'].append(x)
                    data['Year'].append(j)
                    data['Quater'].append(int(k.strip('.json')))

    return data


data = data_extraction()
df = pd.DataFrame(data)
df['State'] = df['State'].str.replace('-', '')
df['State'] = df['State'].str.replace('&', '')
mdf = pd.merge(Statename, df, on='State', how='right')
mdf = mdf.drop('State', axis=1)
mdf = pd.merge(Code, mdf, on='State_code', how='right')
mdf.to_csv('aggregated_transaction.csv')


def data_extraction1(): # preprocessing & creating dataframe and converting to csv file
    path = directory[1]
    data_file = os.listdir(path)
    data1 = {'State': [], 'Year': [], 'Quater': [], 'Usersby_device': [], 'Transacion_count': [], 'percentage': []}

    for x in data_file:
        a = path + "/" + x + "/"
        data_year = os.listdir(a)
        for j in data_year:
            b = a + j + "/"
            data_year_val = os.listdir(b)
            for k in data_year_val:
                c = b + k
                df = pd.read_json(c)
                df = df.dropna()
                if 'data' in df and 'usersByDevice' in df['data']:
                    for z in df['data']['usersByDevice']:
                        brand = z['brand']
                        count = z['count']
                        amount = z['percentage']
                        data1['Usersby_device'].append(brand)
                        data1['Transacion_count'].append(count)
                        data1['percentage'].append(amount)
                        data1['State'].append(x)
                        data1['Year'].append(j)
                        data1['Quater'].append(int(k.strip('.json')))

    return data1


data1 = data_extraction1()
df = pd.DataFrame(data1)
df['State'] = df['State'].str.replace('-', '')
df['State'] = df['State'].str.replace('&', '')
mdf = pd.merge(Statename, df, on='State', how='right')
mdf = mdf.drop('State', axis=1)
mdf = pd.merge(Code, mdf, on='State_code', how='right')
mdf.to_csv('aggregated_user.csv')


def data_extraction2(): # preprocessing & creating dataframe and converting to csv file
    path = directory[2]
    data_file = os.listdir(path)
    data2 = {'State': [], 'Year': [], 'Quater': [], 'Usersby_region': [], 'Transacion_count': [],
             'Transacion_amount': []}

    for x in data_file:
        a = path + "/" + x + "/"
        data_year = os.listdir(a)
        for j in data_year:
            b = a + j + "/"
            data_year_val = os.listdir(b)
            for k in data_year_val:
                c = b + k
                df = pd.read_json(c)
                df = df.dropna()
                for z in df['data']['hoverDataList']:
                    name = z['name']
                    count = z['metric'][0]['count']
                    amount = z['metric'][0]['amount']
                    data2['Usersby_region'].append(name)
                    data2['Transacion_count'].append(count)
                    data2['Transacion_amount'].append(amount)
                    data2['State'].append(x)
                    data2['Year'].append(j)
                    data2['Quater'].append(int(k.strip('.json')))

    return data2


data2 = data_extraction2()
df = pd.DataFrame(data2)
df['State'] = df['State'].str.replace('-', '')
df['State'] = df['State'].str.replace('&', '')
mdf = pd.merge(Statename, df, on='State', how='right')
mdf = mdf.drop('State', axis=1)
mdf = pd.merge(Code, mdf, on='State_code', how='right')
mdf.to_csv('map_transaction.csv')


def data_extraction3(): # preprocessing & creating dataframe and converting to csv file
    path = directory[3]
    data_file = os.listdir(path)
    data3 = {'State': [], 'Year': [], 'Quater': [], 'district': [], 'registeredUsers': [], 'appOpens': []}

    for x in data_file:
        a = path + "/" + x + "/"
        data_year = os.listdir(a)
        for j in data_year:
            b = a + j + "/"
            data_year_val = os.listdir(b)
            for k in data_year_val:
                c = b + k
                df = pd.read_json(c)
                df = df.dropna()
                for y, z in df['data']['hoverData'].items():
                    name = y
                    count = z['registeredUsers']
                    amount = z['appOpens']
                    data3['district'].append(name)
                    data3['registeredUsers'].append(count)
                    data3['appOpens'].append(amount)
                    data3['State'].append(x)
                    data3['Year'].append(j)
                    data3['Quater'].append(int(k.strip('.json')))

    return data3


data3 = data_extraction3()
df = pd.DataFrame(data3)
df['State'] = df['State'].str.replace('-', '')
df['State'] = df['State'].str.replace('&', '')
mdf = pd.merge(Statename, df, on='State', how='right')
mdf = mdf.drop('State', axis=1)
mdf = pd.merge(Code, mdf, on='State_code', how='right')
mdf.to_csv('map_user.csv')


def data_extraction4(): # preprocessing & creating dataframe and converting to csv file
    path = directory[4]
    data_file = os.listdir(path)
    data4 = {'State': [], 'Year': [], 'Quater': [], 'entityName_pincodes': [], 'Transacion_count': [], 'Transacion_amount': []}

    for x in data_file:
        a = path + "/" + x + "/"
        data_year = os.listdir(a)
        for j in data_year:
            b = a + j + "/"
            data_year_val = os.listdir(b)
            for k in data_year_val:
                c = b + k
                df = pd.read_json(c)
                df = df.dropna()
                for z in df['data']['districts'] + df['data']['pincodes']:
                    name = z['entityName']
                    count = z['metric']['count']
                    amount = z['metric']['amount']
                    data4['entityName_pincodes'].append(name)
                    data4['Transacion_count'].append(count)
                    data4['Transacion_amount'].append(amount)
                    data4['State'].append(x)
                    data4['Year'].append(j)
                    data4['Quater'].append(int(k.strip('.json')))

    return data4


data4 = data_extraction4()
df = pd.DataFrame(data4)
df['State'] = df['State'].str.replace('-', '')
df['State'] = df['State'].str.replace('&', '')
mdf = pd.merge(Statename, df, on='State', how='right')
mdf = mdf.drop('State', axis=1)
mdf = pd.merge(Code, mdf, on='State_code', how='right')
mdf.to_csv('top_transaction.csv')


def data_extraction5(): # preprocessing & creating dataframe and converting to csv file
    path = directory[5]
    data_file = os.listdir(path)
    data5 = {'State': [], 'Year': [], 'Quater': [], 'district_pincodes': [], 'registeredUsers': []}

    for x in data_file:
        a = path + "/" + x + "/"
        data_year = os.listdir(a)
        for j in data_year:
            b = a + j + "/"
            data_year_val = os.listdir(b)
            for k in data_year_val:
                c = b + k
                df = pd.read_json(c)
                df = df.dropna()
                for z in df['data']['districts'] + df['data']['pincodes']:
                    name = z['name']
                    count = z['registeredUsers']
                    data5['district_pincodes'].append(name)
                    data5['registeredUsers'].append(count)
                    data5['State'].append(x)
                    data5['Year'].append(j)
                    data5['Quater'].append(int(k.strip('.json')))

    return data5


data5 = data_extraction5()
df = pd.DataFrame(data5)
df['State'] = df['State'].str.replace('-', '')
df['State'] = df['State'].str.replace('&', '')
mdf = pd.merge(Statename, df, on='State', how='right')
mdf = mdf.drop('State', axis=1)
mdf = pd.merge(Code, mdf, on='State_code', how='right')
mdf.to_csv('top_user.csv')


def data_preprocessing():   # reading csv file and importing to MySQL database.
    dict_list = list()

    with open('aggregated_transaction.csv', 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        next(csv_reader)
        for rows in csv_reader:
            dict_list.append({'State': rows[1], 'State_code': rows[2], 'Year': rows[3], 'Quarter': rows[4], 'Transacion_type': rows[5],
                              'Transacion_count': rows[6], 'Transacion_amount': rows[7]})

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Baskar@123",
        database="phonepe_pulse")

    mycursor = mydb.cursor()

    mycursor.execute(
        "CREATE TABLE aggregated_transaction (State VARCHAR(255), State_code INT, Year INT, Quarter INT, Transacion_type VARCHAR(255), Transacion_count INT, Transacion_amount FLOAT)")

    for item in dict_list:
        sql = "INSERT INTO aggregated_transaction (State, State_code, Year, Quarter, Transacion_type, Transacion_count, Transacion_amount) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = item['State'], item['State_code'], item['Year'], item['Quarter'], item['Transacion_type'], item['Transacion_count'], item['Transacion_amount']
        mycursor.execute(sql, val)

    mydb.commit()
    mycursor.close()
    mydb.close()


data_preprocessing()


def data_preprocessing1():  # reading csv file and importing to MySQL database.
    dict_list = list()

    with open('aggregated_user.csv', 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        next(csv_reader)
        for rows in csv_reader:
            dict_list.append({'State': rows[1], 'State_code': rows[2], 'Year': rows[3], 'Quarter': rows[4], 'Usersby_device': rows[5],
                              'Transacion_count': rows[6], 'percentage': rows[7]})

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Baskar@123",
        database="phonepe_pulse")

    mycursor = mydb.cursor()

    mycursor.execute(
        "CREATE TABLE aggregated_user (State VARCHAR(255), State_code INT, Year INT, Quarter INT, Usersby_device VARCHAR(255), Transacion_count INT, percentage FLOAT)")

    for item in dict_list:
        sql = "INSERT INTO aggregated_user (State, State_code, Year, Quarter, Usersby_device, Transacion_count, percentage) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = item['State'], item['State_code'], item['Year'], item['Quarter'], item['Usersby_device'], item['Transacion_count'], item[
            'percentage']
        mycursor.execute(sql, val)

    mydb.commit()
    mycursor.close()
    mydb.close()


data_preprocessing1()


def data_preprocessing2():  # reading csv file and importing to MySQL database.
    dict_list = list()

    with open('map_transaction.csv', 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        next(csv_reader)
        for rows in csv_reader:
            dict_list.append({'State': rows[1], 'State_code': rows[2], 'Year': rows[3], 'Quarter': rows[4], 'Usersby_region': rows[5],
                              'Transacion_count': rows[6], 'Transacion_amount': rows[7]})

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Baskar@123",
        database="phonepe_pulse")

    mycursor = mydb.cursor()

    mycursor.execute(
        "CREATE TABLE map_transaction (State VARCHAR(255), State_code INT, Year INT, Quarter INT, Usersby_region VARCHAR(255), Transacion_count INT, Transacion_amount FLOAT)")

    for item in dict_list:
        sql = "INSERT INTO map_transaction (State, State_code, Year, Quarter, Usersby_region, Transacion_count, Transacion_amount) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = item['State'], item['State_code'], item['Year'], item['Quarter'], item['Usersby_region'], item['Transacion_count'], item['Transacion_amount']
        mycursor.execute(sql, val)

    mydb.commit()
    mycursor.close()
    mydb.close()


data_preprocessing2()


def data_preprocessing3():  # reading csv file and importing to MySQL database.
    dict_list = list()

    with open('map_user.csv', 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        next(csv_reader)
        for rows in csv_reader:
            dict_list.append({'State': rows[1], 'State_code': rows[2], 'Year': rows[3], 'Quarter': rows[4], 'district': rows[5],
                              'registeredUsers': rows[6], 'appOpens': rows[7]})

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Baskar@123",
        database="phonepe_pulse")

    mycursor = mydb.cursor()

    mycursor.execute(
        "CREATE TABLE map_user (State VARCHAR(255), State_code INT, Year INT, Quarter INT, district VARCHAR(255), registeredUsers INT, appOpens INT)")

    for item in dict_list:
        sql = "INSERT INTO map_user (State, State_code, Year, Quarter, district, registeredUsers, appOpens) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = item['State'], item['State_code'], item['Year'], item['Quarter'], item['district'], item['registeredUsers'], item['appOpens']
        mycursor.execute(sql, val)

    mydb.commit()
    mycursor.close()
    mydb.close()


data_preprocessing3()


def data_preprocessing4():  # reading csv file and importing to MySQL database.

    dict_list = list()

    with open('top_transaction.csv', 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        next(csv_reader)
        for rows in csv_reader:
            dict_list.append({'State': rows[1], 'State_code': rows[2], 'Year': rows[3], 'Quarter': rows[4], 'entityName_pincodes': rows[5],
                              'Transacion_count': rows[6], 'Transacion_amount': rows[7]})

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Baskar@123",
        database="phonepe_pulse")

    mycursor = mydb.cursor()

    mycursor.execute(
        "CREATE TABLE top_transaction (State VARCHAR(255), State_code INT, Year INT, Quarter INT, entityName_pincodes VARCHAR(500), Transacion_count INT, Transacion_amount FLOAT)")

    for item in dict_list:
        sql = "INSERT INTO top_transaction (State, State_code, Year, Quarter, entityName_pincodes, Transacion_count, Transacion_amount) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = item['State'], item['State_code'], item['Year'], item['Quarter'], item['entityName_pincodes'], item['Transacion_count'], item[
            'Transacion_amount']
        mycursor.execute(sql, val)

    mydb.commit()
    mycursor.close()
    mydb.close()


data_preprocessing4()


def data_preprocessing5():  # reading csv file and importing to MySQL database.
    dict_list = list()

    with open('top_user.csv', 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        next(csv_reader)
        for rows in csv_reader:
            dict_list.append({'State': rows[1], 'State_code': rows[2], 'Year': rows[3], 'Quarter': rows[4], 'district_pincodes': rows[5],
                              'registeredUsers': rows[6]})

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Baskar@123",
        database="phonepe_pulse")

    mycursor = mydb.cursor()

    mycursor.execute(
        "CREATE TABLE top_user (State VARCHAR(255), State_code INT, Year INT, Quarter INT, district_pincodes VARCHAR(500), registeredUsers INT)")

    for item in dict_list:
        sql = "INSERT INTO top_user (State, State_code, Year, Quarter, district_pincodes, registeredUsers) VALUES (%s, %s, %s, %s, %s, %s)"
        val = item['State'], item['State_code'], item['Year'], item['Quarter'], item['district_pincodes'], item['registeredUsers']
        mycursor.execute(sql, val)

    mydb.commit()
    mycursor.close()
    mydb.close()


data_preprocessing5()
