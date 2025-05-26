import os
import requests
from datetime import datetime
import pandas as pd
import time



""" LOCATIONS = '[[14694,5,"00100, Helsinki"],\
            [14695,5,"00120, Helsinki"],\
            [14696,5,"00130, Helsinki"],\
            [14697,5,"00140, Helsinki"],\
            [14698,5,"00150, Helsinki"],\
            [14699,5,"00160, Helsinki"],\
            [14700,5,"00170, Helsinki"],\
            [14701,5,"00180, Helsinki"],\
            [5079889,5,"00220, Helsinki"],\
            [14705,5,"00250, Helsinki"],\
            [14706,5,"00260, Helsinki"],\
            [14709,5,"00290, Helsinki"],\
            [14725,5,"00500, Helsinki"],\
            [14726,5,"00510, Helsinki"],\
            [14728,5,"00530, Helsinki"],\
            [5079937,5,"00540, Helsinki"],\
            [14729,5,"00550, Helsinki"],\
            [14732,5,"00580, Helsinki"]]' """


LOCATIONS = '[[64,6,"Helsinki"]]'

PARAMS = {'cardType':100,# cardType 101 for rentals, 100 for sale
          'limit':5000,
          'locations':LOCATIONS,
          'offset':0,
          'roomCount[]':[3,4],
          'habitationType[]':[1],
          'sortBy':"published_sort_desc"}

URL = "https://asunnot.oikotie.fi/vuokra-asunnot"
API_URL = "https://asunnot.oikotie.fi/api/cards"



def get_headers():
    r = requests.get(url=URL)
    for r in r.text.split('\n'):
        if (r[:30] == '<meta name="api-token" content'):
            token = (r[32:-2])
        if (r[:28] == '<meta name="loaded" content='):
            loaded = r[29:-2]
        if (r[:26] == '<meta name="cuid" content='):
            cuid = r[27:-2]
    headers = {"OTA-cuid":cuid, "OTA-loaded":loaded, "OTA-token":token}
    return headers


def request_data(headers):
    r = requests.get(url=API_URL, params=PARAMS, headers=headers)
    data=r.json()
    return data

def create_datalist(data):
    fields = ["url","rooms","roomConfiguration","price","published","size","latitude","longitude","coordinates","buildingData"] # URL, huoneet, otsikko, hinta, päivämäärä, pinta-ala ja buildingData, jonka sisältö määritellään if-lauseessa
    datalist = []
    for i in data['cards']:
        row = []
        for j in i:
            if j in fields:
                if (j == "buildingData"):
                    row.append(i[j]['address'])
                    row.append(i[j]['district'])
                    row.append(i[j]['city'])
                    row.append(i[j]['year'])
                elif (j == "coordinates"):
                    row.append(i[j]['latitude'])
                    row.append(i[j]['longitude'])
                else:
                    row.append(i[j])
        datalist.append(row)
    return datalist

def create_dataframe(datalist):
    df = pd.DataFrame(datalist, columns = ['url', 'rooms', 'roomConfiguration', 'price', 'published', 'size', 'address', 'district', 'city', 'buildYear', 'latitude', 'longitude'])
    df['objects_count'] = len(df)  # Nueva columna añadida aquí
    return df




def calculate_persqm(df):
    df['price'] = df['price'].replace(to_replace = "[^0-9]", value = "", regex = True)
    df['price'] = df['price'].apply(pd.to_numeric)
    df['perSquareMetre'] = df['price']/df['size']
    return df

def calculate_quintile(df):
    df['quintile'] = pd.qcut(df['perSquareMetre'], 5, labels=False)
    return df

def calculate_mean_rent(df):
    mean_rent = df['price'].mean() # For later implementations
    return mean_rent

def save_to_csv(obects_count,mean_price, min_price, max_price, daily_variation=None, annualized_variation=None):
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    filename_csv = "historical_data.csv"
    

    
    # Actualizar CSV
    new_data = pd.DataFrame([[current_date,obects_count, mean_price, min_price, max_price, daily_variation, annualized_variation]],
                          columns=['Fecha','Objects', 'Promedio', 'Mínimo', 'Máximo', 'Variación Diaria (%)', 'Variación Anualizada (%)'])
    
    if os.path.exists(filename_csv):
        new_data.to_csv(filename_csv, mode='a', header=False, index=False)
    else:
        new_data.to_csv(filename_csv, mode='w', index=False)

# Bloque principal modificado
headers = get_headers()
data = request_data(headers)
datalist = create_datalist(data)
df = create_dataframe(datalist)

df['price_clean'] = df['price'].str.replace(r'[^\d]', '', regex=True).astype(float)

# Calcular valores
mean_price = round(df['price_clean'].mean(), 2)
min_price = round(df['price_clean'].min(), 2)
max_price = round(df['price_clean'].max(), 2)

# Calcular variaciones
daily_variation = None
annualized_variation = None
try:
    historical = pd.read_csv("historical_data.csv", parse_dates=['Fecha'])
    if not historical.empty:
        last_record = historical.iloc[-1]
        last_mean = last_record['Promedio']
        last_date = last_record['Fecha']
        
        # Calcular días entre registros
        current_date = datetime.now()
        days_diff = (current_date - last_date).days
        days_diff = max(days_diff, 1)  # Evitar división por cero
        
        # Variación diaria
        daily_variation = round(((mean_price - last_mean) / last_mean * 100), 2)
        
        # Variación anualizada precisa
        if last_mean != 0:
            growth_factor = mean_price / last_mean
            annualized_variation = round((growth_factor ** (365 / days_diff) - 1) * 100, 2) 

except (FileNotFoundError, KeyError, IndexError) as e:
    pass

# Resultados en consola
while True:
    time.sleep(3600*24)
    print("\n" + "="*50)
    print(f"Análisis diario - {datetime.now().strftime('%d/%m/%Y')}")
    print("="*50)
    print(f"• Precio promedio actual: €{mean_price:.2f}")
    print(f"• Rango de precios: €{min_price:.2f} - €{max_price:.2f}")
    if daily_variation is not None:
        print(f"\n● Variación desde último registro: {daily_variation:+.2f}%")
        print(f"● Tasa anualizada equivalente: {annualized_variation:+.2f}%")
    else:
        print("\n⚠️ Primera ejecución - Sin datos históricos para comparar")
    print("="*50 + "\n")

    # Guardar en archivo
    save_to_csv(len(df),mean_price, min_price, max_price, daily_variation, annualized_variation)