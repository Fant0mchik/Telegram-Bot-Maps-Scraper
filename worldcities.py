import pandas as pd
import os
import datetime

pd.set_option('display.max_rows', None)
df = pd.read_csv('worldcities.csv') 

def filter_cities_by_state_and_population(state, min_population):
    filtered = df[(df['admin_name'] == state) & (df['population'] >= min_population)]
    result =  filtered.sort_values('population', ascending=False)[['city', 'population']]
    filename = f"{state}_{min_population}_{datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.csv"
    result.to_csv(os.path.join('logs', filename), index=False, encoding='utf-8')
    return filename