import json
import os
import requests

from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, round
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

def set_file_path(root_dir, config_file, file_num):
    return os.path.join(root_dir, 'input_files', config_file['INPUTS'][file_num]['FILE'])

def load_js_file(path):
    return json.load(open(path, mode = 'r', encoding = 'utf-8'))

def get_schema(config_file, file_num):
    return StructType.fromJson(json.loads(json.dumps(config_file['INPUTS'][file_num]['SCHEMA'])))

def get_seller_row(json):
    return (json['body']['site_id'], json['body']['id'], json['body']['nickname'], json['body']['points'])

def get_mpe_row(json):
    return (json['id'], json['sold_quantity'], json['available_quantity'])

def build_df_from_csv(path, schema):
    return spark.read.csv(path, header = True, sep = ',', schema = schema)

def build_df_from_js(js_data, schema, function):
    values = [function(elem) for elem in js_data]
    return spark.createDataFrame(values, schema)

def process_csv_to_df(root_dir, config_file, file_num):
    path = set_file_path(root_dir, config_file, file_num)
    #print('path: ', path)
    schema = get_schema(config_file, file_num)
    #print('Leyendo esquema.')
    df = build_df_from_csv(path, schema)
    return df

def process_js_to_df(root_dir, config_file, file_num, function):
    path = set_file_path(root_dir, config_file, file_num)
    #print('path: ', path)
    js_data = load_js_file(path)
    #print('Leyendo el archivo.')
    schema = get_schema(config_file, file_num)
    #print('Leyendo esquema.')
    if file_num == 1: 
        js_data = js_data['results']
    df = build_df_from_js(js_data, schema, function)
    return df

def build_path(sub_dir):
    year, month, day = str(date.today()).split('-')
    date_dir = os.path.join(parent_dir, 'out_files', year, month, day)
    return os.path.join(date_dir, sub_dir)

#Inicio Desafio N°8: Paths 1
def generateMonthlyPathList(year, month, day):
    url_list = []
    url = 'https://importantdata@location/{}/{}/{}/'
    
    for i in range(int(day)):
        url_list.append(url.format(year, month, i + 1))
    
    return url_list
#Fin Desafio N°8

#Inicio Desafio N°9: Paths 2
def generateLastDaysPaths(date, days):
    url_list = []
    url = 'https://importantdata@location/{}/{}/{}/'
    
    my_date = datetime.strptime(date, '%Y%m%d').date()
    
    for i in range(days):
        my_lower_date = my_date - timedelta(days = i)
        
        year = my_lower_date.year
        month = my_lower_date.month
        day = my_lower_date.day
        url_list.append(url.format(year, month, day))
    
    return url_list[::-1]
#Fin Desafio N°9

spark = SparkSession.builder.appName("Nubi").getOrCreate()
sc = spark.sparkContext

current_path = os.getcwd()
#print('current_path: ', current_path)

parent_dir = os.path.dirname(current_path)
#print('parent_dir: ', parent_dir)

config_file_path = os.path.join(current_path, 'config_file.json')
#print('config_file_path: ', config_file_path)

config_file = json.load(open(config_file_path, mode = 'r', encoding = 'utf-8'))
#print('Leyendo archivo de configuracion.')

#Inicio Desafio N°3: Deserializar un JSON
df_sellers = process_js_to_df(parent_dir, config_file, 0, get_seller_row)

df_sellers_pos = df_sellers.filter(df_sellers.puntos > 0)

df_sellers_neg = df_sellers.filter(df_sellers.puntos < 0)

df_sellers_neu = df_sellers.filter(df_sellers.puntos == 0)

pos_dir = build_path('positivo')
neg_dir = build_path('negativo')
neu_dir = build_path('neutro')

df_sellers_pos.repartition(1).write.format('csv').mode('overwrite').save(pos_dir)
df_sellers_neg.repartition(1).write.format('csv').mode('overwrite').save(neg_dir)
df_sellers_neu.repartition(1).write.format('csv').mode('overwrite').save(neu_dir)
#Fin Desafio N°3 

#Inicio Desafio N°4: Parseo de un Array de Structs en un dataframe
df_mpe = process_js_to_df(parent_dir, config_file, 1, get_mpe_row)

df_mpe.createOrReplaceTempView('df_mpe')
df_mpe = spark.sql('SELECT ROW_NUMBER() OVER (ORDER BY item_id) AS id, * FROM df_mpe')
#Fin Desafio N°4

#Inicio Desafio N°5: Agregar las visitas al DataFrame con datos de ventas
df_visit = process_csv_to_df(parent_dir, config_file, 2)

df_mpe_sold = df_mpe.filter(df_mpe.sold_quantity > 0)

df_joined = df_mpe_sold.join(df_visit, df_mpe_sold.item_id == df_visit.item_id, 'inner'). \
            select(df_mpe_sold.item_id, df_mpe_sold.sold_quantity, df_visit.visit)
#Fin Desafio N°5

#Inicio Desafio N°6: Agregar métricas a un DataFrame
df_rate = df_joined.withColumn("conversion_rate", round(df_joined.sold_quantity / df_joined.visit, 4))

df_rate.createOrReplaceTempView('df_rate')
df_rate = spark.sql('SELECT *, ROW_NUMBER() OVER (ORDER BY conversion_rate DESC) AS conversion_ranking FROM df_rate')
#Fin Desafio N°6

#Inicio Desafio N°7: Porcentaje de Stock
row_total_stock = df_mpe.groupBy().sum('available_quantity').collect()
total_stock = row_total_stock[0]['sum(available_quantity)']

df_stock = df_mpe.withColumn("stock_percentage", df_mpe.available_quantity * 100 / total_stock)
#Fin Desafio N°7

#Inicio Desafio N°2: Interactuar con la API de Mercado Libre
meli_url = 'https://api.mercadolibre.com/sites/MLA/search?category=MLA1000'

r = requests.get(meli_url)
response = r.json()

df_meli_js = spark.read.json(sc.parallelize([response]))

dir_name = 'search' + 'json' + str(date.today().year) + str(date.today().month)

df_meli_js.repartition(1).write.format('json').mode('overwrite').save(os.path.join(parent_dir, 'out_files', dir_name))
#Fin Desafio N°2