# ==============================================================================
# Script de Estudo: Introdução a DataFrames
# ==============================================================================

import sys
import os
sys.stdout.reconfigure(encoding='utf-8')

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, expr, col, to_timestamp, year
import pyspark.sql.functions as Func
from pyspark.sql.types import *

# ==============================================================================
# 0. PREPARAÇÃO DO AMBIENTE (Criando dados de teste automaticamente)
# ==============================================================================
os.makedirs("dados", exist_ok=True)
caminho_csv = "dados/despachantes.csv"
with open(caminho_csv, "w", encoding="utf-8") as f:
    f.write("1,Carmem,Ativo,São Paulo,23,2020-08-11\n")
    f.write("2,Deolinda,Ativo,Campinas,34,2020-03-05\n")
    f.write("3,Fábio,Inativo,Rio de Janeiro,12,2020-07-22\n")
    f.write("4,Gervásio,Ativo,Belo Horizonte,45,2020-01-15\n")

print("Iniciando a sessão do Spark...")

spark = SparkSession.builder \
    .appName("Estudo_DataFrames_Fernando") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("\n" + "="*50)
print(" PARTE 1: CRIAÇÃO DE DATAFRAMES SIMPLES")
print("="*50)

# Criar um DataFrame simples, sem schema (O Spark tenta adivinhar)
df1 = spark.createDataFrame([("Pedro", 10), ("Maria", 20), ("José", 40)])
print("DataFrame sem Schema (Note que as colunas viram _1 e _2):")
df1.show()

# Criar DataFrame com schema explicíto
schema = "Id INT, Nome STRING"
dados = [[1, "Pedro"], [2, "Maria"]]
df2 = spark.createDataFrame(dados, schema)
print("DataFrame COM Schema:")
df2.show()
print("Mostrando apenas 1 linha:")
df2.show(1) 

print("\n" + "="*50)
print(" PARTE 2: TRANSFORMAÇÕES E AGRUPAMENTOS")
print("="*50)

schema2 = "Produtos STRING, Vendas INT"
vendas = [["Caneta", 10], ["Lápis", 20], ["Caneta", 40]]
df3 = spark.createDataFrame(vendas, schema2)

# Agrupamento com soma
agrupado = df3.groupBy("Produtos").agg(sum("Vendas"))
print("Agrupado por Produtos (Soma de Vendas):")
agrupado.show()

# Podemos concatenar as operações (encadeamento)
print("Concatenado em uma linha:")
df3.groupBy("Produtos").agg(sum("Vendas")).show()

# Selecionar colunas específicas
print("Selecionando colunas específicas:")
df3.select("Produtos").show()

# Expressões matemáticas dentro do Select
print("Multiplicando Vendas por 0.2 usando expr():")
df3.select("Produtos", "Vendas", expr("Vendas * 0.2")).show()

# Inspecionando o DataFrame
print("Schema do df3:")
print(df3.schema)
print("\nLista de Colunas do df3:")
print(df3.columns)

print("\n" + "="*50)
print(" PARTE 3: IMPORTAÇÃO E INFERÊNCIA DE SCHEMA (CSV)")
print("="*50)

arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"

# Lendo com schema manual
despachantes = spark.read.csv(caminho_csv, header=False, schema=arqschema)
print("CSV Lido com Schema Manual:")
despachantes.show()

# Lendo com inferência automática (load)
desp_autoschema = spark.read.load(caminho_csv, format="csv", sep=",", inferSchema=True, header=False)
print("CSV Lido com Schema Automático (inferSchema=True):")
desp_autoschema.show()

print("Comparando Schemas:")
print("Manual:     ", despachantes.schema)
print("Automático: ", desp_autoschema.schema)

print("\n" + "="*50)
print(" PARTE 4: FILTROS E DATAS")
print("="*50)

# Condição lógica com where
print("Vendas maiores que 20:")
despachantes.select("id", "nome", "vendas").where(col("vendas") > 20).show()

# Múltiplas condições (& = AND)
print("Vendas entre 20 e 40:")
despachantes.select("id", "nome", "vendas").where((col("vendas") > 20) & (col("vendas") < 40)).show()

# Renomear coluna
novodf = despachantes.withColumnRenamed("nome", "nomes")
print(f"Colunas com nome alterado: {novodf.columns}")

# Transformar String em Data (Timestamp)
despachantes2 = despachantes.withColumn("data2", to_timestamp(col("data"), "yyyy-MM-dd"))
print("\nSchema após conversão de Data:")
print(despachantes2.schema)

# Operações sobre datas
print("\nExtraindo apenas o Ano:")
despachantes2.select(year("data2")).show()

print("Contagem de despachantes por Ano:")
despachantes2.select("data2").groupBy(year("data2")).count().show()

print("Soma total de Vendas:")
despachantes2.select(Func.sum("vendas")).show()

print("\n" + "="*50)
print(" PARTE 5: SALVANDO E LENDO FORMATOS DE BIG DATA")
print("="*50)

pasta_base = "dados/output"

# Salvando em vários formatos
print(f"Salvando dados nas pastas dentro de '{pasta_base}'...")
despachantes.write.mode("overwrite").format("parquet").save(f"{pasta_base}/dfimportparquet")
despachantes.write.mode("overwrite").format("csv").save(f"{pasta_base}/dfimportcsv")
despachantes.write.mode("overwrite").format("json").save(f"{pasta_base}/dfimportjson")
despachantes.write.mode("overwrite").format("orc").save(f"{pasta_base}/dfimportorc")

# Lendo Parquet (O Rei do Big Data)
print("\nLendo do Parquet:")
par = spark.read.format("parquet").load(f"{pasta_base}/dfimportparquet")
par.show(2)

# Lendo JSON
print("Lendo do JSON:")
js = spark.read.format("json").load(f"{pasta_base}/dfimportjson")
js.show(2)

# Lendo ORC
print("Lendo do ORC:")
orc_df = spark.read.format("orc").load(f"{pasta_base}/dfimportorc")
orc_df.show(2)

print("\nDesligando o motor do Spark...")
spark.stop()