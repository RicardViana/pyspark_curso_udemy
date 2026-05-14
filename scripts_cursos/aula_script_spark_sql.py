# Script de Estudo: Spark SQL, Bancos de Dados, Tabelas e Views
# Baseado na aula do Prof. Fernando Amaral

import os
import sys
import shutil
sys.stdout.reconfigure(encoding='utf-8')

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as Func
from pyspark.sql.functions import sum

# PREPARAÇÃO DO AMBIENTE (Dados de Exemplo e Limpeza)
os.makedirs("dados", exist_ok=True)

arq_despachantes = "dados/despachantes.csv"
if not os.path.exists(arq_despachantes):
    with open(arq_despachantes, "w", encoding="utf-8") as f:
        f.write("1,Carmem,Ativo,São Paulo,23,2020-08-11\n")
        f.write("2,Deolinda,Ativo,Campinas,34,2020-03-05\n")
        f.write("3,Fábio,Inativo,Rio de Janeiro,12,2020-07-22\n")

arq_reclamacoes = "dados/reclamacoes.csv"
with open(arq_reclamacoes, "w", encoding="utf-8") as f:
    f.write("101,2020-09-01,1\n") 
    f.write("102,2020-09-05,2\n")

caminho_warehouse = f"{os.getcwd()}/spark-warehouse"
if os.path.exists(caminho_warehouse):
    shutil.rmtree(caminho_warehouse)

spark = SparkSession.builder \
    .appName("Estudo_SparkSQL") \
    .config("spark.sql.warehouse.dir", caminho_warehouse) \
    .getOrCreate()

# PARTE I - BANCOS DE DADOS E TABELAS
print("\n--- Mostrar bancos de dados e tabelas ---")
spark.sql("show databases").show()

print("--- Criar banco de dados ---")
spark.sql("create database desp")
spark.sql("use desp")

print("--- Criar tabela gerenciada ---")
arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
despachantes = spark.read.csv(arq_despachantes, header=False, schema=arqschema)
despachantes.write.saveAsTable("Despachantes")

print("--- Mostrar que a tabela existe ---")
spark.sql("select * from despachantes").show()

print("--- Mostra tabela ---")
spark.sql("show tables").show()

print("--- Mudar banco de dados ---")
spark.sql("use default")

print("--- Executa novamente e mostrar que da erro ---")
spark.sparkContext.setLogLevel("FATAL") # Mutando o log gigante do Java
try:
    spark.sql("select * from despachantes").show()
except Exception as e:
    print("--> ERRO INTENCIONAL CAPTURADO: Tabela não existe no banco default.\n")
spark.sparkContext.setLogLevel("WARN")

print("--- Voltar ao nosso banco de dados ---")
spark.sql("use desp")

print("--- Overwrite e append ---")
spark.sql("DROP TABLE IF EXISTS Despachantes") # Usando DROP TABLE para garantir idempotência no código local
despachantes.write.mode("overwrite").saveAsTable("Despachantes")

print("--- Teste de persistência ---")
spark.sql("use desp")
spark.sql("select * from despachantes").show()
despachantes.show()

print("--- O resultado de uma consulta sem um show gera um dataframe ---")
despachantes = spark.sql("select * from despachantes")
despachantes.show()

print("--- Criar tabela não gerenciada ---")
pasta_parquet = "dados/desparquet"
despachantes.write.mode("overwrite").format("parquet").save(pasta_parquet)
caminho_absoluto = f"{os.getcwd()}/{pasta_parquet}"
spark.sql("DROP TABLE IF EXISTS Despachantes_ng")
despachantes.write.mode("overwrite").option("path", caminho_absoluto).saveAsTable("Despachantes_ng")

print("--- Como saber se uma tabela é gerenciada ou não? ---")
spark.sql("show create table Despachantes").show(truncate=False)
spark.sql("show create table Despachantes_ng").show(truncate=False)

print("--- Outra forma: spark.catalog.listTables() ---")
print(spark.catalog.listTables())

# PARTE II - VIEWS
print("\n--- Criando Views ---")
despachantes.createOrReplaceTempView("Despachantes")
despachantes.createOrReplaceGlobalTempView("Despachantes")

# PARTE III - CONSULTAS
print("\n--- Mostrar a tabela ---")
spark.sql("Select * from Despachantes").show()
despachantes.show()

print("--- Mostrar certas colunas ---")
spark.sql("Select nome,vendas from Despachantes").show()
despachantes.select("nome","vendas").show()

print("--- Condição lógica ---")
spark.sql("Select nome,vendas from Despachantes where vendas > 20").show()
despachantes.select("id","nome","vendas").where(Func.col("vendas") > 20).show()

print("--- Agrupamento ---")
spark.sql("Select cidade,sum(vendas) from Despachantes group by cidade order by 2 desc").show()
despachantes.groupBy("cidade").agg(sum("vendas")).orderBy(Func.col("sum(vendas)").desc()).show()

# PARTE IV - JOINS
print("\n--- Carregando Reclamações ---")
recschema = "idrec INT, datarec STRING, iddesp INT"
reclamacoes = spark.read.csv(arq_reclamacoes, header=False, schema=recschema)
spark.sql("DROP TABLE IF EXISTS reclamacoes")
reclamacoes.write.saveAsTable("reclamacoes")

print("--- INNER JOIN (SQL) ---")
spark.sql("select reclamacoes.*, despachantes.nome from despachantes inner join reclamacoes on (despachantes.id = reclamacoes.iddesp)").show()

print("--- RIGHT JOIN (SQL) ---")
spark.sql("select reclamacoes.*, despachantes.nome from despachantes right join reclamacoes on (despachantes.id = reclamacoes.iddesp)").show()

print("--- LEFT JOIN (SQL) ---")
spark.sql("select reclamacoes.*, despachantes.nome from despachantes left join reclamacoes on (despachantes.id = reclamacoes.iddesp)").show()

print("--- INNER JOIN (DataFrame) ---")
despachantes.join(reclamacoes, despachantes.id == reclamacoes.iddesp, "inner").select("idrec","datarec","iddesp","nome").show()

print("--- RIGHT JOIN (DataFrame) ---")
despachantes.join(reclamacoes, despachantes.id == reclamacoes.iddesp, "right").select("idrec","datarec","iddesp","nome").show()

print("--- LEFT JOIN (DataFrame) ---")
despachantes.join(reclamacoes, despachantes.id == reclamacoes.iddesp, "left").select("idrec","datarec","iddesp","nome").show()

print("\nDesligando o motor do Spark...")
spark.stop()
