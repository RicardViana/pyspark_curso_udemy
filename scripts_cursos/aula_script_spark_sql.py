
# Script de Estudo: Spark SQL, Bancos de Dados, Tabelas e Views
# Baseado na aula do Prof. Fernando Amaral

import os
import sys
sys.stdout.reconfigure(encoding='utf-8')

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as Func
from pyspark.sql.functions import sum

# Preparação do ambiente (Dados de Exemplo)
os.makedirs("dados", exist_ok=True)

# Garantir que temos o arquivo de despachantes
arq_despachantes = "dados/despachantes.csv"
if not os.path.exists(arq_despachantes):
    with open(arq_despachantes, "w", encoding="utf-8") as f:
        f.write("1,Carmem,Ativo,São Paulo,23,2020-08-11\n")
        f.write("2,Deolinda,Ativo,Campinas,34,2020-03-05\n")
        f.write("3,Fábio,Inativo,Rio de Janeiro,12,2020-07-22\n")

# Criar o arquivo de reclamações (iddesp = id do despachante)
arq_reclamacoes = "dados/reclamacoes.csv"
with open(arq_reclamacoes, "w", encoding="utf-8") as f:
    f.write("101,2020-09-01,1\n") 
    f.write("102,2020-09-05,2\n")

print("Iniciando a sessão do Spark (com suporte a Warehouse)...")
# O Spark cria uma pasta 'spark-warehouse' no seu projeto
spark = SparkSession.builder \
    .appName("Estudo_SparkSQL") \
    .config("spark.sql.warehouse.dir", f"{os.getcwd()}/spark-warehouse") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("\n" + "="*50)
print(" PARTE 1: BANCOS DE DADOS E TABELAS GERENCIADAS")
print("="*50)

print("Bancos de dados iniciais:")
spark.sql("show databases").show()

print("Criando e usando o banco de dados 'desp'...")
spark.sql("create database if not exists desp")
spark.sql("use desp") 

# Ler CSV
arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
despachantes = spark.read.csv(arq_despachantes, header=False, schema=arqschema)

# Salvar como Tabela Gerenciada
print("Salvando Tabela Gerenciada 'Despachantes'...")
despachantes.write.mode("overwrite").saveAsTable("Despachantes")

print("Tabelas no banco 'desp':")
spark.sql("show tables").show()

print("Mudando para o banco 'default' e testando erro intencional...")
spark.sql("use default")

try:
    spark.sql("select * from despachantes").show()
except Exception as e:
    print(f"--> ERRO ESPERADO: A tabela não existe no banco default.\nDetalhe: {str(e).split(':')[0]}")

# Voltar ao banco correto
spark.sql("use desp")

print("\n" + "="*50)
print(" PARTE 2: TABELAS NÃO GERENCIADAS E VIEWS")
print("="*50)

pasta_parquet = "dados/desparquet"
despachantes.write.mode("overwrite").format("parquet").save(pasta_parquet)

# Salvar como Tabela NÃO Gerenciada (Apontando o path)
caminho_absoluto = f"{os.getcwd()}/{pasta_parquet}"
despachantes.write.mode("overwrite").option("path", caminho_absoluto).saveAsTable("Despachantes_ng")

print("Estrutura Tabela Gerenciada (Não mostra caminho externo):")
spark.sql("show create table Despachantes").show(1, truncate=False)

print("Estrutura Tabela Não Gerenciada (Mostra o location externo):")
spark.sql("show create table Despachantes_ng").show(1, truncate=False)

# Criar Views (Tabelas Temporárias que somem ao fechar o script)
despachantes.createOrReplaceTempView("View_Despachantes")

print("\n" + "="*50)
print(" PARTE 3: CONSULTAS (SQL VS DATAFRAME API)")
print("="*50)

print("--- Usando SPARK SQL (Linguagem SQL) ---")
spark.sql("Select cidade, sum(vendas) as total from Despachantes group by cidade order by 2 desc").show()

print("--- Usando DATAFRAME API (Linguagem Python) ---")
despachantes.groupBy("cidade").agg(sum("vendas").alias("total")).orderBy(Func.col("total").desc()).show()

print("\n" + "="*50)
print(" PARTE 4: JOINS (SQL VS DATAFRAME API)")
print("="*50)

recschema = "idrec INT, datarec STRING, iddesp INT"
reclamacoes = spark.read.csv(arq_reclamacoes, header=False, schema=recschema)
reclamacoes.write.mode("overwrite").saveAsTable("reclamacoes")

print("INNER JOIN (Usando SQL):")
spark.sql("""
    SELECT r.*, d.nome 
    FROM despachantes d 
    INNER JOIN reclamacoes r ON (d.id = r.iddesp)
""").show()

print("LEFT JOIN (Usando DataFrame API):")

despachantes.join(reclamacoes, despachantes.id == reclamacoes.iddesp, "left") \
    .select("idrec", "datarec", "iddesp", "nome").show()

print("\nDesligando o motor do Spark...")
spark.stop()