# Import de bibliotecas
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Inicializa a sessão
spark = SparkSession.builder.appName("JoinParquet").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Caminho base no WSL
caminho = "/home/ricar/pyspark_udemy/apoio/Spark/download/Atividades"

# Carregar os DataFrames específicos
df_clientes = spark.read.parquet(f"{caminho}/Clientes.parquet")
df_itens_vendas = spark.read.parquet(f"{caminho}/ItensVendas.parquet")
df_produtos = spark.read.parquet(f"{caminho}/Produtos.parquet")
df_vendas = spark.read.parquet(f"{caminho}/Vendas.parquet")
df_vendedores = spark.read.parquet(f"{caminho}/Vendedores.parquet")

print()

# Atividades
print("Atividades")

# 1) Crie um banco de dados no DW do Spark chamado VendasVarejo, e persista todas as tabelas neste banco de dados

# 2) Crie uma consulta que mostre de cada item vendido: Nome do Cliente, Data da Venda,Produto, Vendedor e Valor Total do item

print("\nAtividades concluídas com sucesso!")
spark.stop()