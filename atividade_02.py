# Import de bibliotecas
import os
import sys
import shutil
sys.stdout.reconfigure(encoding='utf-8')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Limpeza do ambiente
caminho_warehouse = f"{os.getcwd()}/spark-warehouse"
if os.path.exists(caminho_warehouse):
    shutil.rmtree(caminho_warehouse)

# Inicializar a sessão e ativando a permissão de pastas não vazias por segurança
spark = SparkSession.builder \
    .appName("JoinParquet") \
    .config("spark.sql.warehouse.dir", caminho_warehouse) \
    .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true") \
    .getOrCreate()
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

# 1) Criar um banco de dados no DW do Spark chamado VendasVarejo, e persista todas as tabelas neste banco de dados
print("\nExecutando Atividade 1: Criando Banco e Persistindo Tabelas ")

# Criar o banco de dados caso ele não exista e definir como o banco atual
spark.sql("CREATE DATABASE IF NOT EXISTS VendasVarejo")
spark.sql("USE VendasVarejo")

# Persistir as 5 tabelas de forma gerenciada e limpando se já existirem
spark.sql("DROP TABLE IF EXISTS Clientes")
df_clientes.write.mode("overwrite").saveAsTable("Clientes")

spark.sql("DROP TABLE IF EXISTS ItensVendas")
df_itens_vendas.write.mode("overwrite").saveAsTable("ItensVendas")

spark.sql("DROP TABLE IF EXISTS Produtos")
df_produtos.write.mode("overwrite").saveAsTable("Produtos")

spark.sql("DROP TABLE IF EXISTS Vendas")
df_vendas.write.mode("overwrite").saveAsTable("Vendas")

spark.sql("DROP TABLE IF EXISTS Vendedores")
df_vendedores.write.mode("overwrite").saveAsTable("Vendedores")

# Mostra as tabelas criadas no banco de dados para conferência
print("Tabelas persistidas com sucesso no banco VendasVarejo:")
spark.sql("SHOW TABLES").show()

# Consultar as tabelas criadas no banco de dados
print("Amostra de Dados das Tabelas (Top 5 linhas)")

print("Tabela: Clientes")
spark.sql("SELECT * FROM Clientes").show(5)
print()

print("Tabela: Vendedores")
spark.sql("SELECT * FROM Vendedores").show(5)
print()

print("Tabela: Produtos")
spark.sql("SELECT * FROM Produtos").show(5)
print()

print("Tabela: Vendas")
spark.sql("SELECT * FROM Vendas").show(5)
print()

print("Tabela: ItensVendas")
spark.sql("SELECT * FROM ItensVendas").show(5)

# 2) Crie uma consulta que mostre de cada item vendido: Nome do Cliente, Data da Venda, Produto, Vendedor e Valor Total do item
print("\nExecutando Atividade 2: Consulta Analítica de Itens Vendidos")

# Tabela base para análise é a 'ItensVendas'
# Usar os dados dela e relacionar com as outras tabelas usando Joins encadeados
# Nota técnica: Usado strings nos argumentos "on" para o Spark remover as colunas duplicadas automaticamente

df_consulta_final = df_itens_vendas \
    .join(df_vendas, "VendasID", "inner") \
    .join(df_clientes, "ClienteID", "inner") \
    .join(df_produtos, "ProdutoID", "inner") \
    .join(df_vendedores, "VendedorID", "inner")

# Selecionar os campos pedidos necessario
df_relatorio = df_consulta_final.select(
    col("Cliente").alias("Nome do Cliente"),
    col("Data").alias("Data da Venda"),
    col("Produto"),
    col("Vendedor"),
    col("Total").alias("Valor Total do Item")
)

# Visualizar o resultado
df_relatorio.show(10, truncate=False)

print("\nAtividades concluídas com sucesso!")
spark.stop()