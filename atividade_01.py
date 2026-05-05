# Import de bibliotecas
import pyspark.sql.functions as Func

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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

df_clientes.show(5, truncate=False)
print("Estrutura de Vendas:")
df_clientes.printSchema()

df_itens_vendas.show(5, truncate=False)
print("\nEstrutura de Produtos:")
df_itens_vendas.printSchema()

df_produtos.show(5, truncate=False)
print("Estrutura de Vendas:")
df_produtos.printSchema()

df_vendas.show(5, truncate=False)
print("\nEstrutura de Produtos:")
df_vendas.printSchema()

df_vendedores.show(5, truncate=False)
print("Estrutura de Vendas:")
df_vendedores.printSchema()

# Atividades

# Crie uma consulta que mostre, nesta ordem, Nome, Estado e Status
print("\nCrie uma consulta que mostre, nesta ordem, Nome, Estado e Status")

print("\nSolução 1:")
colunas = ["Cliente", "Estado", "Status"]
df_clientes.select(*colunas).show(5)

print("\nSolução 2:")
df_clientes.select(
    col("Cliente").alias("Nome"),      
    col("Estado"),
    col("Status")
).show(5)

# Crie uma consulta que mostre apenas os clientes do Status "platiun" e "gold"
print("\nCrie uma consulta que mostre apenas os clientes do Status ""platiun"" e ""gold""")
df_clientes.filter(col("Status").isin("Platinum", "Gold")).show()

# Demostre quanto cada Status de Clientes representa em vendas
print("Demostre quanto cada Status de Clientes representa em vendas")

# Vamos supor que a chave comum se chama "id_produto"
# 'inner' é o padrão (mantém apenas o que existe em ambos)
df_resultado = df_clientes.join(df_vendas, df_clientes.ClienteID == df_vendas.ClienteID, "inner")

df_resultado.show(5, truncate=False)

#print("\nResultado do Join (Primeiras 10 linhas):")
#df_resultado.select(
    #df_vendas.id_venda, 
    #df_vendas.data, 
    #df_produtos.nome_produto, 
    #df_produtos.preco
#).show(10)

print("\nJoin concluído com sucesso!")
spark.stop()