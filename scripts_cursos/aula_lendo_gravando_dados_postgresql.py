# Import de bibliotecas
import os
import sys
sys.stdout.reconfigure(encoding='utf-8')

from pyspark.sql import SparkSession

# ==============================================================================
# 1. CONFIGURAÇÃO E CONEXÃO
# ==============================================================================

# Caminho absoluto de onde você salvou o arquivo .jar que baixamos agora pouco
# Substitua o nome do arquivo abaixo pela versão exata que você baixou (ex: postgresql-42.7.11.jar)
caminho_driver = "/home/ricar/postgresql-42.7.11.jar" 

# Inicializa a sessão do Spark já injetando o driver JDBC do Postgres
spark = SparkSession.builder \
    .appName("ConexaoPostgres") \
    .config("spark.jars", caminho_driver) \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("ERROR")

# ATENÇÃO: Coloque a senha que você configurou para o usuário 'postgres' no seu Linux
senha_banco = "123456" # <- Mude aqui se a sua senha for diferente

# ==============================================================================
# 2. LENDO DADOS DO POSTGRESQL (EXTRACT)
# ==============================================================================
print("\n--- Conectando e Lendo a Tabela Vendas ---")

# Lendo a tabela 'vendas' do PostgreSQL
resumo = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/vendas") \
    .option("dbtable", "vendas") \
    .option("user", "postgres") \
    .option("password", senha_banco) \
    .option("driver", "org.postgresql.Driver") \
    .load()

# ==============================================================================
# 3. TRANSFORMANDO OS DADOS (TRANSFORM)
# ==============================================================================
print("\n--- Transformando os Dados ---")

# Criando um novo DataFrame apenas com as colunas necessárias (Sem o .show() aqui!)
vendadata = resumo.select("data", "total")

print("Amostra do novo DataFrame:")
vendadata.show(5)

# ==============================================================================
# 4. GRAVANDO DE VOLTA NO BANCO DE DADOS (LOAD)
# ==============================================================================
print("\n--- Gravando a Nova Tabela no PostgreSQL ---")

# Vamos gravar o dataframe reduzido de volta no banco criando uma nova tabela chamada 'vendadata'
vendadata.write.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/vendas") \
    .option("dbtable", "vendadata") \
    .option("user", "postgres") \
    .option("password", senha_banco) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("--> SUCESSO: Dados gravados no PostgreSQL!")

spark.stop()