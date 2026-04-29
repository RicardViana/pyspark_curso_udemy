import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name

# Inicializa a sessão
spark = SparkSession.builder.appName("InspecaoIndividual").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Caminho dentro do Linux (WSL)
caminho_base = "/home/ricar/pyspark_udemy/apoio/Spark/download/Atividades"

# Listar os arquivos que terminam com .parquet
arquivos = [f for f in os.listdir(caminho_base) if f.endswith('.parquet')]

print(f"Encontrados {len(arquivos)} arquivos Parquet.\n")

# Loop para ler e fazer o select em cada um
for nome_arquivo in arquivos:
    caminho_completo = os.path.join(caminho_base, nome_arquivo)
    
    print(f"--- Lendo arquivo: {nome_arquivo} ---")
    
    # Lê o arquivo individualmente
    df_temp = spark.read.parquet(caminho_completo)
    
    # Faz o select e mostrar apenas as 5 primeiras linhas
    df_temp.select("*").show(5) 
    
    print(f"Esquema de {nome_arquivo}:")
    df_temp.printSchema()
    print("-" * 50)

print("\nInspeção concluída!")
spark.stop()