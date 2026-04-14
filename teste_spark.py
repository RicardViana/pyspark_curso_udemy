# 1. Importa e inicializa o findspark
# Isso ajuda o Python a encontrar os arquivos do PySpark na sua máquina
import findspark
findspark.init()

# 2. Importa o módulo que cria a sessão do Spark
from pyspark.sql import SparkSession

print("\nIniciando a sessão do Spark (isso pode levar alguns segundos na primeira vez)...")

# 3. Cria a sessão do Spark (É neste momento que o Python liga o motor do Java)
spark = SparkSession.builder \
    .appName("Teste_Ambiente_Local") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 4. Se chegou até aqui sem erros, deu tudo certo! Vamos imprimir a versão.
print("\n" + "="*50)
print(f"🚀 SUCESSO! O motor ligou. Versão do Spark: {spark.version}")
print("="*50 + "\n")

# 5. Boa prática: desligar o motor ao final do script
spark.stop()