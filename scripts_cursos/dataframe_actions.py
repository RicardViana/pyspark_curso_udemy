# ==============================================================================
# Script de Estudo: Ações e Transformações em DataFrames
# ==============================================================================

import sys
import os
sys.stdout.reconfigure(encoding='utf-8')

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, desc
import pyspark.sql.functions as Func

# 0. Preparação de Dados de Exemplo
os.makedirs("dados", exist_ok=True)
caminho_csv = "dados/despachantes.csv"
if not os.path.exists(caminho_csv):
    with open(caminho_csv, "w", encoding="utf-8") as f:
        f.write("1,Carmem,Ativo,São Paulo,23,2020-08-11\n")
        f.write("2,Deolinda Vilela,Ativo,Campinas,34,2020-03-05\n")
        f.write("3,Fábio,Inativo,Rio de Janeiro,12,2020-07-22\n")
        f.write("4,Gervásio,Ativo,Belo Horizonte,45,2020-01-15\n")
        f.write("5,Ana,Ativo,São Paulo,50,2020-09-01\n")

print("\nIniciando a sessão do Spark...\n")
spark = SparkSession.builder.appName("AcoesTransformacoes").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Importar os dados
arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
despachantes = spark.read.csv(caminho_csv, header=False, schema=arqschema)

print("\n" + "="*50)
print(" PARTE 1: FORMATOS DE EXIBIÇÃO E AÇÕES")
print("="*50)

print("Formato Tabular (show 1 linha):")
despachantes.show(1)

print("Formato de Lista (take 1 linha - retorna objeto Row):")
# No terminal interativo aparece direto, no script .py usamos print()
print(despachantes.take(1)) 

print(f"\nTotal de registros (count): {despachantes.count()}")

print("\nRetornando todos os dados como lista (collect):")
todos_dados = despachantes.collect()
for linha in todos_dados:
    print(f"ID: {linha['id']}, Nome: {linha['nome']}")

print("\n" + "="*50)
print(" PARTE 2: ORDENAÇÃO (ORDER BY)")
print("="*50)

print("Ordenação Padrão Crescente (vendas):")
despachantes.orderBy("vendas").show()

print("Ordenação Decrescente (vendas):")
# Usando Func.col().desc()
despachantes.orderBy(Func.col("vendas").desc()).show()

print("Ordenação Múltipla (Cidade DESC e Vendas DESC):")
despachantes.orderBy(Func.col("cidade").desc(), Func.col("vendas").desc()).show()

print("\n" + "="*50)
print(" PARTE 3: AGRUPAMENTO E FILTRO")
print("="*50)

print("Vendas por Cidade (Agrupado):")
despachantes.groupBy("cidade").agg(sum("vendas")).show()

print("Vendas por Cidade (Ordenado por valor decrescente):")
# Corrigido: adicionado o ponto antes do show()
despachantes.groupBy("cidade") \
    .agg(sum("vendas")) \
    .orderBy(Func.col("sum(vendas)").desc()) \
    .show()

print("Filtrando por nome específico (Deolinda Vilela):")
despachantes.filter(Func.col("nome") == "Deolinda").show()

print("\nDesligando o motor do Spark...")
spark.stop()