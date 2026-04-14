# ==============================================================================
# Script de Estudo: Operações com RDDs
# ==============================================================================

import sys
sys.stdout.reconfigure(encoding='utf-8')

import findspark
findspark.init()

from pyspark.sql import SparkSession

print("\nIniciando a sessão do Spark...\n")

# 1. Ligar o motor do Spark
spark = SparkSession.builder \
    .appName("Estudo_RDDs_Prof_Fernando") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Criar a variável SparkContext (sc)
sc = spark.sparkContext

print("\n" + "="*50)
print(" PARTE 1: CRIAÇÃO E AÇÕES BÁSICAS")
print("="*50)

# Criar RDD
numeros = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

# Diversas Ações 
print("Take (Pega os 5 primeiros):", numeros.take(5))
print("Top (Pega os 5 maiores):  ", numeros.top(5))
print("Collect (Todos os elementos):", numeros.collect())

print("\n" + "="*50)
print(" PARTE 2: OPERAÇÕES ARITMÉTICAS")
print("="*50)

print("Count (Contagem):", numeros.count())
print("Mean  (Média):   ", numeros.mean())
print("Sum   (Soma):    ", numeros.sum())
print("Max   (Máximo):  ", numeros.max())
print("Min   (Mínimo):  ", numeros.min())
print("Stdev (Desvio Padrão):", round(numeros.stdev(), 2))

print("\n" + "="*50)
print(" PARTE 3: TRANSFORMAÇÕES")
print("="*50)

# Filtro
filtro = numeros.filter(lambda x: x > 2)
print("Filtro (> 2):           ", filtro.collect())

# Amostra com reposição (True = com reposição, 0.5 = 50% dos dados, 1 = semente aleatória)
amostra = numeros.sample(True, 0.5, 1)
print("Amostra (50% reposição):", amostra.collect())

# Map, aplica uma função matemática a cada elemento
mapa = numeros.map(lambda x: x * 2)
print("Map (Elementos * 2):    ", mapa.collect())

print("\n" + "="*50)
print(" PARTE 4: OPERAÇÕES ENTRE 2 RDDs")
print("="*50)

# Criamos outro RDD
numeros2 = sc.parallelize([6, 7, 8, 9, 10])

# Operador Union - gera rdd com os 2 conjuntos
uniao = numeros.union(numeros2)
print("Union (Une os RDDs):     ", uniao.collect())

# Intersecção
interseccao = numeros.intersection(numeros2)
print("Intersection (Comuns):   ", interseccao.collect())

# Subtração
subtrai = numeros.subtract(numeros2)
print("Subtract (Diferença):    ", subtrai.collect())

# Produto cartesiano
cartesiano = numeros.cartesian(numeros2)
print("Cartesian (Combinações): ", cartesiano.collect())

# Ação, contar por valor o numero de vezes que cada valor aparece
print("\nCountByValue (Contagem Cartesiana):")
print(cartesiano.countByValue())

print("\n" + "="*50)
print(" PARTE 5: TRABALHANDO COM CHAVE-VALOR (PARES)")
print("="*50)

# Compras: RDD de pares (código_cliente, valor_compra)
compras = sc.parallelize([(1, 200), (2, 300), (3, 120), (4, 250), (5, 78)])

# Separar chaves e valores
print("Apenas as Chaves (Clientes):", compras.keys().collect())
print("Apenas os Valores (Compras):", compras.values().collect())

# Conta elementos por chave
print("\nCountByKey (Qtd de compras por cliente):")
print(compras.countByKey())

# Aplica função no valor, sem alterar a chave
soma = compras.mapValues(lambda valor: valor + 1)
print("\nMapValues (+1 no valor da compra):", soma.collect())

# Agrupa por chave e converte o agrupamento para uma lista
agrupa = compras.groupByKey().mapValues(list)
print("GroupByKey (Agrupado em listas):  ", agrupa.collect())

print("\n" + "="*50)
print(" PARTE 6: JOINS (CRUZAMENTO DE RDDs)")
print("="*50)

# Débitos: RDD de pares (código_cliente, valor_debito)
debitos = sc.parallelize([(1, 20), (2, 300)])

# Inner join entre compras e debitos (Mostra apenas clientes que tem ambos)
resultado = compras.join(debitos)
print("Inner Join (Compras x Débitos):", resultado.collect())

# Remove e mostra apenas quem não tem débito
semdebito = compras.subtractByKey(debitos)
print("SubtractByKey (Sem débito):    ", semdebito.collect())

# ==============================================================================
# Finaliza a aplicação
# ==============================================================================
print("\nDesligando o motor do Spark...")
spark.stop()