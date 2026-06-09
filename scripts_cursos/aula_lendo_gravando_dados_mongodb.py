import json
import re
from pymongo import MongoClient

# ==============================================================================
# 1. CONFIGURAÇÕES E CAMINHOS
# ==============================================================================
# Caminho nativo do Linux (ignorando a parte do \\wsl.localhost)
caminho_arquivo = "/home/ricar/pyspark_udemy/apoio/Spark/download/mongo/posts.json"

print("--- Conectando ao MongoDB Local ---")
cliente = MongoClient("mongodb://localhost:27017/")

# Vamos usar o mesmo banco da aula anterior, mas criar uma coleção nova chamada 'posts'
banco_de_dados = cliente["loja_curso"]
colecao_posts = banco_de_dados["posts"]

# ==============================================================================
# 2. LIMPANDO A PISTA (IDEMPOTÊNCIA)
# ==============================================================================
# O delete_many({}) sem filtros apaga tudo na coleção.
# Isso garante que se você rodar o script 10 vezes, não vai duplicar os dados!
colecao_posts.delete_many({})
print("--> Coleção 'posts' limpa e pronta para nova carga.")

# ==============================================================================
# 3. LENDO E TRANSFORMANDO OS DADOS (ETL NA PRÁTICA)
# ==============================================================================
print(f"\n--- Lendo o arquivo: posts.json ---")

lista_documentos = []

with open(caminho_arquivo, "r", encoding="utf-8") as arquivo:
    for linha in arquivo:
        linha = linha.strip() # Tira os espaços em branco e quebras de linha
        
        if linha:
            # O PULO DO GATO: Consertando o JSON mal formatado do professor
            # Isso pega palavras como 'nome:' e transforma em '"nome":'
            linha_corrigida = re.sub(r'([a-zA-Z0-9_]+):', r'"\1":', linha)
            
            try:
                # Transforma o texto corrigido em um Dicionário Python
                documento = json.loads(linha_corrigida)
                lista_documentos.append(documento)
            except Exception as e:
                print(f"Erro ao converter linha: {linha_corrigida} -> {e}")

# ==============================================================================
# 4. GRAVANDO NO MONGODB EM LOTE (BULK INSERT)
# ==============================================================================
if lista_documentos:
    print(f"\n--- Inserindo {len(lista_documentos)} registros no MongoDB ---")
    
    # insert_many é muito mais rápido e eficiente que insert_one dentro do loop
    colecao_posts.insert_many(lista_documentos)
    
    print("--> SUCESSO: Carga finalizada!")
else:
    print("Nenhum dado válido encontrado para inserir.")

# ==============================================================================
# 5. ESPIADINHA NOS DADOS (FOTO DO DEPOIS)
# ==============================================================================
print("\n--- Amostra dos Dados no MongoDB (Primeiros 3 registros) ---")

# Fazendo um "SELECT * LIMIT 3" no MongoDB
for doc in colecao_posts.find().limit(3):
    print(doc)

# Fechando a conexão
cliente.close()