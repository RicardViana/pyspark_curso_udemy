# Import de bibliotecas
import os
import sys
sys.stdout.reconfigure(encoding='utf-8')
import psycopg2 

from pyspark.sql import SparkSession
from dotenv import load_dotenv

# CARREGANDO VARIÁVEIS DE AMBIENTE
caminho_do_env = "/home/ricar/pyspark_udemy/configuracoes_seguras/.env"
load_dotenv(dotenv_path=caminho_do_env)

senha_banco = os.getenv("DB_PASSWORD")
if not senha_banco:
    raise ValueError(f"ERRO: A senha do banco não foi encontrada!")

# CONFIGURAÇÃO E CONEXÃO SPARK
caminho_driver = "/home/ricar/pyspark_udemy/apoio/postgresql-42.7.11.jar" 

spark = SparkSession.builder \
    .appName("Carga_Staging_Postgres") \
    .config("spark.jars", caminho_driver) \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("ERROR")

# LENDO E TRANSFORMANDO (EXTRACT & TRANSFORM)
print("\n- Conectando, Lendo e Transformand")

resumo = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/vendas") \
    .option("dbtable", "vendas") \
    .option("user", "postgres") \
    .option("password", senha_banco) \
    .option("driver", "org.postgresql.Driver") \
    .load()

vendadata = resumo.select("data", "total")
qtd_nova_carga = vendadata.count()

print(f"Dados processados em memória. Total de linhas para atualizar/inserir: {qtd_nova_carga}")

# GRAVANDO NA STAGING TABLE (LOAD - OVERWRITE)
print("\n-Gravando na Staging Table")

# O Spark joga os dados na tabela temporária (ele apaga a staging antiga e cria uma nova rapidinho)
vendadata.write.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/vendas") \
    .option("dbtable", "vendadata_staging") \
    .option("user", "postgres") \
    .option("password", senha_banco) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("--> Dados carregados na Staging com sucesso!")

# ORQUESTRANDO O UPSERT (SQL PURO NO POSTGRES)
print("\n- Executando o UPSERT na Tabela Oficial")

# Vamos usar um bloco 'with' para garantir que a conexão com o banco vai fechar no final
try:
    with psycopg2.connect(
        host="localhost",
        database="vendas",
        user="postgres",
        password=senha_banco,
        port="5432"
    ) as conn:
        with conn.cursor() as cursor:
            
            # Passo A: Garantir que a tabela oficial existe (Caso seja a 1ª vez rodando o script)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS vendadata (
                    data DATE,
                    total DOUBLE PRECISION
                );
            """)
            
            # Passo B: O DELETE (Apaga da tabela oficial tudo que veio de atualização na staging)
            # A chave de comparação aqui é a 'data'. Se vier uma data repetida na carga, a velha é apagada.
            cursor.execute("""
                DELETE FROM vendadata 
                WHERE data IN (SELECT data FROM vendadata_staging);
            """)
            
            linhas_deletadas = cursor.rowcount
            print(f"--> {linhas_deletadas} registros antigos foram apagados para dar lugar aos novos.")
            
            # Passo C: O INSERT (Copia tudo da staging para a oficial)
            cursor.execute("""
                INSERT INTO vendadata (data, total)
                SELECT data, total FROM vendadata_staging;
            """)
            
            linhas_inseridas = cursor.rowcount
            print(f"--> {linhas_inseridas} registros (novos + atualizados) foram inseridos com sucesso!")
            
            # O .commit() é o que salva a transação permanentemente no banco
            conn.commit()
            
except Exception as e:
    print(f"--> ERRO durante o UPSERT no PostgreSQL: {e}")

# 5. FINALIZAÇÃO E LIMPEZA
print("\nProcesso finalizado!")
spark.stop()