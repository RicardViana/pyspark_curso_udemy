# Import de bibliotecas
import os
import sys
sys.stdout.reconfigure(encoding='utf-8')
import psycopg2 

from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Carregar variaveis do ambiente
caminho_do_env = "/home/ricar/pyspark_udemy/configuracoes_seguras/.env"
load_dotenv(dotenv_path=caminho_do_env)

senha_banco = os.getenv("DB_PASSWORD")
if not senha_banco:
    raise ValueError(f"ERRO: A senha do banco não foi encontrada!")

# Configuração e conexão do Spark
caminho_driver = "/home/ricar/pyspark_udemy/apoio/postgresql-42.7.11.jar" 

spark = SparkSession.builder \
    .appName("Carga_Staging_Postgres") \
    .config("spark.jars", caminho_driver) \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("ERROR")

# Consultar a tabela atual
print("\n- Tabela Oficial ANTES da Carga")
try:
    df_antes = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/vendas") \
        .option("dbtable", "vendadata") \
        .option("user", "postgres") \
        .option("password", senha_banco) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    print(f"Total de registros atuais: {df_antes.count()}")
    df_antes.show(5)
except Exception:
    print("A tabela 'vendadata' ainda não existe no banco. Esta será a primeira carga!\n")

# Ler e Transformar
print("\n- Conectando, Lendo e Transformando")

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

# Gravar os dados na Stagin Table (Load e Overwerite)
print("\n- Gravando na Staging Table")

vendadata.write.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/vendas") \
    .option("dbtable", "vendadata_staging") \
    .option("user", "postgres") \
    .option("password", senha_banco) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("--> Dados carregados na Staging com sucesso!")

# Orquestar os dados com o Upsert
print("\n- Executando o UPSERT na Tabela Oficial")

# Bloco 'with' para garantir que a conexão com o banco vai fechar no final
try:
    with psycopg2.connect(
        host="localhost",
        database="vendas",
        user="postgres",
        password=senha_banco,
        port="5432"
    ) as conn:
        with conn.cursor() as cursor:
            
            # Passo A: Garantir que a tabela oficial existe
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS vendadata (
                    data DATE,
                    total DOUBLE PRECISION
                );
            """)
            
            # Passo B: Apagar da tabela oficial tudo que veio de atualização na staging
            # A chave de comparação é a 'data'. Se vier uma data repetida na carga, a velha é apagada.
            cursor.execute("""
                DELETE FROM vendadata 
                WHERE data IN (SELECT data FROM vendadata_staging);
            """)
            
            linhas_deletadas = cursor.rowcount
            print(f"--> {linhas_deletadas} registros antigos foram apagados para dar lugar aos novos.")
            
            # Passo C: Copiar tudo da staging para a oficial (insert)
            cursor.execute("""
                INSERT INTO vendadata (data, total)
                SELECT data, total FROM vendadata_staging;
            """)
            
            linhas_inseridas = cursor.rowcount
            print(f"--> {linhas_inseridas} registros (novos + atualizados) foram inseridos com sucesso!")
            
            # .commit() para salvar a transação permanentemente no banco
            conn.commit()
            
except Exception as e:
    print(f"--> ERRO durante o UPSERT no PostgreSQL: {e}")

# Consultar dados pós carga
print("\n- Tabela Oficial DEPOIS da Carga")

df_depois = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/vendas") \
    .option("dbtable", "vendadata") \
    .option("user", "postgres") \
    .option("password", senha_banco) \
    .option("driver", "org.postgresql.Driver") \
    .load()

print(f"Total de registros finais: {df_depois.count()}")
df_depois.show()

# Finalização
print("\nProcesso finalizado!")
spark.stop()