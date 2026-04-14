# 🚀 Formação PySpark - Curso Udemy

Este repositório foi criado para organizar o desenvolvimento e os estudos realizados durante o curso de PySpark (Prof. Fernando Amaral). O ambiente foi migrado para uma arquitetura híbrida focada em performance e boas práticas de Engenharia de Dados.

## 🛠️ Tecnologias e Ambiente

- **Sistema Operacional:** Linux (Ubuntu via WSL 2)
- **Linguagem:** Python 3.11
- **Motor de Big Data:** Apache Spark 4.x
- **Gerenciador de Pacotes:** [uv](https://github.com/astral-sh/uv) (Extremamente rápido)
- **IDE:** VS Code com extensão Remote - WSL

## 📂 Estrutura do Projeto

Até o momento, o projeto conta com os seguintes scripts de estudo:

* `instalar_ambiente.sh`: Script de automação para configurar o ambiente virtual (`.venv`) e instalar as dependências necessárias no Linux.
* `aula_rdds.py`: Primeiros passos com o Spark usando RDDs (Resilient Distributed Datasets), cobrindo ações, transformações e operações aritméticas.
* `aula_dataframes.py`: Introdução ao uso de DataFrames, criação de Schemas manuais e automáticos, filtros e exportação em formatos de Big Data (Parquet, JSON, ORC).
* `aula_acoes_transformacoes.py`: Aprofundamento em transformações "Lazy" (OrderBy, Filter) e Ações (Collect, Count, Show).

## 🚀 Como Executar

### 1. Configurar o Ambiente
Se for a primeira vez no ambiente ou se precisar resetar as bibliotecas:
```bash
chmod +x instalar_ambiente.sh
./instalar_ambiente.sh
