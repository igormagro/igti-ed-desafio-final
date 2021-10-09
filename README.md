# Desafio Final - Bootcamp Engenheiro de Dados IGTI
## Enunciado Original
<i>"Você foi contratado pela empresa (fictícia) #VamosJuntos - Desenvolvimento Social para desenvolver o seu primeiro projeto de Dados. Em seu trabalho investigativo preliminar, você já identificou que as principais fontes de dados necessárias são:
* Uma Database MongoDB disponível na nuvem para consulta.
* Uma API do IBGE (https://servicodados.ibge.gov.br/api/docs/localidades) para extração de informação de regiões, mesorregiões e microrregiões no Brasil.

Você deve, portanto, construir um pipeline de dados que faça a extração dos dados no MongoDB e na API do IBGE e deposite no Data Lake da empresa. Após a ingestão dos dados no Data Lake, você deve disponibilizar o dado tratado e filtrado apenas para o público de interesse da empresa em um DW. Com os dados no DW, você vai realizar algumas consultas e extrair resultados importantes para a #VamosJuntos."</i>

### Alteração da proposta (por mim)
Em vez de ingerir os dados para um Data Lake, tratá-los e depois disponibilizar em um Data Warehouse, resolvi usar um Data Lakehouse. Utilizando o AWS Athena e Glue Data Catalog, consegui criar um ambiente de constulas SQL diretamente sobre o Lake, sem a necessiadade de uma instancia dedicada a um banco de dados sequencial.

## Pipelines
Devido ao carater mais introdutório do curso, optei por rodar os processos ingestão e tratamento (Airflow e PySpark) localmente.

### Airflow
Programei 4 DAGs para ingestão dos dados na camada *raw* do DL:  **3** referentes aos dados do *IBGE* (regiões, mesorregiões e microrregiões) e **1** referente aos dados disponibilidados no *MongoDB*.

### PySpark
Os processos de tratamento foram feitos com PySpark. Para isso utilizei Notebooks Jupyter rodando sobre um cluster Spark local.

### Athena e Glue
Uma vez disponibilizados os dados na camada *consumer*, executei um Glue Crawler para criação das tabelas a serem consultadas pelo Athena. Por fim, utilizei o Athena para realização das consultas para responder às perguntas do desafio.

## Extra
Os buckets S3 (*raw*, *processing* e *consumer*) foram criados via terraform.

# Pontos de melhoria
1. Como dito, os processos foram rodados localmente. Sendo assim, seria necessário migrá-los para nuvem.
Uma forma de fazê-lo seria a criação de um cluster Kubernetes (AWS EKS) com as imagens do Airflow e Spark, o que garantiria a disponibilidade dos processos.
2. Orquestração dos scripts de tratamento via Airflow. Uma solução possivel, seria a utilização do SparkOperator, disponivel no Airflow. Ou ainda, utilizar KubernetesOperators para criação de pods Spark. Neste ultimo caso, os pods existiriam apenas em tempo de execução e o Airflow faria o trabalho de criar, executar o script e encerrar esses pods.