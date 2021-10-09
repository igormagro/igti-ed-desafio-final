# Rodar Airflow Localmente
## Iniciar ambiente virtual python
```bash
python -m venv .venv
.venv/Scripts/activate
```
**Obs**: o comando para ativar ambiente virtaul pode variar de acordo com o sistema operacional. 

## Checar versão Python
```bash
python --version
```

## Instalar Airflow
```bash
pip install "airflow=2.1.4" --constaint "https://raw.githubusercontent.com/apache/airflow/constraints-2.1.4/constraints-<sua_versao_python>.txt"
```
Uma vez instalado, denifir o diretorio root do Airflow
```bash
export AIRFLOW_HOME=$(pwd)/airflow
```
## Iniciar db de metadados do airflow
```bash
airflow db init
```
Este comando criará uma serie de arquivos, entre eles o arquivo de configuração `airflow.cfg`. Por default, serão carregadas as dags de exemplo. Caso não queira que essas dags sejam carregadas, setar `load_examples = False` no arquivo de configuração. Neste caso será necessário resetar o db do Airflow:
```bash
airflow db reset
```

## Criar usuario para UI
```bash
airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org
```

## Iniciar scheduler e webserver
Para iniciar o scheduler em segundo plano:
```bash
airflow scheduler -D
```
Para iniciar o Airflow UI:
```bash
airflow webserver
```
Por padrão, ficará disponivel em [localhost:8080](http://127.0.0.1:8080).


# Referencias
[Running Airflow locall - Airflow Docs](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html)
