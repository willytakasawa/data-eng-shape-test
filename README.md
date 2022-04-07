# Data Eng. Shape Test

## Dados
Os dados são referentes a captura de falhas nos equipamentos através de sensores, cada equipamento embarcado contém múltiplos sensores. Para cada falha detectada, os dados de todos os sensores do equipamento são armazenados em um arquivo de extensão .log

Foram disponibilizados 3 arquivos:
 - equipment_failure_sensors.log
 - equipment_sensors.csv
 - equipment.json

## Solução
### **1 - Criação do Banco de Dados Relacional**
O primeiro passo para o desenvolvimento do projeto foi a criação de um banco de dados relacional, nesse caso optou-se pelo MySQL.

Para isso, utilizou-se o docker-compose para subir um container com o banco de dados, denominado 'db' e com schema definido no arquivo dump.sql

As configuracoes do docker e do MySQL estão disponíveis na pasta do projeto [db-docker](https://github.com/willytakasawa/data-eng-shape-test/tree/master/db-docker).

### **2 - Desenvolvimento do Processo de Ingestão**
O processo de ingestão dos dados foi realizado através da leitura e manipulação dos arquivos de dados e insercão dos registros no MySQL utilizando Python e a biblioteca SQLAlchemy.
[elt.py](https://github.com/willytakasawa/data-eng-shape-test/blob/master/app/main/etl.py)

### **3 - Queries Extração**
As queries de extração foram pensadas para responder 3 perguntas referentes ao mês de Janeiro-2020:
  - Total de falhas ocorridas no período.
  - Qual codigo de equipamento apresentou maior número de falhas.
  - Média das falhas ocorridas em cada grupo de equipamentos.

Para chegar as respostas, pode-se:
  - Acessar o container do MySQL e utilizar o arquivo [.sql](https://github.com/willytakasawa/data-eng-shape-test/blob/master/app/main/extract.sql)
  - Executar o script [.py](https://github.com/willytakasawa/data-eng-shape-test/blob/master/app/main/answers.py), esse arquivo além de exibir as respostas no terminal do usuário gera arquivos excel na pasta [answers](https://github.com/willytakasawa/data-eng-shape-test/tree/master/answers).

## Reprodução do Projeto
Para reproduzir o projeto em ambiente Linux:
  - Clone o diretório
  - Execute o docker-compose.yml
       - docker-compose -f "db-docker/docker-compose.yml" up -d --build <
  - Instale os requirements especificados no arquivo requirements.txt:
       - conda create --name <env> --file requirements.txt
  - Execute o script [etl.py](https://github.com/willytakasawa/data-eng-shape-test/blob/master/app/main/etl.py) para popular o banco de dados
  - Execute o script [answers.py](https://github.com/willytakasawa/data-eng-shape-test/blob/master/app/main/answers.py) para extrair os dados e gerar as respostas
