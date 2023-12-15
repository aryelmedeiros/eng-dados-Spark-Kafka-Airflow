#### Esse projeto é baseado no repositório: https://github.com/cmdviegas/docker-hadoop-cluster 

Depois que o cluster estiver funcionando, seguir o passo a passo em `7_Primeiros_exemplos_-_Kafka.txt` para instalar corretamente o Kafka, Debezium e configurar o conector com o Postgres.

Uma vez que o Kakfa esteja devidamente configurado, a ideia é usar o script `populate.py` para preencher a tabela do Postgres DB com os dados do arquivo `cars.csv` e depois utilizar `spark.py` para incializar o consumo e processamento dos dados inseridos no DB. O arquivo `Spark.ipynb` é uma alternativa para caso alguma das configurações do cluster estejam incorretas, é possivel demonstrar o Spark em funcionamento fora do cluster.

Comandos uteis:
- Acessando o banco de dados pleo nó mestre:
```
psql -h db -p 5432 -U postgres -d hive_metastore
```
- Verficar se o Conector entre o Kafka e o Postgres está funcionando: 
```
curl http://node-master:8083/connectors/cursospark-connector/status
```

Algum dos problemas encontrados com o projeto que ainda não foram resolvidos:

-  Aparentemente há uma incompatibilidade entre a versão do Apache Spark, Apache Kakfa que faz com que o consumo dos dados por streaming não seja possivel.


## Deploying a cluster with Apache Hadoop 3.3.5 + Apache Spark 3.4.1 + Apache Hive 3.1.3

This is a script that deploys a cluster with Apache Hadoop and Apache Spark (+ Apache Hive) in fully distributed mode using Docker as infrastructure.

### :rocket: How to build and run

⚠️ You should edit `.env` file in order to set several parameters for the cluster, like username and password, memory resources, and other definitions. Basically you just need to edit this file.

Suggestion: before you begin, you can pre-download Apache Hadoop, Apache Spark and/or Apache Hive by running `download.sh` script before invoking docker compose build. However, this is optional, since Dockerfile may download they automatically during building process.

#### Option 1: Hadoop + Spark + Hive 2.3.9 (builtin) + Derby as Metastore (default)

By default, this option creates three containers: one as node-master and two as worker nodes. The number of worker nodes can be changed by setting `$NODE_REPLICAS` to the desired value in `.env` file.

To build and run this option:
```
docker compose build && docker compose up 
```

#### Option 2: Hadoop + Spark + Hive 3.1.3 (external) + PostgreSQL as Metastore

By default, this option creates five containers: one as node-master, two as worker nodes, one as hive server and one as hive metastore (with postgresql). The number of worker nodes can be changed by setting `$NODE_REPLICAS` to the desired value in `.env` file, as well as other definitions regarding postgresql.

To build and run this option:
```
docker compose build && docker compose --profile hive up 
```


<!-- 
#### [manual mode] 
#### Dockerfile option

1. Build image based on Dockerfile
```
docker build --build-arg USER=spark --build-arg PASS=spark -t hadoopcluster/hadoop-spark:v4 .
```

2. Create an isolated network to run Hadoop nodes
```
docker network create --subnet=172.18.0.0/24 hadoop_network
```

3. Run Hadoop slaves (data nodes)
```
docker run -it -d --network=hadoop_network --ip 172.18.0.3 --name=slave1 --hostname=slave1 hadoopcluster/hadoop-spark:v4
docker run -it -d --network=hadoop_network --ip 172.18.0.4 --name=slave2 --hostname=slave2 hadoopcluster/hadoop-spark:v4
```

4. Run Hadoop master (name node)
```
docker run -it -p 9870:9870 -p 8088:8088 -p 18080:18080 -p 2222:22 --network=hadoop_network --ip 172.18.0.2 --name=node-master --hostname=node-master hadoopcluster/hadoop-spark:v4
```
-->
### :bulb: Tips

#### To access node-master
```
ssh -p 2222 spark@localhost
```
or
```
docker exec -it node-master /bin/bash
```

### :page_facing_up: License

Copyright (c) 2023 [CARLOS M. D. VIEGAS](https://github.com/cmdviegas).

This script is free and open-source software licensed under the [MIT License](https://github.com/cmdviegas/docker-hadoop-cluster/blob/master/LICENSE). 

`Apache Hadoop`, `Apache Spark` and `Apache Hive` are free and open-source software licensed under the [Apache License](https://github.com/cmdviegas/docker-hadoop-cluster/blob/master/LICENSE.apache).
