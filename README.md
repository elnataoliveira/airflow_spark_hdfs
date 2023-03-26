# Pipeline with Airflow, Spark and Hadoop
####  _Data Source: ftp://ftp.mtps.gov.br/pdet/microdados_

[![Build Status](https://raw.githubusercontent.com/elnataoliveira/airflow_spark_hdfs/main/static/img/_Fluxograma.svg)](https://raw.githubusercontent.com/elnataoliveira/airflow_spark_hdfs/main/static/img/_Fluxograma.svg)

## Features

- Realizar a ingestão dos dados de VÍNCULOS PÚBLICOS da RAIS 2020
- Transformar os dados no formato parquet e escrevê-los na zona staging ou zona silver do seu Data Lake

|-- [2.8G]  RAIS_VINC_PUB_CENTRO_OESTE.txt
|-- [6.2G]  RAIS_VINC_PUB_MG_ES_RJ.txt
|-- [5.2G]  RAIS_VINC_PUB_NORDESTE.txt
|-- [1.7G]  RAIS_VINC_PUB_NORTE.txt
|-- [8.6G]  RAIS_VINC_PUB_SP.txt
|-- [5.5G]  RAIS_VINC_PUB_SUL.txt
    [30.G]  total
## Tech

Containers:

- [Airflow](http://150.136.179.71:8282) - airflow container
- [Spark](http://150.136.179.71:8080) - spark-master container
- [Namenode](http://150.136.179.71:9870) - namenode master container

And of course Dillinger itself is open source with a [public repository][dill]
 on GitHub.

## Installation

Dillinger requires [Node.js](https://nodejs.org/) v10+ to run.

Install the dependencies and devDependencies and start the server.

```sh
cd dillinger
npm i
node app
```

For production environments...

```sh
npm install --production
NODE_ENV=production node app
```

## Docker

Dillinger is very easy to install and deploy in a Docker container.

Once done, run the Docker image and map the port to whatever you wish on
your host. In this example, we simply map port 8000 of the host to
port 8080 of the Docker (or whatever port was exposed in the Dockerfile):

```sh
docker run -d -p 8000:8080 --restart=always --cap-add=SYS_ADMIN --name=dillinger <youruser>/dillinger:${package.json.version}
```

```sh
127.0.0.1:8000
```

## License

MIT

**Free Software, Hell Yeah!**
