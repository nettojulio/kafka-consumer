# Kafka Consumer

<a href="https://github.com/nettojulio/ilab-aws-desafio-semana-3">Repositório Central</a>

1. Para executar o projeto, crie as seguintes variáveis de ambiente.

```
KAFKA_HOST="(ip desejado/localhost):9092"
KAFKA_TOPIC="Tópico a ser criado"
KAFKA_GROUP_ID_READER="Grupo para leitura"
```

2. Certifique-se de ter o <a href="https://maven.apache.org/download.cgi">Maven</a> instalado no seu computador com o seguinte comando:

```mvn -version```

3. Instale o Kafka com o comando:

```brew install kafka```

4. Depois execute os seguintes comandos:

```nohup zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties &```
```nohup kafka-server-start /usr/local/etc/kafka/server.properties &```

Para verificar se o `Zookeeper` e o `Kafka` estão funcionando, execute os comandos:

```sudo lsof -t -i:2181```
```sudo lsof -t -i:9092```
5. Execute os comandos para iniciar a aplicação

```./build.sh```
```./start.sh```

6. Ao sair da aplicação, execute os seguintes comandos para finalizar o `Zookeeper` e o `Kafka`

```sudo kill $(sudo lsof -t -i:9092)```
```sudo kill $(sudo lsof -t -i:2181)```
