# YouTube Collector

This repository contains a pipeline to collect metadata, comments, and thumbnails of YouTube videos by receiving a Video_ID.

# Requirements

## Minikube

```bash
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
```
```bash
sudo install minikube-linux-amd64 /usr/local/bin/minikube
```

Start minikube (change 192.168.49.0 if your network setup is different):

```bash
minikube start --cpus=4 --insecure-registry "192.168.49.0/24"
```

Start dashboard:

```bash
minikube dashboard
```

## Helm

```bash
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
```
```bash
chmod 700 get_helm.sh
```
```bash
./get_helm.sh
```

## Kubectl

```
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
```
```bash
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
```

## DigitalHub

```bash
git clone https://github.com/scc-digitalhub/digitalhub.git
```

```bash
cd digitalhub
```

Follow the `README` instructions on the DigitalHub repository.

```bash
helm upgrade digitalhub helm/digitalhub/ -n digitalhub --install --create-namespace --timeout 15m0s
```

## Strimzi (Kafka Cluster)

```bash
kubectl create namespace kafka
```
```bash
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
```
```bash
kubectl apply -f https://strimzi.io/examples/latest/kafka/kafka-persistent-single.yaml -n kafka 
```
```bash
kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka 
```

Test Send and receive messages:

**Sender**:

```bash
kubectl -n kafka run kafka-producer -ti --image=quay.io/strimzi/kafka:0.37.0-kafka-3.5.1 --rm=true --restart=Never -- bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic my-topic
```

**Receiver**:
```bash
kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.37.0-kafka-3.5.1 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic my-topic --from-beginning
```

## Minio (Data Lake)

Login in the Minio Console (http://192.168.49.2:30090/) with the default credentials (`minioadmin:minio123`) and create the buckets:

* videos
* iceberg

Create the access keys, use base64 on the keys (`echo -n 'mykey' | base64`) and update the yaml file:
```
secrets/minio_credentials.yaml
```
Create the secrets:
```bash
kubectl apply -f secrets/minio_credentials.yaml
```

## Nessie (Iceberg Catalog)

To use nessie we need a database to store the catalog for quick access. The Digitalhub already contains a PostgreSQL server for MlRun, but we need to create a database for nessie to use. Open the Kubernetes Dashboard (`minikube dashboard`), navigate to the namespace `digitalhub` and find the following pod: `
mlrun-postgres-cluster-0` and attach a shell and run the following commands.

```bash
su - postgres
```

```bash
psql
```

```sql
CREATE DATABASE nessie
```

Exit the shell line in the pod. Now we can proceed to install Nessie, first add the repository to helm:

```bash
helm repo add nessie-helm https://charts.projectnessie.org
```

```bash
helm repo update
```
Create the namespace:
```bash
kubectl create namespace nessie-ns
```
In the Digitalhub namespace we need to find the credentials for the Postgre database, the username and password are available in the secret `postgres.mlrun-postgres-cluster.credentials.postgresql.acid.zalan.do`

```bash
kubectl apply -f iceberg/nessie/nessie-postgree-credentials.yaml
```

```bash
helm install -n nessie-ns nessie nessie-helm/nessie -f iceberg/nessie/values.yaml
```

## Dremio (Query Engine)

To deploy our Dremio service we need to access the Coder (http://192.168.49.2:30170) use the following credtials:


* `Username`: `test@digitalhub.test `
* `Password`: `Test12456@!`


Click on the `Templates` button on the top menu and select `Create Workspace` in the Dremio row. Define a workspace name and password of your choice, once deployed Dremio will be available at http://192.168.49.2:30120.

![alt text](/imgs/coderdremio.png "Coder deploy Dremio")

Inside the Dremio dashboard click on `Add source` and select Nessie.

* Set the name to `nessie`
* Set the endpoint to `http://nessie.nessie-ns:19120/api/v2`
* Set the authentication to `none`

![alt text](/imgs/nessie1.png "First Nessie configuration")

Then on the Storage tab configure the following:

* For your access key, set `minio`
* For your secret key, set `minio123`
* Set root path to `/iceberg`

Click on the `Add Property` button under Connection Properties to create and configure the following properties:
* `fs.s3a.path.style.access` to `true`
* `fs.s3a.endpoint` to `minio.digitalhub:9000`
* `dremio.s3.compat` to `true`
* Uncheck `Encrypt connection`

![alt text](/imgs/nessie2.png "Second Nessie configuration")


## Kafka Connect

Deploy the Kafka Connect cluster:

```bash
kubectl apply -f iceberg/kafkaconnect/kafkaconnect.yaml
```

Configure the plugins camel and iceberg sink.

### Test plugin

```bash
kubectl apply -f iceberg/kafkaconnect/camelconfig.yaml
```

Verify if the messages are being created:

```bash
kubectl run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.22.1-kafka-2.7.0 --rm=true --restart=Never -n kafka -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic timer-topic
```

You should receive messages such as:

```json
{"schema":null,"payload":{"message":"Hello World","timestamp":1616517487920}}
{"schema":null,"payload":{"message":"Hello World","timestamp":1616517487914}}
{"schema":null,"payload":{"message":"Hello World","timestamp":1616517488925}}
```

### Apache Iceberg Sink

```bash
kubectl apply -f iceberg/kafkaconnect/icebergsinkconfig.yaml
```

# Running
Go on https://console.cloud.google.com/ create a project and create a Youtube API key and update the yaml file:

```
secrets/youtube_credentials.yaml
```

**Note**: Every string in the secrets yaml files must be encoded in base64. Example: `echo -n 'mykey' | base64`

Create the **YouTube** secret:

```bash
kubectl apply -f secrets/youtube_credentials.yaml
```


Create the **Kafka** secret:

```bash
kubectl apply -f secrets/kafka_credentials.yaml
```



Acess the Nuclio dashboard (http://192.168.49.2:30050) and deploy in the default project the yamls files:

```
collectors/comments/youtubecomments.yaml
collectors/metadata/youtubemeta.yaml
collectors/thumbnails/youtubethumbnails.yaml
```


# References
* https://github.com/scc-digitalhub/digitalhub
* https://strimzi.io/quickstarts/
* https://projectnessie.org/try/kubernetes/
* https://blog.min.io/uncover-data-lake-nessie-dremio-iceberg/
* https://strimzi.io/blog/2021/03/29/connector-build/
* https://github.com/tabular-io/iceberg-kafka-connect
* https://tabular.io/blog/streaming-event-data-to-iceberg-with-kafka-connect/


