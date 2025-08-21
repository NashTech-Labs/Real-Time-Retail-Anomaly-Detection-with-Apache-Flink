# Tiltfile for Flink job with Kafka

# Paths to Kubernetes YAMLs
kafka_yaml = "k8s/kafka.yaml"
flink_yaml = "k8s/flink-session-cluster.yaml"

# -----------------------------------------------------------------------------
# Step 1: Load Kafka as source and sink for flink job
# -----------------------------------------------------------------------------
k8s_yaml(kafka_yaml)
k8s_resource(
    'kafka',
    resource_deps=[],
    labels=["kafka"]
)
k8s_resource(
    'kafka-ui',
    resource_deps=['kafka'],
    labels=["kafka"]
)

# -----------------------------------------------------------------------------
# Step 2: Build Flink job image
# -----------------------------------------------------------------------------
local_resource(
    'flink-job-jar',
    'cd flink-job && mvn clean package -DskipTests',
    labels=["flink-job"]

)

docker_build(
    'anomalies-detection',
    '.',
    dockerfile='Dockerfile',
    ignore=['target', '.idea', '*.iml']
)

# -----------------------------------------------------------------------------
# Step 3: Deploy Flink session cluster and submit a flink job
# -----------------------------------------------------------------------------
k8s_yaml('k8s/flink-session-cluster.yaml')

# Flink job should only deploy after Kafka and Mongo are ready
k8s_resource('flink-jobmanager', resource_deps=['kafka', 'kafka-ui'], labels=["flink-job"])
k8s_resource('flink-taskmanager', resource_deps=['kafka', 'kafka-ui'], labels=["flink-job"])

k8s_yaml('k8s/flink-job-submitter.yml')
k8s_resource("flink-job-submitter",
  resource_deps=['flink-jobmanager', 'flink-taskmanager'], labels=["flink-job"])

# -----------------------------------------------------------------------------
# Step 3: A pipeline to move data from kafka to influx db through kafka connect
# -----------------------------------------------------------------------------
k8s_yaml([
  "k8s/influxdb.yml",
  "k8s/kafka-connector.yml",
  "k8s/influx-connector-job.yml"
])

k8s_resource("influxdb", labels=["kafka-to-influx"])
k8s_resource("kafka-connect", labels=["kafka-to-influx"])
k8s_resource("register-influx-sink",labels=["kafka-to-influx"])

# ---------------------------------------------------------------------------------
# Step 4: Grafana setup for the observability for the alerts of anomalies detection
# ---------------------------------------------------------------------------------
k8s_yaml("k8s/grafana.yml")
k8s_resource("grafana", labels=["alerts-observability"])


# -----------------------------------------------------------------------------
# Step 5: Push test events to kafka through data generator utility
# -----------------------------------------------------------------------------
docker_build("data-generator", "data-gen")
k8s_yaml("data-gen/k8s/generator.yml")
k8s_resource(
    'data-generator',
    resource_deps=['flink-jobmanager'],
    labels=["data-generator"]
)