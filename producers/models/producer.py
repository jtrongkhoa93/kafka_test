"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient
from kafka.admin import KafkaAdminClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARN)


BROKER_URL = "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094"
# BROKER_URL = "PLAINTEXT://kafka0:9092,PLAINTEXT://kafka1:9093,PLAINTEXT://kafka2:9094"
# BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
# SCHEMA_REGISTRY = "http://schema-registry:8081"

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            # "schema.registry.url": SCHEMA_REGISTRY,
            "linger.ms":"10000",
            "queue.buffering.max.messages":"10000000"
        }

        self.admin_client = AdminClient({
            "bootstrap.servers": 'localhost:9092,localhost:9094,localhost:9094'
            })
        
        schema_registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})


        # self.kk_admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092')

        print(self.topic_name)
        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            # self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer
        self.producer = AvroProducer(
            self.broker_properties,
            schema_registry=schema_registry,
            default_key_schema=self.key_schema, 
            default_value_schema=self.value_schema
        )

    def is_new_topic(self, topic_name):
        try:
            topic_metadata = self.admin_client.list_topics(timeout=5)
            # topic_metaset = set(t.topic for t in iter(topic_metadata.topics.values()))
            # print(topic_metaset)
        except Exception as e:
            print(e)
            print("exiting production loop")
            return (False)
        # return topic_name not in topic_metaset
        return False
        # topic_metadata = self.kk_admin_client.list_topics()

        # if topic_name in topic_metadata:
        #     print(f"Topic {topic_name} exists")
        #     return False
        # else:
        #     print(f"Topic {topic_name} does not exist")
        #     return True

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        # print(self.topic_name)

        if self.is_new_topic(self.topic_name):
            # print(self.topic_name)
            futures = self.admin_client.create_topics(
                [NewTopic(topic=self.topic_name, num_partitions=self.num_partitions, replication_factor=self.num_replicas)]
            )

            for _, future in futures.items():
                try:
                    future.result()
                    print(f"{self.topic_name} is created")
                except Exception as e:
                    print(e)
                    print("exiting production loop")
                    # pass

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        # Flush the producer before closing
        self.producer.flush()

        # Close the producer
        self.producer.close()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
