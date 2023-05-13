"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://kafka0:9092,PLAINTEXT://kafka1:9093,PLAINTEXT://kafka2:9094"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081/"

class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        self.broker_properties = {
            'bootstrap.servers': BROKER_URL,
            'group.id': 'org.chicago.cta',
            'enable.auto.commit': False
        }

        if self.is_avro is True:
            self.broker_properties["schema.registry.url"] = SCHEMA_REGISTRY_URL
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)


        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        if consumer.offset_earliest:
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING

        consumer.assign(partitions)

        logger.info("partitions assigned for %s", self.topic_name_pattern)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""

        msg = self.consumer.poll(self.consume_timeout)

        if msg is None:
            logger.info("no message received by consumer")
            return 0

        if msg.error() is not None:
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                logger.info('Reached end of partition')
            else:
                # Handle any other errors
                logger.info('Error while polling for messages: {}'.format(msg.error()))
            return 0

        self.message_handler(msg)

        return 1


    def close(self):
        """Cleans up any open kafka consumers"""
        consumer.close()
