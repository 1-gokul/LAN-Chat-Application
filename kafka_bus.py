"""
kafka_bus.py
------------
Kafka integration for the LAN Chat Application.

Turns a single-process chat server into a distributed chat backbone: multiple
ChatServer instances (different hosts/ports, or just different terminals on
the same LAN box) can share one Kafka cluster and see each other's clients as
if they were on the same process's ClientRegistry.

Topics
------
chat.broadcast   Public TEXT messages, fanned out to every server instance.
chat.whisper     Private WHISPER messages; every instance consumes and only
                 delivers locally if the target username is registered there.
chat.system      JOIN / LEAVE presence announcements, relayed as SYSTEM
                 messages to clients connected to *other* instances.

Design notes
------------
- Every server instance has a unique `instance_id`. Every message published
  carries that id as `origin`. When an instance reads a message back off
  Kafka, it skips anything it originated itself -- it already applied that
  message to its own registry synchronously, so re-applying it from Kafka
  would double-deliver it to local clients.
- Consumers use `group_id=None` (anonymous group per process) so we get
  fan-out: every instance sees every message, rather than Kafka's usual
  consumer-group load-balancing where only one consumer gets each message.
"""

import json
import logging
import threading
from typing import Callable

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

log = logging.getLogger("kafka_bus")

TOPIC_BROADCAST = "chat.broadcast"
TOPIC_WHISPER = "chat.whisper"
TOPIC_SYSTEM = "chat.system"

ALL_TOPICS = [TOPIC_BROADCAST, TOPIC_WHISPER, TOPIC_SYSTEM]


class ChatKafkaBus:
    """
    Wraps a KafkaProducer plus a background KafkaConsumer thread.
    Each ChatServer instance owns exactly one ChatKafkaBus.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        instance_id: str,
        on_message: Callable[[str, dict], None],
    ):
        """
        on_message(topic, payload_dict) is invoked from the consumer thread
        for every message NOT originated by this instance. It must be
        thread-safe -- it runs concurrently with client handler threads.
        """
        self.instance_id = instance_id
        self._on_message = on_message
        self._stop_event = threading.Event()

        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
            linger_ms=10,
        )

        self._consumer = KafkaConsumer(
            *ALL_TOPICS,
            bootstrap_servers=bootstrap_servers,
            group_id=None,  # anonymous group -> every instance gets every message
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=False,
            consumer_timeout_ms=1000,  # lets the loop check _stop_event periodically
        )

        self._consumer_thread = threading.Thread(
            target=self._consume_loop, daemon=True, name="kafka-consumer"
        )

    # -- lifecycle --

    def start(self) -> None:
        self._consumer_thread.start()
        log.info("Kafka bus started (instance=%s)", self.instance_id)

    def stop(self) -> None:
        self._stop_event.set()
        self._consumer_thread.join(timeout=3)
        try:
            self._consumer.close()
        except Exception:
            pass
        try:
            self._producer.flush(timeout=5)
            self._producer.close(timeout=5)
        except Exception:
            pass
        log.info("Kafka bus stopped (instance=%s)", self.instance_id)

    # -- publish helpers --

    def publish_broadcast(self, sender: str, body: str) -> None:
        self._publish(TOPIC_BROADCAST, {"sender": sender, "body": body})

    def publish_whisper(self, sender: str, target: str, body: str) -> None:
        self._publish(
            TOPIC_WHISPER, {"sender": sender, "target": target, "body": body}
        )

    def publish_system(self, event: str, username: str) -> None:
        """event is 'join' or 'leave'."""
        self._publish(TOPIC_SYSTEM, {"event": event, "username": username})

    def _publish(self, topic: str, payload: dict) -> None:
        payload = {**payload, "origin": self.instance_id}
        try:
            self._producer.send(topic, value=payload)
        except KafkaError as exc:
            log.error("Kafka publish failed on %s: %s", topic, exc)

    # -- consume loop --

    def _consume_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                records = self._consumer.poll(timeout_ms=1000)
            except Exception:
                log.exception("Kafka poll failed")
                continue
            for _tp, messages in records.items():
                for msg in messages:
                    payload = msg.value
                    if payload.get("origin") == self.instance_id:
                        continue  # already applied locally, skip our own echo
                    try:
                        self._on_message(msg.topic, payload)
                    except Exception:
                        log.exception(
                            "Error handling Kafka message on %s", msg.topic
                        )
