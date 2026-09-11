"""Consume UWB data from a Kafka topic."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Union

from kafka import KafkaConsumer
from kafka.errors import KafkaError


DEFAULT_SERVER = "115.145.167.47:9095"
DEFAULT_TOPIC = "uwb"
DEFAULT_GROUP = "uwb-python-client"
DEFAULT_TIMEOUT_MS = 10_000

JsonValue = Union[
    None,
    bool,
    int,
    float,
    str,
    List["JsonValue"],
    Dict[str, "JsonValue"],
]
Payload = Union[JsonValue, bytes]

LOGGER = logging.getLogger(__name__)


class OffsetMode(str, Enum):
    """Supported Kafka offset reset modes."""

    LATEST = "latest"
    EARLIEST = "earliest"


@dataclass(frozen=True)
class ConsumerConfig:
    """Kafka consumer connection settings."""

    server: str = DEFAULT_SERVER
    topic: str = DEFAULT_TOPIC
    group: str = DEFAULT_GROUP
    offset: OffsetMode = OffsetMode.LATEST


def loadConfig() -> ConsumerConfig:
    """Load consumer settings from environment variables."""
    raw_offset = os.getenv("KAFKA_OFFSET_RESET", OffsetMode.LATEST.value).lower()

    try:
        offset = OffsetMode(raw_offset)
    except ValueError as error:
        valid_values = ", ".join(mode.value for mode in OffsetMode)
        raise ValueError(
            f"KAFKA_OFFSET_RESET must be one of: {valid_values}"
        ) from error

    return ConsumerConfig(
        server=os.getenv("KAFKA_BOOTSTRAP_SERVER", DEFAULT_SERVER),
        topic=os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC),
        group=os.getenv("KAFKA_GROUP_ID", DEFAULT_GROUP),
        offset=offset,
    )


def decodeText(raw_value: Optional[bytes]) -> Optional[str]:
    """Decode an optional Kafka key without losing invalid byte sequences."""
    if raw_value is None:
        return None

    return raw_value.decode("utf-8", errors="replace")


def decodePayload(raw_value: Optional[bytes]) -> Payload:
    """Decode UTF-8 JSON when possible and preserve other payloads safely."""
    if raw_value is None:
        return None

    try:
        text_value = raw_value.decode("utf-8")
    except UnicodeDecodeError:
        return raw_value

    try:
        return json.loads(text_value)
    except json.JSONDecodeError:
        return text_value


def createConsumer(config: ConsumerConfig) -> KafkaConsumer:
    """Create a Kafka consumer without subscribing to a topic."""
    return KafkaConsumer(
        bootstrap_servers=[config.server],
        group_id=config.group,
        auto_offset_reset=config.offset.value,
        enable_auto_commit=True,
        request_timeout_ms=DEFAULT_TIMEOUT_MS,
    )


def subscribeTopic(consumer: KafkaConsumer, topic: str) -> None:
    """Verify that a topic exists before subscribing to it."""
    available_topics = consumer.topics()
    if topic not in available_topics:
        raise ValueError(f"Kafka topic does not exist: {topic!r}")

    consumer.subscribe([topic])


def startConsumer(config: ConsumerConfig) -> KafkaConsumer:
    """Create a consumer and establish its topic subscription."""
    consumer = createConsumer(config)

    try:
        subscribeTopic(consumer, config.topic)
    except (KafkaError, ValueError):
        consumer.close()
        raise

    return consumer


def consumeMessages(consumer: KafkaConsumer) -> None:
    """Receive and log messages until the process is interrupted."""
    for message in consumer:
        message_key = decodeText(message.key)
        message_data = decodePayload(message.value)
        LOGGER.info(
            "message received | topic=%s partition=%d offset=%d key=%r data=%r",
            message.topic,
            message.partition,
            message.offset,
            message_key,
            message_data,
        )


def main() -> int:
    """Run the UWB Kafka consumer."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    logging.getLogger("kafka").setLevel(logging.WARNING)

    try:
        config = loadConfig()
        consumer = startConsumer(config)
    except (KafkaError, ValueError) as error:
        LOGGER.error("consumer startup failed | error=%s", error)
        return 1

    LOGGER.info(
        "consumer started | server=%s topic=%s group=%s offset=%s",
        config.server,
        config.topic,
        config.group,
        config.offset.value,
    )

    try:
        consumeMessages(consumer)
    except KeyboardInterrupt:
        LOGGER.info("consumer interrupted")
    except KafkaError as error:
        LOGGER.error("Kafka receive failed | error=%s", error)
        return 1
    finally:
        consumer.close()
        LOGGER.info("consumer stopped")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
