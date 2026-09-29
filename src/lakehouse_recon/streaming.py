"""Kafka producer and consumer.

Kept thin and importable without a broker: the driver is imported inside the
methods, so the unit suite runs with no Kafka on the machine.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Iterator
from typing import Any

from lakehouse_recon.config import Settings
from lakehouse_recon.events import TransactionEvent

logger = logging.getLogger(__name__)


def serialize(event: TransactionEvent) -> bytes:
    """JSON bytes for the wire. ``mode="json"`` keeps datetimes ISO-8601."""
    return json.dumps(event.model_dump(mode="json")).encode("utf-8")


class EventProducer:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._producer: Any | None = None

    def _client(self) -> Any:
        if self._producer is None:
            from kafka import KafkaProducer

            self._producer = KafkaProducer(
                bootstrap_servers=self.settings.kafka_bootstrap_servers.split(","),
                value_serializer=lambda v: v,
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                acks="all",
                retries=5,
            )
        return self._producer

    def publish(self, events: Iterable[TransactionEvent]) -> int:
        """Key on ``account_id`` so a single account's events keep their order."""
        client = self._client()
        count = 0
        for event in events:
            client.send(self.settings.kafka_topic, key=event.account_id, value=serialize(event))
            count += 1
        client.flush()
        logger.info("published %d events to %s", count, self.settings.kafka_topic)
        return count

    def close(self) -> None:
        if self._producer is not None:
            self._producer.close()
            self._producer = None


class EventConsumer:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def drain(self, max_messages: int | None = None) -> Iterator[bytes]:
        """Read what is currently on the topic and stop, rather than blocking forever.

        Batch semantics on purpose: reconciliation runs against a closed set of
        data. A consumer that never ends has no point at which the totals are
        allowed to agree.
        """
        from kafka import KafkaConsumer

        consumer = KafkaConsumer(
            self.settings.kafka_topic,
            bootstrap_servers=self.settings.kafka_bootstrap_servers.split(","),
            group_id=self.settings.kafka_consumer_group,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=self.settings.poll_timeout_ms,
        )
        seen = 0
        try:
            for message in consumer:
                yield message.value
                seen += 1
                if max_messages is not None and seen >= max_messages:
                    break
            consumer.commit()
        finally:
            consumer.close()
