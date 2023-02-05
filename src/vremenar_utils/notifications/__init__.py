"""Notifications support."""
from datetime import datetime, timezone

from firebase_admin import initialize_app, messaging  # type: ignore

from vremenar_utils.cli.logging import Logger

firebase_app = initialize_app()


def make_message(
    title: str,
    subtitle: str,
    body: str,
    important: bool = False,
    expires: datetime | None = None,
    badge: int = 0,
) -> messaging.Message:
    """Make a notification message."""
    return messaging.Message(
        notification=messaging.Notification(
            title=title,
            body=body,
        ),
        android=messaging.AndroidConfig(
            notification=messaging.AndroidNotification(
                channel_id="vremenar_alerts" if important else "vremenar_forecast",
                sound="default",
            ),
            priority="high" if important else "normal",
            ttl=expires - datetime.now(tz=timezone.utc) if expires else None,
        ),
        apns=messaging.APNSConfig(
            payload=messaging.APNSPayload(
                aps=messaging.Aps(
                    alert=messaging.ApsAlert(subtitle=subtitle),
                    badge=badge,
                    sound="default",
                    custom_data={"interruption-level": "time-sensitive"}
                    if important
                    else None,
                ),
            ),
        ),
    )


def prepare_message(
    message: messaging.Message,
    topics: list[str] | None = None,
    token: str | None = None,
    logger: Logger | None = None,
) -> None:
    """Prepare a message to send to topic subscribers or to a dedicated device token."""
    if topics is None and token is None:
        err = "Either a list of topics or a token need to be specified."
        raise ValueError(err)

    if topics is not None and token is not None:
        err = "Topics and a token can not be set at the same time."
        raise ValueError(err)

    if topics is not None:
        prepare_message_for_topics(message, topics, logger)

    if token is not None:
        prepare_message_for_token(message, token, logger)


def prepare_message_for_topics(
    message: messaging.Message,
    topics: list[str],
    logger: Logger | None,
) -> None:
    """Prepare a message to send to topic subscribers."""
    if not topics:
        err = "Topics should not be empty."
        raise ValueError(err)

    if len(topics) > 5:
        err = "Too many topics used at the same time"
        raise ValueError(err)

    if len(topics) == 1:
        message.topic = topics[0]
        if logger:
            logger.debug('Sending notification with topic "%s"', message.topic)
    else:
        message.condition = " || ".join([f"'{topic}' in topics" for topic in topics])
        if logger:
            logger.debug('Sending notification with condition "%s"', message.condition)


def prepare_message_for_token(
    message: messaging.Message,
    token: str,
    logger: Logger | None,
) -> None:
    """Prepare a message to send to a dedicated device token."""
    if not token:
        err = "Token should not be empty."
        raise ValueError(err)

    message.token = token
    if logger:
        logger.debug('Sending notification to device with token "%s"', message.token)


def send_message(message: messaging.Message) -> None:
    """Send a single message."""
    messaging.send(message, app=firebase_app, dry_run=False)


def send_messages(messages: list[messaging.Message]) -> None:
    """Send a batch of messages."""
    messaging.send_all(messages, app=firebase_app, dry_run=False)


class BatchNotify:
    """Send notifications in batches."""

    def __init__(self, logger: Logger) -> None:
        """Initialise batch notifications."""
        self.logger = logger
        self.queue: list[messaging.Message] = []
        self.limit = 100

    def __enter__(self) -> "BatchNotify":
        """Context manager init."""
        return self

    def __exit__(self, *args: None) -> None:
        """Context manager exit."""
        self._drain()

    def send(self, message: messaging.Message) -> None:
        """Send the notification to topic."""
        if len(self.queue) == self.limit:
            self._drain()
        self.queue.append(message)

    def _drain(self) -> None:
        """Drain the queue."""
        if self.queue:
            send_messages(self.queue)
        self.queue.clear()
