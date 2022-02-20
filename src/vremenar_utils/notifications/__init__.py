"""Notifications support."""
from firebase_admin import messaging, initialize_app  # type: ignore
from typing import Optional

firebase_app = initialize_app()


def make_message(title: str, subtitle: str, body: str) -> messaging.Message:
    """Make a notification message."""
    message = messaging.Message(
        notification=messaging.Notification(
            title=title,
            body=body,
        ),
        android=messaging.AndroidConfig(
            # ttl=datetime.timedelta(seconds=3600),
            priority='normal',
            notification=messaging.AndroidNotification(
                # icon='stock_ticker_update', color='#f45342'
            ),
        ),
        apns=messaging.APNSConfig(
            payload=messaging.APNSPayload(
                aps=messaging.Aps(
                    alert=messaging.ApsAlert(subtitle=subtitle),
                    badge=42,
                    custom_data={'interruption-level': 'time-sensitive'},
                ),
            ),
        ),
    )
    return message


def send_message(
    message: messaging.Message, topic: Optional[str] = None, token: Optional[str] = None
) -> None:
    """Send a message to topic subscribers or to a dedicated token."""
    if topic is None and token is None:
        raise ValueError('Either topic or token need to be specified.')

    if topic is not None and token is not None:
        raise ValueError('Topic and token can not be set at the same time.')

    if topic is not None:
        message.topic = topic
        print(f'Sending notification with topic "{message.topic}"')

    if token is not None:
        message.token = token
        print(f'Sending notification to device with token "{message.token}"')

    response = messaging.send(message)
    print('Successfully sent message:', response)
