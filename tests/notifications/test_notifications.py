"""Notifications tests."""

import re

import pytest
from typer.testing import CliRunner

runner = CliRunner()


@pytest.mark.forked
def test_send_message(env: dict[str, str]) -> None:
    """Test send message."""
    from vremenar_utils.cli import application

    result = runner.invoke(
        application,
        ["send-message", "en_minor_DE058", "test message"],
        env=env,
        catch_exceptions=False,
    )
    assert result.exit_code == 0


def test_notifications_make() -> None:
    """Test making notifications."""
    from vremenar_utils.notifications import make_message, prepare_message

    message = make_message("Hello", "subtitle", "test message")

    with pytest.raises(
        ValueError,
        match=re.escape("Either a list of topics or a token need to be specified."),
    ):
        prepare_message(message)

    with pytest.raises(
        ValueError,
        match=re.escape("Topics and a token can not be set at the same time."),
    ):
        prepare_message(message, topics=["topic"], token="test")  # noqa: S106

    prepare_message(message, topics=["topic"])
    prepare_message(message, topics=["topic1", "topic2"])

    with pytest.raises(
        ValueError,
        match=re.escape("Topics should not be empty."),
    ):
        prepare_message(message, topics=[])

    with pytest.raises(
        ValueError,
        match=re.escape("Too many topics used at the same time."),
    ):
        prepare_message(message, topics=["topic"] * 6)

    prepare_message(message, token="test")  # noqa: S106

    with pytest.raises(
        ValueError,
        match=re.escape("Token should not be empty."),
    ):
        prepare_message(message, token="")
