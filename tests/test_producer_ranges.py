"""Tests for Kafka producer transaction-range behaviour."""

from __future__ import annotations

import pytest

from src.streaming.producer import validate_arguments


def calculate_expected_id_range(
    start_row: int,
    limit: int,
) -> tuple[int, int]:
    """
    Return the one-based transaction-ID range for a dataset row range.
    """

    return start_row + 1, start_row + limit


@pytest.mark.parametrize(
    ("start_row", "limit", "expected_range"),
    [
        (0, 5, (1, 5)),
        (100, 5, (101, 105)),
        (2000, 5, (2001, 2005)),
        (4000, 2000, (4001, 6000)),
        (10000, 10000, (10001, 20000)),
    ],
)
def test_expected_transaction_id_ranges(
    start_row: int,
    limit: int,
    expected_range: tuple[int, int],
) -> None:
    """
    Dataset row ranges should map to predictable transaction IDs.
    """

    assert (
        calculate_expected_id_range(
            start_row=start_row,
            limit=limit,
        )
        == expected_range
    )


def test_valid_arguments_are_accepted() -> None:
    """
    Valid producer arguments should not raise an exception.
    """

    validate_arguments(
        start_row=2000,
        chunk_size=50000,
        limit=5,
        sleep_time=0.1,
    )


@pytest.mark.parametrize(
    "invalid_start_row",
    [-1, -10, -100],
)
def test_negative_start_row_is_rejected(
    invalid_start_row: int,
) -> None:
    """
    Negative dataset row positions are invalid.
    """

    with pytest.raises(
        ValueError,
        match="--start-row must be zero or greater",
    ):
        validate_arguments(
            start_row=invalid_start_row,
            chunk_size=50000,
            limit=5,
            sleep_time=0.1,
        )


@pytest.mark.parametrize(
    "invalid_limit",
    [0, -1, -100],
)
def test_non_positive_limit_is_rejected(
    invalid_limit: int,
) -> None:
    """
    A producer run must request at least one transaction.
    """

    with pytest.raises(
        ValueError,
        match="--limit must be greater than zero",
    ):
        validate_arguments(
            start_row=0,
            chunk_size=50000,
            limit=invalid_limit,
            sleep_time=0.1,
        )


@pytest.mark.parametrize(
    "invalid_chunk_size",
    [0, -1, -50000],
)
def test_non_positive_chunk_size_is_rejected(
    invalid_chunk_size: int,
) -> None:
    """
    Chunk size must remain positive.
    """

    with pytest.raises(
        ValueError,
        match="--chunk-size must be greater than zero",
    ):
        validate_arguments(
            start_row=0,
            chunk_size=invalid_chunk_size,
            limit=5,
            sleep_time=0.1,
        )


@pytest.mark.parametrize(
    "invalid_sleep_time",
    [-0.01, -1.0],
)
def test_negative_sleep_is_rejected(
    invalid_sleep_time: float,
) -> None:
    """
    Message delay cannot be negative.
    """

    with pytest.raises(
        ValueError,
        match="--sleep must be zero or greater",
    ):
        validate_arguments(
            start_row=0,
            chunk_size=50000,
            limit=5,
            sleep_time=invalid_sleep_time,
        )