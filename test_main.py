import json
from datetime import datetime, timezone

import pyodbc
import pytest
from fastapi.testclient import TestClient

import main

client = TestClient(main.app)


# --- DB fakes ---

class FakeCursor:
    def __init__(self, rows=None, execute_error=None, existing=None):
        self._rows = rows or []
        self._execute_error = execute_error
        self._existing = existing
        self.executed = []

    def execute(self, sql, *params):
        if self._execute_error is not None:
            raise self._execute_error
        self.executed.append((sql, params))

    def fetchone(self):
        return self._existing

    def fetchall(self):
        return self._rows


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False
        self.closed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


class FakeRow:
    def __init__(self, **fields):
        self.__dict__.update(fields)


def patch_db(monkeypatch, cursor):
    conn = FakeConnection(cursor)
    monkeypatch.setattr(main, "get_db_connection", lambda: conn)
    return conn


# --- Service Bus fakes ---

class FakeMessage:
    def __init__(self, body):
        self._body = body

    def __str__(self):
        return self._body


class FakeReceiver:
    def __init__(self, messages):
        self._messages = messages
        self.completed = []
        self.abandoned = []
        self.dead_lettered = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def __iter__(self):
        return iter(self._messages)

    def complete_message(self, message):
        self.completed.append(message)

    def abandon_message(self, message):
        self.abandoned.append(message)

    def dead_letter_message(self, message, reason=None, error_description=None):
        self.dead_lettered.append((message, reason, error_description))


class FakeServiceBusClient:
    def __init__(self, receiver):
        self._receiver = receiver

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get_queue_receiver(self, queue_name, max_wait_time=None):
        return self._receiver


# --- /health ---

def test_health():
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


# --- /feedback/driver/{driver_id} ---

def test_get_feedback_returns_rows(monkeypatch):
    submitted = datetime(2026, 6, 23, 12, 0, tzinfo=timezone.utc)
    row = FakeRow(
        FeedbackId="fb-1",
        RideId="ride-1",
        DriverId="driver-1",
        PassengerId="pass-1",
        Rating=5,
        Comment="Great ride",
        SubmittedAt=submitted,
    )
    patch_db(monkeypatch, FakeCursor(rows=[row]))

    resp = client.get("/feedback/driver/driver-1")

    assert resp.status_code == 200
    assert resp.json() == [
        {
            "feedbackId": "fb-1",
            "rideId": "ride-1",
            "driverId": "driver-1",
            "passengerId": "pass-1",
            "rating": 5,
            "comment": "Great ride",
            "submittedAt": submitted.isoformat(),
        }
    ]


def test_get_feedback_empty(monkeypatch):
    patch_db(monkeypatch, FakeCursor(rows=[]))

    resp = client.get("/feedback/driver/unknown-driver")

    assert resp.status_code == 200
    assert resp.json() == []


def test_get_feedback_null_passenger(monkeypatch):
    row = FakeRow(
        FeedbackId="fb-2",
        RideId="ride-2",
        DriverId="driver-2",
        PassengerId=None,
        Rating=None,
        Comment=None,
        SubmittedAt=None,
    )
    patch_db(monkeypatch, FakeCursor(rows=[row]))

    resp = client.get("/feedback/driver/driver-2")

    assert resp.status_code == 200
    body = resp.json()[0]
    assert body["passengerId"] is None
    assert body["submittedAt"] is None


def test_get_feedback_db_error_returns_500(monkeypatch):
    patch_db(monkeypatch, FakeCursor(execute_error=pyodbc.Error("connection lost")))

    resp = client.get("/feedback/driver/driver-1")

    assert resp.status_code == 500
    assert "Database error" in resp.json()["detail"]


# --- save_feedback ---

def test_save_feedback_inserts(monkeypatch):
    cursor = FakeCursor()
    conn = patch_db(monkeypatch, cursor)

    main.save_feedback(
        {
            "rideId": "ride-1",
            "driverId": "driver-1",
            "passengerId": "pass-1",
            "rating": 4,
            "comment": "ok",
        }
    )

    assert conn.committed is True
    assert conn.closed is True
    assert len(cursor.executed) == 2
    params = cursor.executed[-1][1]
    assert "ride-1" in params and "driver-1" in params and 4 in params


def test_save_feedback_skips_duplicate_ride(monkeypatch):
    cursor = FakeCursor(existing=(1,))
    conn = patch_db(monkeypatch, cursor)

    main.save_feedback({"rideId": "ride-1", "driverId": "driver-1", "rating": 4})

    assert len(cursor.executed) == 1
    assert conn.committed is False


def test_save_feedback_rejects_without_ride_or_driver(monkeypatch):
    cursor = FakeCursor()
    conn = patch_db(monkeypatch, cursor)

    with pytest.raises(main.InvalidFeedbackMessage):
        main.save_feedback({"driverId": "driver-1"})

    assert cursor.executed == []
    assert conn.committed is False


@pytest.mark.parametrize("rating", [0, 6, None])
def test_save_feedback_rejects_invalid_rating(monkeypatch, rating):
    cursor = FakeCursor()
    conn = patch_db(monkeypatch, cursor)

    with pytest.raises(main.InvalidFeedbackMessage):
        main.save_feedback({"rideId": "ride-1", "driverId": "driver-1", "rating": rating})

    assert cursor.executed == []
    assert conn.committed is False


# --- process_service_bus_messages ---

def test_process_messages_completes_on_success(monkeypatch):
    cursor = FakeCursor()
    patch_db(monkeypatch, cursor)
    message = FakeMessage(
        json.dumps({"rideId": "ride-1", "driverId": "driver-1", "rating": 5})
    )
    receiver = FakeReceiver([message])
    monkeypatch.setattr(
        main.ServiceBusClient,
        "from_connection_string",
        classmethod(lambda cls, conn_str: FakeServiceBusClient(receiver)),
    )

    main.process_service_bus_messages()

    assert receiver.completed == [message]
    assert receiver.abandoned == []
    assert len(cursor.executed) == 2


def test_process_messages_dead_letters_invalid_rating(monkeypatch):
    cursor = FakeCursor()
    patch_db(monkeypatch, cursor)
    message = FakeMessage(
        json.dumps({"rideId": "ride-1", "driverId": "driver-1", "rating": 9})
    )
    receiver = FakeReceiver([message])
    monkeypatch.setattr(
        main.ServiceBusClient,
        "from_connection_string",
        classmethod(lambda cls, conn_str: FakeServiceBusClient(receiver)),
    )

    main.process_service_bus_messages()

    assert [m for m, _, _ in receiver.dead_lettered] == [message]
    assert receiver.completed == []
    assert receiver.abandoned == []
    assert cursor.executed == []


def test_process_messages_abandons_on_bad_payload(monkeypatch):
    cursor = FakeCursor()
    patch_db(monkeypatch, cursor)
    message = FakeMessage("not-json")
    receiver = FakeReceiver([message])
    monkeypatch.setattr(
        main.ServiceBusClient,
        "from_connection_string",
        classmethod(lambda cls, conn_str: FakeServiceBusClient(receiver)),
    )

    main.process_service_bus_messages()

    assert receiver.abandoned == [message]
    assert receiver.completed == []
    assert cursor.executed == []
