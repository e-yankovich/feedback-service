import os
import json
import uuid
import asyncio
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import pyodbc
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from azure.servicebus import ServiceBusClient

load_dotenv()

DB_SERVER = os.getenv("DB_SERVER")
DB_DATABASE = os.getenv("DB_DATABASE")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")

SERVICE_BUS_QUEUE_NAME = os.getenv("SERVICE_BUS_QUEUE_NAME")
SERVICE_BUS_LISTEN_CONNECTION_STRING = os.getenv("SERVICE_BUS_LISTEN_CONNECTION_STRING")

SCHEMA_NAME = "EvgeniyaYankovich"
TABLE_NAME = "Feedbacks"

POLL_INTERVAL_SECONDS = 15

CONNECTION_STRING = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={DB_SERVER};"
    f"DATABASE={DB_DATABASE};"
    f"UID={DB_USERNAME};"
    f"PWD={DB_PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)


class InvalidFeedbackMessage(Exception):
    pass


def get_db_connection():
    return pyodbc.connect(CONNECTION_STRING)


def init_db():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        cursor.execute(
            f"""
            IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{SCHEMA_NAME}')
                EXEC('CREATE SCHEMA [{SCHEMA_NAME}]');
            """
        )

        cursor.execute(
            f"""
            IF NOT EXISTS (
                SELECT 1 FROM sys.tables t
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = '{SCHEMA_NAME}' AND t.name = '{TABLE_NAME}'
            )
            CREATE TABLE [{SCHEMA_NAME}].[{TABLE_NAME}] (
                FeedbackId UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
                RideId UNIQUEIDENTIFIER NOT NULL,
                DriverId UNIQUEIDENTIFIER NOT NULL,
                PassengerId UNIQUEIDENTIFIER NULL,
                Rating INT NULL,
                Comment NVARCHAR(MAX) NULL,
                SubmittedAt DATETIME2 NOT NULL
            );
            """
        )

        conn.commit()
        print(f"[init_db] Schema [{SCHEMA_NAME}] and table [{TABLE_NAME}] ready.")
    finally:
        conn.close()


def save_feedback(message_body: dict):
    ride_id = message_body.get("rideId")
    driver_id = message_body.get("driverId")
    passenger_id = message_body.get("passengerId")
    rating = message_body.get("rating")
    comment = message_body.get("comment")

    if not ride_id or not driver_id:
        raise InvalidFeedbackMessage(f"missing rideId/driverId: {message_body}")

    if rating is None or rating < 1 or rating > 5:
        raise InvalidFeedbackMessage(f"rating must be between 1 and 5, got {rating}")

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT 1 FROM [{SCHEMA_NAME}].[{TABLE_NAME}] WHERE RideId = ?",
            ride_id,
        )
        if cursor.fetchone() is not None:
            print(f"[save_feedback] Feedback for ride {ride_id} already exists, skipping.")
            return
        cursor.execute(
            f"""
            INSERT INTO [{SCHEMA_NAME}].[{TABLE_NAME}]
                (FeedbackId, RideId, DriverId, PassengerId, Rating, Comment, SubmittedAt)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            str(uuid.uuid4()),
            ride_id,
            driver_id,
            passenger_id,
            rating,
            comment,
            datetime.now(timezone.utc),
        )
        conn.commit()
        print(f"[save_feedback] Saved feedback for ride {ride_id}, driver {driver_id}.")
    finally:
        conn.close()


def process_service_bus_messages():
    with ServiceBusClient.from_connection_string(
        SERVICE_BUS_LISTEN_CONNECTION_STRING
    ) as client:
        with client.get_queue_receiver(
            queue_name=SERVICE_BUS_QUEUE_NAME, max_wait_time=5
        ) as receiver:
            for message in receiver:
                try:
                    raw = str(message)
                    print(f"[service_bus] Received message from queue: {raw}")
                    body = json.loads(raw)
                    save_feedback(body)
                    receiver.complete_message(message)
                except InvalidFeedbackMessage as exc:
                    print(f"[service_bus] Dead-lettering invalid message: {exc}")
                    receiver.dead_letter_message(
                        message,
                        reason="InvalidFeedback",
                        error_description=str(exc),
                    )
                except Exception as exc:
                    print(f"[service_bus] Failed to process message: {exc}")
                    receiver.abandon_message(message)


async def service_bus_listener():
    while True:
        try:
            await asyncio.to_thread(process_service_bus_messages)
        except Exception as exc:
            print(f"[service_bus_listener] Error polling queue: {exc}")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    listener_task = asyncio.create_task(service_bus_listener())
    try:
        yield
    finally:
        listener_task.cancel()
        try:
            await listener_task
        except asyncio.CancelledError:
            pass


app = FastAPI(title="FeedbackService", lifespan=lifespan)


@app.get("/feedback/driver/{driver_id}")
def get_feedback_for_driver(driver_id: str):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT FeedbackId, RideId, DriverId, PassengerId, Rating, Comment, SubmittedAt
            FROM [{SCHEMA_NAME}].[{TABLE_NAME}]
            WHERE DriverId = ?
            ORDER BY SubmittedAt DESC
            """,
            driver_id,
        )
        rows = cursor.fetchall()
    except pyodbc.Error as exc:
        raise HTTPException(status_code=500, detail=f"Database error: {exc}")
    finally:
        conn.close()

    return [
        {
            "feedbackId": str(row.FeedbackId),
            "rideId": str(row.RideId),
            "driverId": str(row.DriverId),
            "passengerId": str(row.PassengerId) if row.PassengerId else None,
            "rating": row.Rating,
            "comment": row.Comment,
            "submittedAt": row.SubmittedAt.isoformat() if row.SubmittedAt else None,
        }
        for row in rows
    ]


@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)