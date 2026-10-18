"""
Test persistence and previous-state snapshots with manual server restarts.

Run the version 3 server in another terminal before starting this script.
Run: python test_persistence.py
"""

import json
import uuid
from pathlib import Path

import requests

BASE_URL = "http://127.0.0.1:8080"
SNAPSHOT_FILE = Path(__file__).resolve().parent / "data" / "snapshot.json"
REQUEST_TIMEOUT = 5


def check_response(method, key, expected_status, expected_body, value=None):
    """Send a request and verify its status and JSON body."""
    kwargs = {}
    if method == "PUT":
        kwargs["json"] = {"value": value}

    response = requests.request(
        method,
        f"{BASE_URL}/{key}",
        timeout=REQUEST_TIMEOUT,
        **kwargs,
    )

    if response.status_code != expected_status:
        raise AssertionError(
            f"{method}: expected status {expected_status}, "
            f"got {response.status_code}: {response.text[:200]}"
        )

    if response.json() != expected_body:
        raise AssertionError(
            f"{method}: unexpected response body: {response.text[:200]}"
        )


def check_snapshot(key, expected_value):
    """Verify that the snapshot contains the previous value."""
    with SNAPSHOT_FILE.open("r", encoding="utf-8") as file:
        snapshot = json.load(file)

    if key not in snapshot or snapshot[key] != expected_value:
        raise AssertionError(f"Snapshot does not contain {key!r} = {expected_value!r}")


def restart_server():
    print("\nIn the SERVER terminal:")
    print("1. Press Ctrl+C to stop the server.")
    print("2. Run python server.py again.")
    print("3. Wait for 'Application startup complete'.")
    input("Then return here and press Enter to continue: ")


def main():
    key = f"persistence_test_{uuid.uuid4().hex}"
    print(f"Test key: {key}")
    print("Keep other clients and benchmarks stopped during this test.")

    try:
        # Create data and verify it survives a restart.
        check_response(
            "PUT",
            key,
            201,
            {"key": key, "value": "Kevin"},
            value="Kevin",
        )
        print("\nPASS: initial value saved.")

        restart_server()

        check_response(
            "GET",
            key,
            200,
            {"key": key, "value": "Kevin"},
        )
        print("PASS: initial value survived the restart.")

        # Update data and verify that the snapshot retains the old value.
        check_response(
            "PUT",
            key,
            200,
            {"key": key, "value": "Alice"},
            value="Alice",
        )
        check_snapshot(key, "Kevin")
        print("\nPASS: value updated; snapshot contains the previous value.")

        restart_server()

        check_response(
            "GET",
            key,
            200,
            {"key": key, "value": "Alice"},
        )
        check_snapshot(key, "Kevin")
        print("PASS: updated value and snapshot survived the restart.")

        # Delete data and verify that the deletion also persists.
        check_response(
            "DELETE",
            key,
            200,
            {"key": key, "deleted": True},
        )
        check_snapshot(key, "Alice")
        print("\nPASS: value deleted; snapshot contains its previous value.")

        restart_server()

        check_response(
            "GET",
            key,
            404,
            {"error": f"Key '{key}' not found"},
        )
        check_snapshot(key, "Alice")
        print("PASS: deletion and snapshot survived the restart.")

    except (requests.RequestException, OSError, ValueError, AssertionError) as exc:
        print(f"\nFAIL: {exc}")
        print("Check the server terminal and saved files before retrying.")
        return 1

    print("\nAll persistence checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
