import time
import random

EVENT_TYPES = ["product_view", "add_to_cart", "purchase"]

def generate_test_event():
    return {
        "user_id": f"user_{random.randint(1, 10)}",
        "event_type": random.choice(EVENT_TYPES),
        "product_id": f"product_{random.randint(1, 50)}",
        "timestamp": time.time()
    }

def test_event_has_required_fields():
    event = generate_test_event()

    assert "user_id" in event
    assert "event_type" in event
    assert "product_id" in event
    assert "timestamp" in event

def test_event_type_valid():
    event = generate_test_event()

    assert event["event_type"] in EVENT_TYPES

def test_ids_are_strings():
    event = generate_test_event()

    assert isinstance(event["user_id"], str)
    assert isinstance(event["product_id"], str)

def test_timestamp_is_float():
    event = generate_test_event()

    assert isinstance(event["timestamp"], float)