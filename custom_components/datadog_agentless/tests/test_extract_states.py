from os import path, environ
import sys
current_dir = path.dirname(__file__)
parent_dir = path.dirname(current_dir)
sys.path.append(".")
sys.path.append(parent_dir)
import time

from homeassistant.core import Event, EventStateChangedData, EventOrigin, Context, State
from custom_components.datadog_agentless import extract_states, _extract_state

import unittest
import json

def read_context(d: dict) -> Context:
    return Context(
            id=d["id"],
            user_id=d["user_id"],
            parent_id=d["parent_id"],
    )

def read_event(event_raw_data: dict) -> Event[EventStateChangedData]:
        time_fired_timestamp=None
        if "time_fired" in event_raw_data:
            time_fired_timestamp = time.mktime(time.strptime(event_raw_data["time_fired"], "%Y-%m-%dT%H:%M:%S.%f%z"))
        origin=EventOrigin.local
        if "origin" in event_raw_data:
            origin=EventOrigin(event_raw_data["origin"])
        context=None
        if "context" in event_raw_data:
            context = read_context(event_raw_data["context"])

        escd : EventStateChangedData = {
                "old_state": State.from_dict(event_raw_data["data"]["old_state"]),
                "new_state": State.from_dict(event_raw_data["data"]["new_state"]),
                "entity_id": event_raw_data["data"]["entity_id"],
        }

        return Event(
                event_type="EventStateChangedData",
                data=escd,
                context=context,
                origin=origin,
                time_fired_timestamp=time_fired_timestamp
        )



class TestExtractStates(unittest.TestCase):
    def test_value_zero(self):
        event_raw_data = json.loads('{"event_type": "state_changed", "data": {"entity_id": "sensor.offline_zigbee_devices", "old_state": {"entity_id": "sensor.offline_zigbee_devices", "state": "1", "attributes": {"devices": [], "icon": "mdi:zigbee", "friendly_name": "Offline Zigbee Devices", "state_class": "measurement"}, "last_changed": "2025-01-18T09:26:43.652119+00:00", "last_reported": "2025-01-18T09:26:43.745228+00:00", "last_updated": "2025-01-18T09:26:43.652119+00:00", "context": {"id": "01JHWBRWP2220YP0QYBQHGGG02", "parent_id": null, "user_id": null}}, "new_state": {"entity_id": "sensor.offline_zigbee_devices", "state": "0", "attributes": {"devices": [], "icon": "mdi:zigbee", "friendly_name": "Offline Zigbee Devices", "state_class": "measurement"}, "last_changed": "2025-01-18T09:29:27.354286+00:00", "last_reported": "2025-01-18T09:29:27.354757+00:00", "last_updated": "2025-01-18T09:29:27.354286+00:00", "context": {"id": "01JHWBXWHRPRVT7P9SHSBBXZ4Q", "parent_id": null, "user_id": null}}}, "origin": "LOCAL", "time_fired": "2025-01-18T09:29:27.354286+00:00", "context": {"id": "01JHWBXWHRPRVT7P9SHSBBXZ4Q", "parent_id": null, "user_id": null}}')
        event = read_event(event_raw_data)

        states = extract_states(event)
        self.assertEqual(len(states), 1)
        _, _, state = states[0]
        self.assertEqual(state, 0)

    def test_unusal_timeformat(self):
        event_raw_data = json.loads('{"event_type": "state_changed", "data": {"entity_id": "sensor.date_time", "old_state": {"entity_id": "sensor.date_time", "state": "2025-01-18, 14:48", "attributes": {"icon": "mdi:calendar-clock", "friendly_name": "Date & Time"}, "last_changed": "2025-01-18T13:48:00.001236+00:00", "last_reported": "2025-01-18T13:48:00.001920+00:00", "last_updated": "2025-01-18T13:48:00.001236+00:00", "context": {"id": "01JHWTQ9M1N3D9DRKD69211Q71", "parent_id": null, "user_id": null}}, "new_state": {"entity_id": "sensor.date_time", "state": "2025-01-18, 14:49", "attributes": {"icon": "mdi:calendar-clock", "friendly_name": "Date & Time"}, "last_changed": "2025-01-18T13:49:00.002321+00:00", "last_reported": "2025-01-18T13:49:00.002819+00:00", "last_updated": "2025-01-18T13:49:00.002321+00:00", "context": {"id": "01JHWTS4726GQE1520YT16JW75", "parent_id": null, "user_id": null}}}, "origin": "LOCAL", "time_fired": "2025-01-18T13:49:00.002321+00:00", "context": {"id": "01JHWTS4726GQE1520YT16JW75", "parent_id": null, "user_id": null}}')
        environ['TZ'] = 'Europe/Paris'
        event = read_event(event_raw_data)

        states = extract_states(event)
        self.assertEqual(len(states), 1)
        _, _, state = states[0]
        self.assertEqual(state, 1737208140.0)


if __name__ == "__main__":
    unittest.main()
