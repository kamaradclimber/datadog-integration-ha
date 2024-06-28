import logging
from functools import partial
from typing import Optional, Tuple
import datetime
import re
import dateutil.parser
import json
import orjson
from threading import Lock


from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v2.api.metrics_api import MetricsApi
from datadog_api_client.v2.model.metric_content_encoding import MetricContentEncoding
from datadog_api_client.v2.model.metric_intake_type import MetricIntakeType
from datadog_api_client.v2.model.metric_payload import MetricPayload
from datadog_api_client.v2.model.metric_point import MetricPoint
from datadog_api_client.v2.model.metric_series import MetricSeries

from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v1.api.events_api import EventsApi
from datadog_api_client.v1.model.event_create_request import EventCreateRequest

from homeassistant.const import Platform, EVENT_STATE_CHANGED, MATCH_ALL
from homeassistant.helpers.device_registry import EVENT_DEVICE_REGISTRY_UPDATED
from homeassistant.components.automation import EVENT_AUTOMATION_TRIGGERED
from homeassistant.const import __version__ as HAVERSION
import homeassistant.const
from homeassistant.core import HomeAssistant, Event, EventStateChangedData
from homeassistant.config_entries import ConfigEntry
from .const import DOMAIN
from homeassistant.helpers.state import state_as_number
from homeassistant.components.sensor.const import SensorDeviceClass

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    hass.data.setdefault(DOMAIN, {})

    if entry.entry_id not in hass.data[DOMAIN]:
        hass.data[DOMAIN][entry.entry_id] = {}

    # will make sure async_setup_entry from sensor.py is called
    await hass.config_entries.async_forward_entry_setups(entry, [Platform.SENSOR])

    # subscribe to config updates
    entry.async_on_unload(entry.add_update_listener(update_entry))
    
    # we'll use that dictionary to store for each metric the last value and timestamp of last last event
    constant_emitter = ConstantMetricEmitter()
    
    event_listener=partial(full_event_listener, entry.data, constant_emitter)


    unsubscribe = hass.bus.async_listen(EVENT_STATE_CHANGED, event_listener)
    hass.data[DOMAIN][entry.entry_id]["unsubscribe_handler"] = unsubscribe

    all_event_listener=partial(full_all_event_listener, entry.data)
    unsubscribe_all_events = hass.bus.async_listen(MATCH_ALL, all_event_listener)
    hass.data[DOMAIN][entry.entry_id]["unsubscribe_all_event_handler"] = unsubscribe_all_events


    return True


async def update_entry(hass, entry):
    """
    This method is called when options are updated
    We trigger the reloading of entry (that will eventually call async_unload_entry)
    """
    _LOGGER.debug("update_entry method called")
    # will make sure async_setup_entry from sensor.py is called
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """This method is called to clean all sensors before re-adding them"""
    _LOGGER.debug("async_unload_entry method called")
    unload_ok = await hass.config_entries.async_unload_platforms(
        entry, [Platform.SENSOR]
    )
    if "unsubscribe_handler" in hass.data[DOMAIN][entry.entry_id]:
        hass.data[DOMAIN][entry.entry_id]["unsubscribe_handler"]()
    return unload_ok

PREFIX = "hass"


class ConstantMetricEmitter:
    def __init__(self):
        self.last_sent = dict()
        self.last_flush = int(datetime.datetime.now().timestamp())
        self.flush_interval = 900
        self.mutex = Lock()

    def record_last_sent(self, metric: MetricSeries) -> None:
        with self.mutex:
            self.last_sent[(metric.metric, tuple(metric.tags))] = metric
    
    def flush_old_series(self) -> list[MetricSeries]:
        with self.mutex:
            now = int(datetime.datetime.now().timestamp())
            old_series = []
            for metric_id in self.last_sent:
                serie = self.last_sent[metric_id]
                if serie.points[0].timestamp < now - self.flush_interval:
                    serie.points[0].timestamp = now
                    _LOGGER.debug(f"Will flush {serie.metric} {serie.tags}")
                    old_series.append(serie)
            self.last_flush = now
            _LOGGER.info(f"Flushing {len(old_series)} old metrics (out of {len(self.last_sent)}) to make sure we have regular points")
            return old_series

    def should_flush(self) -> bool:
        now = int(datetime.datetime.now().timestamp())
        return self.last_flush < now - self.flush_interval


def ignore_by_entity_id(event: Event[EventStateChangedData]) -> bool:
    new_state = event.data["new_state"]
    assert new_state is not None
    if re.match(".+_version$", new_state.entity_id):
        return True
    if re.match("event.repair", new_state.entity_id):
        return True
    if re.match(".+airing_now_on_.+", new_state.entity_id):
        return True
    if re.match("sensor.time", new_state.entity_id):
        return True
    if re.match("sensor.date_time", new_state.entity_id):
        return True

    return False


# returns None if state cannot be convert to float and thus event should be ignored for metrics
def extract_state(event: Event[EventStateChangedData]) -> Optional[float]:
    new_state = event.data["new_state"]
    if new_state is None:
        return None
    state = new_state.state
    # if it already a number, that's the easy case
    if isinstance(state, (float, int)):
        return state
    # let's ignore "known" string values
    if new_state.state.lower() in ["unavailable", "unknown", "info", "warn", "debug", "error", False, "false", "none", None, "on/off", "off/on", "restore", "up", "down", "stop", "opening", "", "scene_mode", "sunny"]:
        return None

    # we can treat timestamps
    if "device_class" in new_state.attributes and new_state.attributes["device_class"] == SensorDeviceClass.TIMESTAMP:
        try:
            timestamp = datetime.datetime.strptime(new_state.state, "%Y-%m-%dT%H:%M:%S%z").timestamp()
            return timestamp
        except ValueError:
            _LOGGER.warn(f"Unable to parse {new_state.state} as a timestamp")

    if re.match("^input_datetime.+", new_state.entity_id) and new_state.attributes["timestamp"]:
        return new_state.attributes["timestamp"]

    # we can treat things that look like timestamp
    if re.match("20..-..-..T..:..:...+" ,new_state.state):
        try:
            timestamp = dateutil.parser.parse(new_state.state).timestamp()
            return timestamp
        except ValueError:
            _LOGGER.warn(f"Unable to parse {new_state.state} as a timestamp")

    # some values can reasonnably be converted to numeric value
    if new_state.state.lower() in ["unprotected", "dead", "disabled", "inactive"]:
        return 0
    if new_state.state.lower() in ["alive", "ready", "enabled", "pending"]:
        return 1

    # looks like a ssid
    if re.match("..:..:..:..:..:..", new_state.state):
        return None

    # deal with entities whose state are a list of options
    for key in ["operation_list", "options"]:
        if key in new_state.attributes and new_state.state in new_state.attributes[key]:
            return None

    try:
        state_as_number(new_state)
    except:
        # we cannot treat this kind of event

        if ignore_by_entity_id(event):
            return None

        _LOGGER.warn(f"Cannot treat this state changed event: {event} to convert to metric")
        return None
    return state_as_number(new_state)

def full_event_listener(creds: dict, constant_emitter: ConstantMetricEmitter, event: Event[EventStateChangedData]):
    new_state = event.data["new_state"]
    if new_state is None:
        _LOGGER.warn(f"This event has no new state, isn't it strange?. Event is {event}")
        return
    domain = new_state.domain if new_state.domain else new_state.entity_id.split(".")[0]
    metric_name = f"{PREFIX}.{domain}".replace(" ", "_")
    value = extract_state(event)
    if value is None:
        return

    tags = [f"entity:{new_state.entity_id}", "service:home-assistant", f"version:{HAVERSION}", f"env:{creds['env']}"]
    unit = None
    if "friendly_name" in new_state.attributes:
        tags.append(f"friendly_name:{new_state.attributes['friendly_name']}")
        unit = (new_state.attributes["unit_of_measurement"] if "unit_of_measurement" in new_state.attributes else None)
    timestamp = new_state.last_changed
    if value is None:
        # Ignore event if state is not number and cannot be converted to a number
        return
    if value is not None:
        metric_serie = MetricSeries(metric=metric_name, type=MetricIntakeType.GAUGE, tags=tags, unit=unit,
                           points=[MetricPoint(timestamp=int(timestamp.timestamp()), value=value)])
        series = [metric_serie]
        constant_emitter.record_last_sent(metric_serie)
        if constant_emitter.should_flush():
            old_series = constant_emitter.flush_old_series()
            for serie in old_series:
                series.append(serie)


        payload = MetricPayload(series=series)
        configuration = Configuration(
                api_key={"apiKeyAuth": creds["api_key"],
                         },
                server_variables={"site": creds["site"]},
                request_timeout=5,
                enable_retry=True,
        )
        with ApiClient(configuration) as api_client:
            api_instance = MetricsApi(api_client)
            response = api_instance.submit_metrics(content_encoding=MetricContentEncoding.ZSTD1, body=payload)
            if len(response["errors"]) > 0:
                _LOGGER.error(f"Error sending metric to Datadog {response['errors'][0]}")

def full_all_event_listener(creds: dict, event: Event):
    if event.event_type == "state_changed":
        if extract_state(event) is not None:
            # those events will be converted to metric, no need to double send them
            # in addition we expect events about "metrics" to change more often than the others
            return

    tags = ["service:home-assistant", f"version:{HAVERSION}", f"event_type:{event.event_type}", f"env:{creds['env']}"]
    if event.event_type == homeassistant.const.EVENT_STATE_CHANGED:
        text=json.dumps(orjson.loads(orjson.dumps(event.json_fragment)))
    else:
        text=json.dumps(event.as_dict())
    title, event_specific_tags = generate_message(event)
    event_request = EventCreateRequest(
            date_happened=int(event.time_fired.timestamp()),
            title=title,
            text=text,
            tags=tags + event_specific_tags,
    )

    configuration = Configuration(
            api_key={"apiKeyAuth": creds["api_key"],
                     },
            server_variables={"site": creds["site"]},
            request_timeout=5,
            enable_retry=True,
            )
    with ApiClient(configuration) as api_client:
        api_instance = EventsApi(api_client)
        response = api_instance.create_event(body=event_request)
        if response.status != "ok":
            _LOGGER.error(f"Error sending event to Datadog {response["errors"]}")

def generate_message(event: Event) -> Tuple[str, list[str]]:
    tags = []
    def enrich(data_key):
        if data_key in event.data:
            tags.append(f"{data_key}:{event.data[data_key]}")

    tags.append(f"origin:{event.origin}")
    tags.append(f"user_id:{event.context.user_id}")
    tags.append(f"parent_id:{event.context.parent_id}")
    if event.event_type == homeassistant.const.EVENT_CALL_SERVICE:
        enrich("domain")
        enrich("service")
        return ("Called service", tags)
    elif event.event_type == homeassistant.const.EVENT_COMPONENT_LOADED:
        enrich("component")
        return ("Loaded component", tags)
    elif event.event_type == homeassistant.const.EVENT_SERVICE_REGISTERED:
        enrich("domain")
        enrich("service")
        return ("Registered a service", tags)
    elif event.event_type == homeassistant.const.EVENT_STATE_CHANGED:
        if "old_state" in event.data and event.data["old_state"] is not None:
            tags.append(f"entity_id:{event.data["old_state"].entity_id}")
        if "new_state" in event.data and event.data["new_state"] is not None:
            tags.append(f"entity_id:{event.data["new_state"].entity_id}")
        state_tag = build_state_tag(event)
        if state_tag is not None:
            tags.append(f"new_state:{state_tag}")
        return ("State changed", list(set(tags)))
    elif event.event_type == EVENT_DEVICE_REGISTRY_UPDATED:
        enrich("device_id")
        enrich("action")
        return ("device_registry_updated", tags)
    elif event.event_type == EVENT_AUTOMATION_TRIGGERED:
        enrich("source")
        enrich("entity_id")
        enrich("name")
        return ("automation_triggered", tags)
    else:
        return (str(event.event_type), tags)

def build_state_tag(event) -> Optional[str]:
    if extract_state(event) is not None:
        return None
    if "new_state" not in event.data or event.data["new_state"] is None:
        return None
    new_state = event.data["new_state"]
    if "device_class" in new_state.attributes and new_state.attributes["device_class"] == SensorDeviceClass.TIMESTAMP:
        return None
    for key in ["operation_list", "options"]:
        if key in new_state.attributes and new_state.state in new_state.attributes[key]:
            return new_state.state
    if "icon" in new_state.attributes and re.match(".*clock.*", new_state.attributes["icon"]):
        return None
    if new_state.state == "":
        return None
    return new_state.state

async def async_migrate_entry(hass, config_entry: ConfigEntry):
    if config_entry.version == 1:
        _LOGGER.warn("config entry is version 1, migrating to version 2")
        new = {**config_entry.data}
        new["env"] = "prod"
        hass.config_entries.async_update_entry(config_entry, data=new, version=2)
