import logging
from functools import partial
from typing import Optional


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

from homeassistant.const import Platform, EVENT_STATE_CHANGED, __version__
from homeassistant.core import HomeAssistant, Event, EventStateChangedData, State
from homeassistant.config_entries import ConfigEntry
from .const import DOMAIN
from datetime import datetime
from homeassistant.helpers.storage import Store
from homeassistant.helpers.state import state_as_number

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    hass.data.setdefault(DOMAIN, {})

    if entry.entry_id not in hass.data[DOMAIN]:
        hass.data[DOMAIN][entry.entry_id] = {}

    # will make sure async_setup_entry from sensor.py is called
    await hass.config_entries.async_forward_entry_setups(entry, [Platform.SENSOR])

    # subscribe to config updates
    entry.async_on_unload(entry.add_update_listener(update_entry))
    
    event_listener=partial(full_event_listener, entry.data)


    unsubscribe = hass.bus.async_listen(EVENT_STATE_CHANGED, event_listener)
    hass.data[DOMAIN][entry.entry_id]["unsubscribe_handler"] = unsubscribe


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

def extract_state(event: Event[EventStateChangedData]) -> Optional[float]:
    new_state = event.data["new_state"]
    state = new_state.state
    try:
        state_as_number(new_state)
    except:
        # we cannot treat this kind of event
        _LOGGER.warn(f"Cannot treat this state changed event: {event} to convert to metric")
        return None
    if isinstance(state, (float, int)):
        return state
    return state_as_number(new_state)

def full_event_listener(creds: dict, event: Event[EventStateChangedData]):
    new_state = event.data["new_state"]
    if new_state is None:
        _LOGGER.warn(f"This event has no new state, isn't it strange?. Event is {event}")
        return
    domain = new_state.domain if new_state.domain else new_state.entity_id.split(".")[0]
    metric_name = f"{PREFIX}.{domain}".replace(" ", "_")
    value = extract_state(event)
    if value is None:
        return

    tags = [f"entity:{new_state.entity_id}", "service:home-assistant", f"version:{__version__}"]
    unit = None
    if "friendly_name" in new_state.attributes:
        tags.append(f"friendly_name:{new_state.attributes['friendly_name']}")
        unit = (new_state.attributes["unit_of_measurement"] if "unit_of_measurement" in new_state.attributes else None)
    timestamp = new_state.last_changed
    if value is None:
        # Ignore event if state is not number and cannot be converted to a number
        return
    if value is not None:
        payload = MetricPayload(series=[
            MetricSeries(metric=metric_name, type=MetricIntakeType.GAUGE, tags=tags, unit=unit,
                           points=[MetricPoint(timestamp=int(timestamp.timestamp()), value=value)])
        ])
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
