import logging
from typing import Any, Optional, Tuple
import voluptuous as vol
from homeassistant.core import callback, HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
import homeassistant.helpers.config_validation as cv
from homeassistant import config_entries
from .const import (
    DOMAIN,
)
from homeassistant import config_entries

_LOGGER = logging.getLogger(__name__)

# Description of the config flow:
# async_step_user is called when user starts to configure the integration
# we follow with a flow of form/menu
# eventually we call async_create_entry with a dictionnary of data
# HA calls async_setup_entry with a ConfigEntry which wraps this data (defined in __init__.py)
# in async_setup_entry we call hass.config_entries.async_forward_entry_setups to setup each relevant platform (sensor in our case)
# HA calls async_setup_entry from sensor.py


class SetupConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    def __init__(self):
        """Initialize"""
        self.data = {}

    async def async_step_user(self, user_input: Optional[dict[str, Any]] = None):
        return self.async_create_entry(title="Datadog via API", data=self.data)
