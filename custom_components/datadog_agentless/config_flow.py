import logging
from typing import Any, Optional, Tuple
import voluptuous as vol
from datadog_api_client import AsyncApiClient, Configuration
from datadog_api_client.v2.api.metrics_api import MetricsApi


from homeassistant.exceptions import ConfigEntryAuthFailed, HomeAssistantError
from homeassistant.core import callback, HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
import homeassistant.helpers.config_validation as cv
from homeassistant import config_entries
from .const import (
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)

# Description of the config flow:
# async_step_user is called when user starts to configure the integration
# we follow with a flow of form/menu
# eventually we call async_create_entry with a dictionnary of data
# HA calls async_setup_entry with a ConfigEntry which wraps this data (defined in __init__.py)
# in async_setup_entry we call hass.config_entries.async_forward_entry_setups to setup each relevant platform (sensor in our case)
# HA calls async_setup_entry from sensor.py


class SetupConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 2

    def __init__(self):
        """Initialize"""
        self.data = {}

    async def async_step_user(self, user_input: Optional[dict[str, Any]] = None):
        if user_input is not None:

            configuration = Configuration(
                    api_key={"apiKeyAuth": user_input["api_key"],
                             },
                    server_variables={"site": user_input["site"]},
                    request_timeout=5,
                    enable_retry=True,
            )
            try:
                #async with AsyncApiClient(configuration) as api_client:
                #    client = MetricsApi(api_client)
                #    await client.list_tag_configurations()
                pass
            except Exception as err:
                raise ConfigEntryAuthFailed(err) from err
            
            await self.async_set_unique_id(user_input["api_key"])
            self._abort_if_unique_id_configured()
            return self.async_create_entry(title="Datadog via API", data=user_input)
        return self.async_show_form(
            step_id="user", data_schema=vol.Schema({
                vol.Required("api_key"): str,
                vol.Required("site"): str,
                vol.Required("env"): str,
            })
        )
