# Datadog Agentless integration for Home Assistant

This integration is sending metrics and events from Home Assistant to Datadog.
As opposed to the official datadog integration, it contacts the datadog api directly, which removes the need to run a datadog agent along side HA.

The code is a fork of https://github.com/tjschiffer/hassio-addons 🙇.


## Installation

Installation is done through hacs (pending integration in default repository, the installation can be done as a custom repository).

1. Go to HACS page on your Home Assistant instance
1. Add this repository (https://github.com/kamaradclimber/datadog-integration-ha) via HACS Custom repositories [How to add Custom Repositories](https://hacs.xyz/docs/faq/custom_repositories/)
1. Select `Integration`
1. Press add icon and search for `Datadog`
1. Restart Home Assistant
1. Add `datadog` integration

[![Open your Home Assistant instance and open a repository inside the Home Assistant Community Store.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=kamaradclimber&repository=datadog-integration-ha&category=integration)


## Configuration

Add a new integration "datadog-agentless". You'll be asked to give an API key and the site (datadoghq.com or datadoghq.eu for instance).
You'll also need to configure an environment, this will be passed as the `env` tag on all events/metrics. Suggested value is `prod`.

## Usage

This integration does not expose any sensor at the moment. It sends metrics to datadog under the prefix `hass` for instance `hass.sensor`. It also sends events that cannot be converted to metrics as event.

It adds a few tags such as `service:home-assistant` and `version:<your ha version>`.
