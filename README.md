# Datadog Agentless integration for Home Assistant

This integration is sending metrics and events from Home Assistant to Datadog.
As opposed to the official datadog integration, it contacts the datadog api directly, which removes the need to run a datadog agent along side HA.

The code is a fork of https://github.com/tjschiffer/hassio-addons 🙇.


## Installation

Installation is done through hacs (pending integration in default repository, the installation can be done as a custom repository).

## Configuration

Add a new integration "datadog-agentless". You'll be asked to give an API key and the site (datadoghq.com or datadoghq.eu for instance).

## Usage

This integration does not expose any sensor at the moment. It sends metrics to datadog under the prefix `hass` for instance `hass.sensor`. It also sends events that cannot be converted to metrics as event.

It adds a few tags such as `service:home-assistant` and `version:<your ha version>`.
