from __future__ import annotations

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfTemperature
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from . import AlphaSmartCoordinator
from .const import CONF_DEVICE_IDS, CONF_DEVICES, DOMAIN


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Add AlphaSmartClimate entities from a config_entry."""
    coordinator: AlphaSmartCoordinator = hass.data[DOMAIN][config_entry.entry_id]
    collect = []
    device_ids = list(
        config_entry.options.get(
            CONF_DEVICE_IDS,
            hass.data[DOMAIN]["data"].get(CONF_DEVICE_IDS, []),
        )
    )
    devices = hass.data[DOMAIN]["data"].get(CONF_DEVICES, [])
    device_map = {device["deviceId"]: device for device in devices}
    for device_id in device_ids:
        if device_id not in coordinator.data:
            continue
        device = device_map.get(device_id, {})
        if device.get("oem") and device.get("oem") not in {"Moehlenhoff", "alphaSmart"}:
            continue
        if device.get("type") in {"gateway", "baseStation"}:
            continue
        if device.get("isGateway") or device.get("isBaseStation"):
            continue
        data = coordinator.data[device_id]
        if "31" in data:
            collect.append(AlphaSmartSensor(coordinator, device_id, "temperature"))
        if "33" in data:
            collect.append(AlphaSmartSensor(coordinator, device_id, "humidity"))
    async_add_entities(collect)


class AlphaSmartSensor(CoordinatorEntity[AlphaSmartCoordinator], SensorEntity):
    """Alpha Smart SensorEntity."""

    _attr_state_class = SensorStateClass.MEASUREMENT
    type: str

    def __init__(
        self, coordinator: AlphaSmartCoordinator, device_id: str, type: str
    ) -> None:
        """Initialize Alpha Smart SensorEntity."""
        super().__init__(coordinator)
        self._attr_unique_id = device_id + "_" + type
        self._attr_name = coordinator.data[device_id]["name"] + " " + type
        self.type = type

    @property
    def device_class(self) -> str:
        """Return the device class of the sensor."""
        if self.type == "temperature":
            return SensorDeviceClass.TEMPERATURE
        return SensorDeviceClass.HUMIDITY

    @property
    def native_unit_of_measurement(self) -> str:
        """Return the unit of measurement."""
        if self.type == "temperature":
            return UnitOfTemperature.CELSIUS
        return "%"

    @property
    def native_value(self) -> float:
        """Return the current humidity."""
        if self.type == "temperature":
            return self.coordinator.data[self.unique_id.removesuffix("_" + self.type)][
                "31"
            ]
        return self.coordinator.data[self.unique_id.removesuffix("_" + self.type)]["33"]
