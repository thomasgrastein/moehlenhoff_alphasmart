from __future__ import annotations

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.const import UnitOfTemperature
from homeassistant.core import DOMAIN, HomeAssistant
from homeassistant.helpers.config_validation import config_entry_only_config_schema
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from . import AlphaSmartCoordinator


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: config_entry_only_config_schema,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Add AlphaSmartClimate entities from a config_entry."""
    coordinator: AlphaSmartCoordinator = hass.data[DOMAIN][config_entry.entry_id]
    collect = []
    for device in hass.data[DOMAIN]["data"]["devices"]:
        if device["oem"] == "Moehlenhoff" and device["deviceId"] not in [
            "5f5ad901-e014-4c55-94c9-fd8d4e96bf0d",
            "c32401c5-a65c-4be8-8099-187a5bfea52a",
        ]:
            collect.append(
                AlphaSmartSensor(coordinator, device["deviceId"], "temperature")
            )
            collect.append(
                AlphaSmartSensor(coordinator, device["deviceId"], "humidity")
            )
    async_add_entities(collect)


class AlphaSmartSensor(CoordinatorEntity[AlphaSmartCoordinator], SensorEntity):
    """Alpha Smart SensorEntity."""

    _attr_state_class = SensorStateClass.MEASUREMENT

    def __init__(
        self, coordinator: AlphaSmartCoordinator, device_id: str, type: str
    ) -> None:
        """Initialize Alpha Smart SensorEntity."""
        super().__init__(coordinator)
        self._attr_unique_id = device_id + "_" + type
        self._attr_name = coordinator.data[self.unique_id]["name"] + " " + type
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
            return self.coordinator.data[self.unique_id]["31"]
        return self.coordinator.data[self.unique_id]["33"]