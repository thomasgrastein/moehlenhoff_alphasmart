from homeassistant.components.climate import ClimateEntity, ClimateEntityFeature
from homeassistant.components.climate.const import HVACMode
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import ATTR_TEMPERATURE, UnitOfTemperature
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from . import AlphaSmartCoordinator
from .const import CONF_DEVICE_IDS, CONF_DEVICES, DOMAIN, PRESET_AUTO, PRESET_MANUAL


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
        if not any(key in data for key in ("30", "31")):
            continue
        collect.append(AlphaSmartClimate(coordinator, device_id))
    async_add_entities(collect)


class AlphaSmartClimate(CoordinatorEntity[AlphaSmartCoordinator], ClimateEntity):
    """Alpha Smart ClimateEntity."""

    target_temperature_step = 0.1

    _attr_supported_features = (
        ClimateEntityFeature.TARGET_TEMPERATURE | ClimateEntityFeature.PRESET_MODE
    )
    _attr_temperature_unit = UnitOfTemperature.CELSIUS
    _attr_preset_modes = [PRESET_AUTO, PRESET_MANUAL]
    _attr_hvac_modes = [HVACMode.HEAT]

    def __init__(self, coordinator: AlphaSmartCoordinator, device_id: str) -> None:
        """Initialize Alpha Smart ClimateEntity."""
        super().__init__(coordinator)
        self._attr_unique_id = device_id
        self._attr_name = coordinator.data[self.unique_id]["name"]

    @property
    def min_temp(self) -> float:
        """Return the minimum temperature."""
        return float(0)

    @property
    def max_temp(self) -> float:
        """Return the maximum temperature."""
        return float(40)

    @property
    def current_temperature(self) -> float:
        """Return the current temperature."""
        return self.coordinator.data[self.unique_id]["31"]

    @property
    def target_temperature(self) -> float:
        """Return the temperature we try to reach."""
        return self.coordinator.data[self.unique_id]["30"]

    @property
    def preset_mode(self) -> str:
        """Return the current preset mode."""
        return PRESET_MANUAL

    @property
    def hvac_mode(self) -> HVACMode:
        """Return current hvac mode."""
        return HVACMode.HEAT

    @property
    def current_humidity(self) -> float:
        """Return the current humidity."""
        return self.coordinator.data[self.unique_id]["33"]

    async def async_set_temperature(self, **kwargs) -> None:
        """Set new target temperature."""
        temperature = kwargs.get(ATTR_TEMPERATURE)
        if temperature is None:
            return
        await self.coordinator.async_set_target_temperature(self.unique_id, temperature)
