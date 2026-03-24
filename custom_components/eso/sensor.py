from __future__ import annotations

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

_LOGGER = logging.getLogger(__name__)

DOMAIN = "eso"
CONF_OBJECTS = "objects"
CONF_NAME = "name"
CONF_ID = "id"
CONF_CONSUMED = "consumed"
CONF_RETURNED = "returned"
CONF_PRICE_ENTITY = "price_entity"
CONF_PRICE_CURRENCY = "price_currency"

POWER_CONSUMED = "P+"
POWER_RETURNED = "P-"

DATA_TYPE_MAP = {
    CONF_CONSUMED: POWER_CONSUMED,
    CONF_RETURNED: POWER_RETURNED,
}


GRANULARITY_HOURLY = "hourly"
GRANULARITY_MONTHLY = "monthly"


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    sensors: list[SensorEntity] = []
    for obj in entry.data[CONF_OBJECTS]:
        for data_type, series_key in DATA_TYPE_MAP.items():
            if not obj.get(data_type, data_type == CONF_CONSUMED):
                continue
            sensors.append(ESOSensor(entry, obj, data_type, series_key, GRANULARITY_HOURLY))
            sensors.append(ESOSensor(entry, obj, data_type, series_key, GRANULARITY_MONTHLY))
        price_entity = obj.get(CONF_PRICE_ENTITY, "")
        if price_entity:
            sensors.append(ESOCostSensor(entry, obj))

    async_add_entities(sensors)
    hass.data[DOMAIN][entry.entry_id]["sensors"] = sensors


class ESOSensor(SensorEntity):
    _attr_device_class = SensorDeviceClass.ENERGY
    _attr_state_class = SensorStateClass.TOTAL_INCREASING
    _attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
    _attr_has_entity_name = True
    _attr_should_poll = False

    def __init__(
        self,
        entry: ConfigEntry,
        obj: dict,
        data_type: str,
        series_key: str,
        granularity: str,
    ) -> None:
        self._entry = entry
        self._obj = obj
        self._data_type = data_type
        self._series_key = series_key
        self._granularity = granularity
        obj_id = obj[CONF_ID]
        self._attr_unique_id = f"eso_{obj_id}_{data_type}_{granularity}"
        self._attr_name = f"{obj[CONF_NAME]} ({data_type} {granularity})"
        self._attr_native_value: float | None = None

    def update_from_dataset(self, dataset: dict) -> None:
        series = dataset.get(self._series_key)
        if not series:
            return
        if self._granularity == GRANULARITY_MONTHLY:
            sorted_ts = sorted(series.keys())
            non_zero = [(ts, series[ts]) for ts in sorted_ts if series[ts] > 0]
            if not non_zero:
                return
            latest_ts, value = non_zero[-1]
        else:
            latest_ts = max(series.keys())
            value = series[latest_ts]
        self._attr_native_value = value
        _LOGGER.debug(
            "Updated sensor %s: %s kWh at %s",
            self._attr_unique_id,
            self._attr_native_value,
            datetime.fromtimestamp(latest_ts, tz=ZoneInfo("Europe/Vilnius")).isoformat(),
        )
        self.async_write_ha_state()


class ESOCostSensor(SensorEntity):
    _attr_state_class = SensorStateClass.TOTAL_INCREASING
    _attr_has_entity_name = True
    _attr_should_poll = False

    def __init__(self, entry: ConfigEntry, obj: dict) -> None:
        self._entry = entry
        self._obj = obj
        obj_id = obj[CONF_ID]
        currency = obj.get(CONF_PRICE_CURRENCY, "EUR")
        self._attr_unique_id = f"eso_{obj_id}_cost"
        self._attr_name = f"{obj[CONF_NAME]} (cost)"
        self._attr_native_unit_of_measurement = currency
        self._attr_native_value: float | None = None

    def update_cost(self, cost_sum: float) -> None:
        self._attr_native_value = cost_sum
        self.async_write_ha_state()
