from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMeanType,
    StatisticMetaData,
)
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    statistics_during_period,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    CONF_PASSWORD,
    CONF_USERNAME,
    EVENT_HOMEASSISTANT_STARTED,
    UnitOfEnergy,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.event import async_track_time_change
from homeassistant.util import dt as dt_util

from .eso_client import ESOClient

_LOGGER = logging.getLogger(__name__)
DOMAIN = "eso"
CONF_OBJECTS = "objects"
CONF_NAME = "name"
CONF_ID = "id"
CONF_CONSUMED = "consumed"
CONF_RETURNED = "returned"
CONF_COST = "cost"
CONF_PRICE_ENTITY = "price_entity"
CONF_PRICE_CURRENCY = "price_currency"
CONF_RETENTION_DAYS = "retention_days"
POWER_CONSUMED = "P+"
POWER_RETURNED = "P-"
ENERGY_TYPE_MAP = {
    CONF_CONSUMED: POWER_CONSUMED,
    CONF_RETURNED: POWER_RETURNED,
}
RETRY_DELAY_SECONDS = 3 * 3600


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    hass.data.setdefault(DOMAIN, {})
    client = ESOClient(
        username=entry.data[CONF_USERNAME],
        password=entry.data[CONF_PASSWORD],
    )
    hass.data[DOMAIN][entry.entry_id] = {"client": client, "sensors": []}

    await hass.config_entries.async_forward_entry_setups(entry, ["sensor"])

    async def async_import_generation(
        now: datetime, retry: bool = False, backfill: bool = False
    ) -> None:
        if hass.is_stopping:
            return
        all_failed = False
        try:
            _LOGGER.info("Logging in to ESO...")
            await hass.async_add_executor_job(client.login)
        except Exception as e:
            _LOGGER.error("ESO login error: %s", e)
            all_failed = True

        sensors: list = hass.data[DOMAIN][entry.entry_id].get("sensors", [])

        for obj in entry.data[CONF_OBJECTS]:
            if backfill:
                _LOGGER.info("Backfilling ESO hourly dataset [%s] (12 weeks)", obj[CONF_NAME])
                try:
                    dataset = await hass.async_add_executor_job(
                        client.fetch_dataset_backfill,
                        obj[CONF_ID],
                        now,
                        12,
                    )
                except Exception as e:
                    _LOGGER.error("ESO backfill error [%s]: %s", obj[CONF_NAME], e)
                    all_failed = True
                    continue
            else:
                _LOGGER.info("Fetching ESO hourly dataset [%s]", obj[CONF_NAME])
                try:
                    await hass.async_add_executor_job(
                        client.fetch_dataset,
                        obj[CONF_ID],
                        now,
                    )
                except Exception as e:
                    _LOGGER.error("ESO fetch dataset error [%s]: %s", obj[CONF_NAME], e)
                    all_failed = True
                    continue
                dataset = client.get_dataset(obj[CONF_ID])

            await async_insert_statistics(hass, entry, obj, dataset)
            if obj.get(CONF_PRICE_ENTITY):
                await async_insert_cost_statistics(hass, entry, obj, dataset)

            for sensor in sensors:
                if (
                    hasattr(sensor, "update_from_dataset")
                    and sensor._obj[CONF_ID] == obj[CONF_ID]
                    and getattr(sensor, "_granularity", None) == "hourly"
                ):
                    sensor.update_from_dataset(dataset)

            years_to_fetch = [now.year, now.year - 1, now.year - 2] if backfill else [now.year, now.year - 1]
            for year in years_to_fetch:
                _LOGGER.info("Fetching ESO monthly dataset [%s] for year %d", obj[CONF_NAME], year)
                try:
                    monthly_dataset = await hass.async_add_executor_job(
                        client.fetch_dataset_monthly,
                        obj[CONF_ID],
                        year,
                    )
                except Exception as e:
                    _LOGGER.error("ESO fetch monthly dataset error [%s] year %d: %s", obj[CONF_NAME], year, e)
                    monthly_dataset = None

                if monthly_dataset:
                    await async_insert_statistics_monthly(hass, entry, obj, monthly_dataset)
                    if year == now.year:
                        for sensor in sensors:
                            if (
                                hasattr(sensor, "update_from_dataset")
                                and sensor._obj[CONF_ID] == obj[CONF_ID]
                                and getattr(sensor, "_granularity", None) == "monthly"
                            ):
                                sensor.update_from_dataset(monthly_dataset)

            _LOGGER.info("Import completed for %s", obj[CONF_NAME])

        if all_failed and not retry:
            _LOGGER.warning("Fetch failed, will retry later")
            hass.loop.call_later(
                RETRY_DELAY_SECONDS,
                lambda: asyncio.create_task(async_import_generation(datetime.now(), retry=True)),
            )
        elif all_failed and retry:
            _LOGGER.error("Fetch failed, postponing fetch for next day")

    async_track_time_change(hass, async_import_generation, hour=5, minute=11, second=0)

    async def _on_ha_started(_event) -> None:
        await async_import_generation(datetime.now(), backfill=True)

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STARTED, _on_ha_started)

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    unload_ok = await hass.config_entries.async_unload_platforms(entry, ["sensor"])
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id, None)
    return unload_ok


async def async_insert_statistics(
    hass: HomeAssistant, entry: ConfigEntry, obj: dict, dataset: dict
) -> None:
    retention_days: int = entry.options.get(CONF_RETENTION_DAYS, 0)
    statistic_ids_to_purge: list[str] = []
    for data_type in [CONF_CONSUMED, CONF_RETURNED]:
        if obj.get(data_type) is False:
            continue
        statistic_id = f"{DOMAIN}:energy_{data_type}_{obj[CONF_ID]}"
        mapped_key = ENERGY_TYPE_MAP[data_type]
        if not dataset or mapped_key not in dataset:
            _LOGGER.error("Received empty generation data for %s", statistic_id)
            continue
        generation_data = dataset[mapped_key]
        metadata = StatisticMetaData(
            has_sum=True,
            mean_type=StatisticMeanType.NONE,
            name=f"{obj[CONF_NAME]} ({data_type})",
            source=DOMAIN,
            statistic_id=statistic_id,
            unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
            unit_class="energy",
        )
        statistics = await _async_get_statistics(hass, metadata, generation_data)
        async_add_external_statistics(hass, metadata, statistics)
        statistic_ids_to_purge.append(statistic_id)
    if retention_days > 0 and statistic_ids_to_purge:
        await _async_purge_old_statistics(hass, statistic_ids_to_purge, retention_days)


async def _async_get_statistics(
    hass: HomeAssistant, metadata: StatisticMetaData, generation_data: dict
) -> list[StatisticData]:
    if not generation_data:
        return []

    tz = dt_util.get_time_zone("Europe/Vilnius")
    sorted_ts = sorted(generation_data.keys())
    range_start = datetime.fromtimestamp(sorted_ts[0]).replace(tzinfo=tz)
    range_end = datetime.fromtimestamp(sorted_ts[-1]).replace(tzinfo=tz) + timedelta(hours=1)

    existing_raw = await get_instance(hass).async_add_executor_job(
        statistics_during_period,
        hass,
        range_start,
        range_end,
        {metadata["statistic_id"]},
        "hour",
        None,
        {"state"},
    )
    existing_by_ts: dict[float, float] = {}
    for rec in existing_raw.get(metadata["statistic_id"], []):
        start = rec["start"]
        ts = start if isinstance(start, (int, float)) else start.timestamp()
        existing_by_ts[ts] = rec.get("state") or 0.0

    statistics: list[StatisticData] = []
    sum_ = None
    for ts in sorted_ts:
        kwh = generation_data[ts]
        existing_val = existing_by_ts.get(ts, 0.0)
        if kwh == 0.0 and existing_val > 0.0:
            _LOGGER.debug(
                "Preserving existing value %.3f kWh at ts=%s for %s (ESO returned 0)",
                existing_val, ts, metadata["statistic_id"],
            )
            kwh = existing_val
        dt_object = datetime.fromtimestamp(ts).replace(tzinfo=tz)
        if sum_ is None:
            sum_ = await get_previous_sum(hass, metadata, dt_object)
        sum_ += kwh
        statistics.append(StatisticData(start=dt_object, state=kwh, sum=sum_))
    return statistics


async def get_previous_sum(
    hass: HomeAssistant, metadata: StatisticMetaData, date: datetime
) -> float:
    statistic_id = metadata["statistic_id"]
    start = date - timedelta(hours=1)
    end = date
    stat = await get_instance(hass).async_add_executor_job(
        statistics_during_period, hass, start, end, {statistic_id}, "hour", None, {"sum"}
    )
    if statistic_id not in stat:
        return 0.0
    return stat[statistic_id][0]["sum"]


async def _async_purge_old_statistics(
    hass: HomeAssistant, statistic_ids: list[str], retention_days: int
) -> None:
    cutoff_ts = (datetime.now() - timedelta(days=retention_days)).timestamp()
    _LOGGER.debug(
        "Purging statistics older than %d days (cutoff ts=%s) for: %s",
        retention_days, cutoff_ts, statistic_ids,
    )
    await get_instance(hass).async_add_executor_job(
        _purge_statistics_sync, get_instance(hass), statistic_ids, cutoff_ts
    )


def _purge_statistics_sync(instance, statistic_ids: list[str], cutoff_ts: float) -> None:
    from homeassistant.components.recorder.db_schema import Statistics, StatisticsMeta
    with instance.get_session() as session:
        meta_ids = (
            session.query(StatisticsMeta.id)
            .filter(StatisticsMeta.statistic_id.in_(statistic_ids))
            .subquery()
        )
        deleted = (
            session.query(Statistics)
            .filter(
                Statistics.metadata_id.in_(meta_ids),
                Statistics.start_ts < cutoff_ts,
            )
            .delete(synchronize_session=False)
        )
        session.commit()
        _LOGGER.debug("Purged %d old statistic rows", deleted)


async def async_insert_statistics_monthly(
    hass: HomeAssistant, entry: ConfigEntry, obj: dict, dataset: dict
) -> None:
    retention_days: int = entry.options.get(CONF_RETENTION_DAYS, 0)
    statistic_ids_to_purge: list[str] = []
    for data_type in [CONF_CONSUMED, CONF_RETURNED]:
        if obj.get(data_type) is False:
            continue
        statistic_id = f"{DOMAIN}:energy_{data_type}_{obj[CONF_ID]}_monthly"
        mapped_key = ENERGY_TYPE_MAP[data_type]
        if not dataset or mapped_key not in dataset:
            _LOGGER.error("Received empty monthly data for %s", statistic_id)
            continue
        generation_data = dataset[mapped_key]
        metadata = StatisticMetaData(
            has_sum=True,
            mean_type=StatisticMeanType.NONE,
            name=f"{obj[CONF_NAME]} ({data_type} monthly)",
            source=DOMAIN,
            statistic_id=statistic_id,
            unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
            unit_class="energy",
        )
        statistics = await _async_get_statistics(hass, metadata, generation_data)
        async_add_external_statistics(hass, metadata, statistics)
        statistic_ids_to_purge.append(statistic_id)
    if retention_days > 0 and statistic_ids_to_purge:
        await _async_purge_old_statistics(hass, statistic_ids_to_purge, retention_days)


async def async_insert_cost_statistics(
    hass: HomeAssistant, entry: ConfigEntry, obj: dict, consumption_dataset: dict
) -> None:
    if obj.get(CONF_CONSUMED) is False:
        return
    cons_dataset = consumption_dataset.get(POWER_CONSUMED)
    if not cons_dataset:
        return
    start_time = datetime.fromtimestamp(min(cons_dataset.keys())).replace(
        tzinfo=dt_util.get_time_zone("Europe/Vilnius")
    )
    end_time = datetime.fromtimestamp(max(cons_dataset.keys())).replace(
        tzinfo=dt_util.get_time_zone("Europe/Vilnius")
    )
    prices = await _async_generate_price_dict(hass, obj, start_time, end_time)
    if not prices:
        return
    cost_metadata = StatisticMetaData(
        has_sum=True,
        mean_type=StatisticMeanType.NONE,
        name=f"{obj[CONF_NAME]} ({CONF_COST})",
        source=DOMAIN,
        statistic_id=f"{DOMAIN}:energy_{CONF_COST}_{obj[CONF_ID]}",
        unit_of_measurement=obj.get(CONF_PRICE_CURRENCY, "EUR"),
        unit_class=None,
    )
    cost_stats: list[StatisticData] = []
    cost_sum_ = None
    for ts, cons_kwh in cons_dataset.items():
        dt_object = datetime.fromtimestamp(ts).replace(
            tzinfo=dt_util.get_time_zone("Europe/Vilnius")
        )
        price = prices.get(ts, 0)
        cost = round(cons_kwh * price, 5)
        if cost_sum_ is None:
            cost_sum_ = await get_previous_sum(hass, cost_metadata, start_time)
        cost_sum_ += cost
        cost_stats.append(StatisticData(start=dt_object, state=cost, sum=cost_sum_))
    async_add_external_statistics(hass, cost_metadata, cost_stats)
    retention_days: int = entry.options.get(CONF_RETENTION_DAYS, 0)
    if retention_days > 0:
        cost_statistic_id = f"{DOMAIN}:energy_{CONF_COST}_{obj[CONF_ID]}"
        await _async_purge_old_statistics(hass, [cost_statistic_id], retention_days)


async def _async_generate_price_dict(
    hass: HomeAssistant, obj: dict, time_from: datetime, time_to: datetime
) -> dict:
    stats = await get_instance(hass).async_add_executor_job(
        statistics_during_period,
        hass,
        time_from,
        time_to,
        {obj[CONF_PRICE_ENTITY]},
        "hour",
        None,
        {"state"},
    )
    price_stats = stats.get(obj[CONF_PRICE_ENTITY])
    if price_stats is None:
        _LOGGER.warning(
            "No price statistics for %s between %s and %s",
            obj[CONF_PRICE_ENTITY],
            time_from.isoformat(),
            time_to.isoformat(),
        )
        return {}
    prices = {}
    for rec in price_stats:
        prices[rec["start"]] = rec["state"]
    return prices
