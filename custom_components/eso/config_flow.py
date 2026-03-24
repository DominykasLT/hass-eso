from __future__ import annotations

import voluptuous as vol
from homeassistant import config_entries
from homeassistant.const import CONF_USERNAME, CONF_PASSWORD
from homeassistant.core import HomeAssistant, callback

from .eso_client import ESOClient

DOMAIN = "eso"
CONF_OBJECTS = "objects"
CONF_NAME = "name"
CONF_ID = "id"
CONF_CONSUMED = "consumed"
CONF_RETURNED = "returned"
CONF_PRICE_ENTITY = "price_entity"
CONF_PRICE_CURRENCY = "price_currency"
CONF_ADD_ANOTHER = "add_another"
CONF_RETENTION_DAYS = "retention_days"
_BYTES_PER_ROW = 80
_HOURLY_POINTS_PER_DAY = 24
_SERIES_PER_OBJECT = 2


def _estimate_storage(days: int, num_objects: int) -> str:
    if days == 0:
        return "unlimited (data kept forever)"
    total_bytes = days * _HOURLY_POINTS_PER_DAY * _SERIES_PER_OBJECT * _BYTES_PER_ROW * num_objects
    if total_bytes < 1024:
        return f"~{total_bytes} bytes"
    if total_bytes < 1024 * 1024:
        return f"~{total_bytes // 1024} KB"
    return f"~{total_bytes / 1024 / 1024:.1f} MB"


async def _validate_credentials(hass: HomeAssistant, username: str, password: str) -> str | None:
    client = ESOClient(username, password)
    await hass.async_add_executor_job(client.login)
    if not client.cookies:
        return "invalid_auth"
    if client.form_parser.get("form_id") != "eso_consumption_history_form":
        return "confirm_contact"
    return None


class ESOConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    def __init__(self) -> None:
        self._data: dict = {}

    async def async_step_user(self, user_input: dict | None = None):
        errors: dict = {}
        if user_input is not None:
            error = await _validate_credentials(
                self.hass, user_input[CONF_USERNAME], user_input[CONF_PASSWORD]
            )
            if error:
                errors["base"] = error
            else:
                self._data[CONF_USERNAME] = user_input[CONF_USERNAME]
                self._data[CONF_PASSWORD] = user_input[CONF_PASSWORD]
                self._data[CONF_OBJECTS] = []
                return await self.async_step_object()

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({
                vol.Required(CONF_USERNAME): str,
                vol.Required(CONF_PASSWORD): str,
            }),
            errors=errors,
        )

    async def async_step_object(self, user_input: dict | None = None):
        errors: dict = {}
        if user_input is not None:
            self._data[CONF_OBJECTS].append({
                CONF_NAME: user_input[CONF_NAME],
                CONF_ID: user_input[CONF_ID],
                CONF_CONSUMED: user_input.get(CONF_CONSUMED, True),
                CONF_RETURNED: user_input.get(CONF_RETURNED, False),
                CONF_PRICE_ENTITY: user_input.get(CONF_PRICE_ENTITY, ""),
                CONF_PRICE_CURRENCY: user_input.get(CONF_PRICE_CURRENCY, "EUR"),
            })
            if user_input.get(CONF_ADD_ANOTHER, False):
                return await self.async_step_object()
            return self.async_create_entry(title="ESO", data=self._data)

        return self.async_show_form(
            step_id="object",
            data_schema=vol.Schema({
                vol.Required(CONF_NAME): str,
                vol.Required(CONF_ID): str,
                vol.Required(CONF_CONSUMED, default=True): bool,
                vol.Required(CONF_RETURNED, default=False): bool,
                vol.Optional(CONF_PRICE_ENTITY, default=""): str,
                vol.Optional(CONF_PRICE_CURRENCY, default="EUR"): str,
                vol.Optional(CONF_ADD_ANOTHER, default=False): bool,
            }),
            errors=errors,
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: config_entries.ConfigEntry):
        return ESOOptionsFlow(config_entry)


class ESOOptionsFlow(config_entries.OptionsFlow):
    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        self._entry = config_entry
        self._data: dict = dict(config_entry.data)
        self._options: dict = dict(config_entry.options)
        self._pending_retention: int = 0

    async def async_step_init(self, user_input: dict | None = None):
        errors: dict = {}
        if user_input is not None:
            retention = user_input.get(CONF_RETENTION_DAYS, self._options.get(CONF_RETENTION_DAYS, 0))
            if not isinstance(retention, int) or retention < 0:
                errors[CONF_RETENTION_DAYS] = "retention_invalid"
            else:
                error = await _validate_credentials(
                    self.hass,
                    user_input.get(CONF_USERNAME, self._data[CONF_USERNAME]),
                    user_input.get(CONF_PASSWORD, self._data[CONF_PASSWORD]),
                )
                if error:
                    errors["base"] = error
                else:
                    self._data[CONF_USERNAME] = user_input.get(CONF_USERNAME, self._data[CONF_USERNAME])
                    self._data[CONF_PASSWORD] = user_input.get(CONF_PASSWORD, self._data[CONF_PASSWORD])
                    self._pending_retention = retention
                    return await self.async_step_retention_confirm()

        current_retention = self._options.get(CONF_RETENTION_DAYS, 0)
        num_objects = len(self._data.get(CONF_OBJECTS, []))
        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema({
                vol.Optional(CONF_USERNAME, default=self._data[CONF_USERNAME]): str,
                vol.Optional(CONF_PASSWORD, default=self._data[CONF_PASSWORD]): str,
                vol.Optional(CONF_RETENTION_DAYS, default=current_retention): vol.All(
                    vol.Coerce(int), vol.Range(min=0, max=36500)
                ),
            }),
            description_placeholders={
                "current_estimate": _estimate_storage(current_retention, num_objects),
                "num_objects": str(num_objects),
            },
            errors=errors,
        )

    async def async_step_retention_confirm(self, user_input: dict | None = None):
        if user_input is not None:
            self.hass.config_entries.async_update_entry(self._entry, data=self._data)
            return self.async_create_entry(
                title="",
                data={CONF_RETENTION_DAYS: self._pending_retention},
            )

        num_objects = len(self._data.get(CONF_OBJECTS, []))
        estimate = _estimate_storage(self._pending_retention, num_objects)
        days_label = "forever" if self._pending_retention == 0 else f"{self._pending_retention} days"
        return self.async_show_form(
            step_id="retention_confirm",
            data_schema=vol.Schema({}),
            description_placeholders={
                "days": days_label,
                "estimate": estimate,
                "num_objects": str(num_objects),
            },
        )
