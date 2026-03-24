import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests
from .form_parser import FormParser

LOGIN_URL = "https://mano.eso.lt/?destination=/consumption"
GENERATION_URL = "https://mano.eso.lt/consumption?ajax_form=1&_wrapper_format=drupal_ajax"
MONTHS = [
    "Sausio", "Vasario", "Kovo", "Balandžio", "Gegužės", "Birželio", "Liepos", "Rugpjūčio", "Rugsėjo", "Spalio", "Lapkričio", "Gruodžio"
]
_LOGGER = logging.getLogger(__name__)

class ESOClient:
    def __init__(self, username: str, password: str):
        self.username: str = username
        self.password: str = password
        self.session: requests.Session = requests.Session()
        self.cookies: dict | None = None
        self.form_parser: FormParser = FormParser()
        self.dataset: dict = {}

    def login(self) -> None:
        self.dataset = {}
        try:
            response = self.session.post(
                LOGIN_URL,
                data={
                    "name": self.username,
                    "pass": self.password,
                    "login_type": 1,
                    "form_id": "user_login_form"
                },
                allow_redirects=True
            )
            response.raise_for_status()
            _LOGGER.debug(f"Got login response: {response.text}")
            self.cookies = requests.utils.dict_from_cookiejar(response.cookies)
            self.form_parser.feed(response.text)
        except requests.exceptions.RequestException as e:
            _LOGGER.error(f"ESO login error: {e}")
            return

    def fetch(
        self,
        obj: str,
        date: datetime,
        display_type: str = "hourly",
        period: str = "week",
    ) -> dict:
        if not self.cookies:
            _LOGGER.error("Cookies are empty. Check your credentials.")
            return {}
        if self.form_parser.get("form_id") != "eso_consumption_history_form":
            _LOGGER.error("Form ID not found. Check your credentials OR login to ESO and confirm contact information.")
            return {}
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
        }
        data = {
            "objects[]": obj,
            "objects_mock": "",
            "display_type": display_type,
            "period": period,
            "energy_type": "general",
            "scales": "total",
            "active_date_value": date.strftime("%Y-%m-%d 00:00"),
            "made_energy_status": 1,
            "visible_scales_field": 0,
            "visible_last_year_comparison_field": 0,
            "form_build_id": self.form_parser.get("form_build_id"),
            "form_token": self.form_parser.get("form_token"),
            "form_id": self.form_parser.get("form_id"),
            "_drupal_ajax": "1",
            "_triggering_element_name": "display_type",
        }
        try:
            response = self.session.post(
                GENERATION_URL,
                data=data,
                headers=headers,
                cookies=self.cookies,
                allow_redirects=False
            )
            response.raise_for_status()
            _LOGGER.debug(f"Got fetch response: {response.text}")
            return response.json()
        except requests.exceptions.RequestException as e:
            _LOGGER.error(f"ESO fetch error: {e}")
            return {}

    def fetch_monthly(self, obj: str, year: int) -> dict:
        date = datetime(year, 1, 1)
        return self.fetch(obj, date, display_type="monthly", period="year")

    def fetch_dataset(self, obj: str, date: datetime) -> dict | None:
        if obj in self.dataset:
            return self.dataset[obj]
        self.dataset[obj] = {}
        data = self.fetch(obj, date)
        self._populate_dataset(obj, data)
        return self.dataset[obj]

    def fetch_dataset_monthly(self, obj: str, year: int) -> dict | None:
        cache_key = f"{obj}_monthly_{year}"
        if cache_key in self.dataset:
            return self.dataset[cache_key]
        self.dataset[cache_key] = {}
        data = self.fetch_monthly(obj, year)
        self._populate_dataset(cache_key, data)
        return self.dataset[cache_key]

    def _populate_dataset(self, cache_key: str, data: list) -> None:
        for d in data:
            if d.get("command") == "update_build_id":
                self.form_parser.set("form_build_id", d["new"])
                continue
            if d.get("command") != "settings":
                continue
            if "eso_consumption_history_form" not in d["settings"] or not d["settings"]["eso_consumption_history_form"]:
                continue
            datasets = d["settings"]["eso_consumption_history_form"]["graphics_data"]["datasets"]
            for dataset in datasets:
                consumption_type = dataset["key"]
                if consumption_type not in self.dataset[cache_key]:
                    self.dataset[cache_key][consumption_type] = {}
                self.dataset[cache_key][consumption_type] = self.parse_dataset(dataset)

    def get_dataset(self, obj: str) -> dict | None:
        if obj not in self.dataset:
            return None
        return self.dataset[obj]

    @staticmethod
    def parse_dataset(dataset: dict) -> dict:
        result = {}
        DATE_FORMATS = ["%Y%m%d%H%M", "%Y-%m", "%Y%m"]
        for record in dataset["record"]:
            try:
                raw_date = record["date"]
                dt = None
                for fmt in DATE_FORMATS:
                    try:
                        dt = datetime.strptime(raw_date, fmt)
                        break
                    except ValueError:
                        continue
                if dt is None:
                    _LOGGER.error(f"Unrecognised date format in record: {record}")
                    continue
                ts = dt.timestamp()
                val = abs(float(record["value"])) if record["value"] is not None else 0.0
                result[ts] = val
            except Exception as e:
                _LOGGER.error(f"Failed to parse dataset record {record}: {e}")
        return result
