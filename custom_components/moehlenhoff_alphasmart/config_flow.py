"""Config flow for Alpha Smart integration."""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResult
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv, httpx_client

from .const import CONF_CLOUD_INFO, CONF_DEVICE_IDS, CONF_DEVICES, CONF_TOKENS, CONF_USERNAME, CONF_PASSWORD, DOMAIN

_LOGGER = logging.getLogger(__name__)
STEP_USER_DATA_SCHEMA = vol.Schema(
    {vol.Required(CONF_USERNAME): str, vol.Required(CONF_PASSWORD): str}
)

async def validate_input(hass: HomeAssistant, data: dict[str, Any]) -> dict[str, Any]:
    """Validate the user input allows us to connect.

    Data has the keys from STEP_USER_DATA_SCHEMA with values provided by the user.
    """

    # If your PyPI package is not built with async, pass your methods
    # to the executor:
    # await hass.async_add_executor_job(
    #     your_validate_func, data[CONF_USERNAME], data[CONF_PASSWORD]
    # )
    from boto3 import client
    from pycognito.aws_srp import AWSSRP
    from requests_aws4auth import AWS4Auth

    httpx_session = httpx_client.get_async_client(hass)

    cloud_info = await httpx_session.get(
        "https://iot-prod-config.s3.eu-central-1.amazonaws.com/v1.json",
    )
    _LOGGER.debug(cloud_info)
    cloud_info_json = cloud_info.json()
    api_endpoint = cloud_info_json["endpoint"]
    user_pool_id = cloud_info_json["cognito"]["alphaSmart"]["userPoolId"]
    user_pool_region = cloud_info_json["region"]
    client_id = cloud_info_json["cognito"]["alphaSmart"]["webClientId"]
    identity_pool_id = cloud_info_json["cognito"]["alphaSmart"]["identityPoolId"]
    mqtt_broker_endpoint = cloud_info_json["mqttBrokerEndpoint"]

    def get_idp_client():
        return client("cognito-idp", region_name=user_pool_region)

    idp_client = await hass.async_add_executor_job(get_idp_client)
    aws_srp = AWSSRP(
        data[CONF_USERNAME],
        data[CONF_PASSWORD],
        user_pool_id,
        client_id,
        client=idp_client,
    )
    tokens = await hass.async_add_executor_job(aws_srp.authenticate_user)
    if not tokens["AuthenticationResult"]:
        raise InvalidAuth

    def get_identity_client():
        return client("cognito-identity", region_name=user_pool_region)

    identity_client = await hass.async_add_executor_job(get_identity_client)

    def get_identity_id():
        return identity_client.get_id(
            IdentityPoolId=identity_pool_id,
            Logins={
                f"cognito-idp.{user_pool_region}.amazonaws.com/{user_pool_id}": tokens[
                    "AuthenticationResult"
                ]["IdToken"]
            },
        )

    identity_id = await hass.async_add_executor_job(get_identity_id)
    _LOGGER.info(identity_id)

    def temp_credentials():
        return identity_client.get_credentials_for_identity(
            IdentityId=identity_id["IdentityId"],
            Logins={
                f"cognito-idp.{user_pool_region}.amazonaws.com/{user_pool_id}": tokens[
                    "AuthenticationResult"
                ]["IdToken"]
            },
        )

    credentials = await hass.async_add_executor_job(temp_credentials)
    if not credentials["Credentials"]:
        raise InvalidAuth
    _LOGGER.debug(credentials)
    auth = AWS4Auth(
        credentials["Credentials"]["AccessKeyId"],
        credentials["Credentials"]["SecretKey"],
        user_pool_region,
        "execute-api",
        session_token=credentials["Credentials"]["SessionToken"],
    )
    httpx_session.auth = auth
    devices = await httpx_session.get(
        api_endpoint + "/v1/devices",
    )
    devices_json = devices.json()
    _LOGGER.debug(devices_json)
    device_details = await _async_build_device_details(
        httpx_session, api_endpoint, devices_json
    )
    _LOGGER.debug("device details for onboarding: %s", device_details)
    return {
        "title": "Alpha Smart",
        CONF_TOKENS: tokens["AuthenticationResult"],
        "credentials": credentials["Credentials"],
        CONF_DEVICES: devices_json,
        "device_details": device_details,
        CONF_CLOUD_INFO: {
            "api_endpoint": api_endpoint,
            "user_pool_id": user_pool_id,
            "user_pool_region": user_pool_region,
            "client_id": client_id,
            "mqtt_broker_endpoint": mqtt_broker_endpoint,
        },
        "identity_id": identity_id["IdentityId"],
        CONF_USERNAME: data[CONF_USERNAME],
        CONF_PASSWORD: data[CONF_PASSWORD],
    }


async def _async_build_device_details(
    httpx_session, api_endpoint: str, devices: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    async def fetch_detail(device: dict[str, Any]) -> dict[str, Any] | None:
        device_id = device.get("deviceId")
        if not device_id:
            return None
        url = api_endpoint + "/v1/devices/" + device_id + "/values"
        response = await httpx_session.get(url)
        values = response.json()
        name = (
            values.get("name")
            or device.get("name")
            or device.get("deviceName")
            or device_id
        )
        supports_climate = any(key in values for key in ("30", "31", "33"))
        return {
            "deviceId": device_id,
            "name": name,
            "supports_climate": supports_climate,
        }

    results = await asyncio.gather(
        *(fetch_detail(device) for device in devices), return_exceptions=True
    )
    details: list[dict[str, Any]] = []
    for result in results:
        if isinstance(result, Exception):
            continue
        if result:
            details.append(result)
    return details


class ConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Alpha Smart."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial step."""
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                info = await validate_input(self.hass, user_input)
            except CannotConnect:
                errors["base"] = "cannot_connect"
            except InvalidAuth:
                errors["base"] = "invalid_auth"
            except Exception:  # pylint: disable=broad-except
                _LOGGER.exception("Unexpected exception")
                errors["base"] = "unknown"
            else:
                self._discovered_devices = info[CONF_DEVICES]
                self._device_details = info.pop("device_details", [])
                self._auth_payload = info
                return await self.async_step_select_devices()

        return self.async_show_form(
            step_id="user", data_schema=STEP_USER_DATA_SCHEMA, errors=errors
        )

    async def async_step_select_devices(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the device selection step."""
        errors: dict[str, str] = {}
        if user_input is not None:
            selected_devices = user_input.get(CONF_DEVICE_IDS, [])
            if not selected_devices:
                errors["base"] = "no_devices_selected"
            else:
                data = dict(self._auth_payload)
                data[CONF_DEVICE_IDS] = list(selected_devices)
                return self.async_create_entry(title=data["title"], data=data)

        device_details = getattr(self, "_device_details", [])
        if device_details:
            filtered = [
                device for device in device_details if device.get("supports_climate")
            ]
            if not filtered:
                filtered = device_details
            options = {
                device["deviceId"]: f"{device['name']} ({device['deviceId']})"
                for device in filtered
            }
        else:
            devices = getattr(self, "_discovered_devices", [])
            options = {
                device["deviceId"]: device.get("name", device["deviceId"])
                for device in devices
            }
        schema = vol.Schema(
            {
                vol.Required(
                    CONF_DEVICE_IDS, default=list(options.keys())
                ): cv.multi_select(options)
            }
        )
        return self.async_show_form(
            step_id="select_devices", data_schema=schema, errors=errors
        )

    @staticmethod
    @config_entries.callback
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> config_entries.OptionsFlow:
        return OptionsFlowHandler(config_entry)


class OptionsFlowHandler(config_entries.OptionsFlow):
    """Handle options flow for Alpha Smart."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        self._config_entry = config_entry
        self._device_details: list[dict[str, Any]] = []

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Manage the options."""
        errors: dict[str, str] = {}
        if user_input is not None:
            selected_devices = user_input.get(CONF_DEVICE_IDS, [])
            if not selected_devices:
                errors["base"] = "no_devices_selected"
            else:
                return self.async_create_entry(
                    title="",
                    data={CONF_DEVICE_IDS: list(selected_devices)},
                )

        await self._async_refresh_devices()
        filtered = [
            device
            for device in self._device_details
            if device.get("supports_climate")
        ]
        if not filtered:
            filtered = self._device_details
        options = {
            device["deviceId"]: f"{device['name']} ({device['deviceId']})"
            for device in filtered
        }
        current_device_ids = self._current_device_ids
        schema = vol.Schema(
            {
                vol.Required(
                    CONF_DEVICE_IDS, default=current_device_ids
                ): cv.multi_select(options)
            }
        )
        return self.async_show_form(
            step_id="init", data_schema=schema, errors=errors
        )

    @property
    def _current_device_ids(self) -> list[str]:
        return list(
            self._config_entry.options.get(
                CONF_DEVICE_IDS,
                self._config_entry.data.get(CONF_DEVICE_IDS, []),
            )
        )

    async def _async_refresh_devices(self) -> None:
        httpx_session = httpx_client.get_async_client(self.hass)
        cloud_info = self._config_entry.data[CONF_CLOUD_INFO]
        auth = await self._async_get_auth(cloud_info, self._config_entry.data[CONF_TOKENS])
        httpx_session.auth = auth
        api_endpoint = cloud_info["api_endpoint"]
        devices = await httpx_session.get(api_endpoint + "/v1/devices")
        self._device_details = await _async_build_device_details(
            httpx_session, api_endpoint, devices.json()
        )

    async def _async_get_auth(self, cloud_info: dict[str, Any], tokens: dict[str, Any]):
        from boto3 import client
        from pycognito import Cognito
        from pycognito.aws_srp import AWSSRP
        from requests_aws4auth import AWS4Auth

        user_pool_id = cloud_info["user_pool_id"]
        user_pool_region = cloud_info["user_pool_region"]
        client_id = cloud_info["client_id"]

        def get_identity_client():
            return client("cognito-identity", region_name=user_pool_region)

        def get_cognito_client():
            return Cognito(
                user_pool_id,
                client_id,
                id_token=tokens["IdToken"],
                refresh_token=tokens["RefreshToken"],
                access_token=tokens["AccessToken"],
            )

        u = await self.hass.async_add_executor_job(get_cognito_client)
        try:
            await self.hass.async_add_executor_job(u.verify_tokens)
        except Exception:  # pylint: disable=broad-except
            try:
                await self.hass.async_add_executor_job(u.check_token)
                tokens.update({"IdToken": u.id_token, "AccessToken": u.access_token})
            except Exception as refresh_err:
                # Refresh token expired — try full SRP login
                username = self._config_entry.data.get(CONF_USERNAME)
                password = self._config_entry.data.get(CONF_PASSWORD)
                if username and password:
                    _LOGGER.warning("Options flow: refresh token expired, performing SRP re-auth")
                    def get_idp_client():
                        return client("cognito-idp", region_name=user_pool_region)
                    idp_client = await self.hass.async_add_executor_job(get_idp_client)
                    def do_srp_login():
                        aws_srp = AWSSRP(
                            username, password, user_pool_id, client_id, client=idp_client,
                        )
                        return aws_srp.authenticate_user()
                    auth_result = await self.hass.async_add_executor_job(do_srp_login)
                    new_tokens = auth_result["AuthenticationResult"]
                    tokens.update(
                        {
                            "IdToken": new_tokens["IdToken"],
                            "AccessToken": new_tokens["AccessToken"],
                            "RefreshToken": new_tokens.get("RefreshToken", tokens.get("RefreshToken")),
                        }
                    )
                    # Persist new tokens
                    new_data = dict(self._config_entry.data)
                    new_data[CONF_TOKENS] = dict(tokens)
                    self.hass.config_entries.async_update_entry(self._config_entry, data=new_data)
                else:
                    raise refresh_err
        await self.hass.async_add_executor_job(u.verify_tokens)

        identity_client = await self.hass.async_add_executor_job(get_identity_client)

        def temp_credentials():
            return identity_client.get_credentials_for_identity(
                IdentityId=self._config_entry.data["identity_id"],
                Logins={
                    f"cognito-idp.{user_pool_region}.amazonaws.com/{user_pool_id}": tokens[
                        "IdToken"
                    ]
                },
            )

        credentials = await self.hass.async_add_executor_job(temp_credentials)
        return AWS4Auth(
            credentials["Credentials"]["AccessKeyId"],
            credentials["Credentials"]["SecretKey"],
            user_pool_region,
            "execute-api",
            session_token=credentials["Credentials"]["SessionToken"],
        )



class CannotConnect(HomeAssistantError):
    """Error to indicate we cannot connect."""


class InvalidAuth(HomeAssistantError):
    """Error to indicate there is invalid auth."""
