"""The Alpha Smart integration."""
from __future__ import annotations

from asyncio import wrap_future
from datetime import timedelta
import json
import logging
from typing import Any

from awscrt import auth
from awsiot import mqtt, mqtt_connection_builder
from boto3 import client
from pycognito import Cognito
from requests_aws4auth import AWS4Auth

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers import httpx_client
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import CONF_CLOUD_INFO, CONF_DEVICE_IDS, CONF_TOKENS, CONF_USERNAME, CONF_PASSWORD, DOMAIN

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [Platform.CLIMATE, Platform.SENSOR]

UPDATE_INTERVAL = timedelta(hours=1)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Alpha Smart from a config entry."""

    hass.data.setdefault(DOMAIN, {})
    coordinator = AlphaSmartCoordinator(hass)

    hass.data[DOMAIN][entry.entry_id] = coordinator
    hass.data[DOMAIN]["data"] = entry.data
    hass.data[DOMAIN]["device_ids"] = list(
        entry.options.get(CONF_DEVICE_IDS, entry.data.get(CONF_DEVICE_IDS, []))
    )
    entry.async_on_unload(entry.add_update_listener(update_listener))

    await coordinator.async_config_entry_first_refresh()

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        hass.data[DOMAIN].pop(entry.entry_id)

    return unload_ok


async def update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Handle options update."""
    await hass.config_entries.async_reload(entry.entry_id)


class AlphaSmartCoordinator(DataUpdateCoordinator):
    """Class to manage fetching Alpha Smart data."""

    def __init__(self, hass: HomeAssistant) -> None:
        """Initialize."""
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=UPDATE_INTERVAL,
        )

    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch data from Alpha Smart."""
        try:
            device_ids = list(self.hass.data[DOMAIN].get("device_ids", []))
            if not device_ids:
                raise UpdateFailed("No devices selected")
            httpx_session = httpx_client.get_async_client(self.hass)
            tokens = await self.async_get_auth()
            auth = AWS4Auth(
                tokens["AccessKeyId"],
                tokens["SecretKey"],
                self.hass.data[DOMAIN]["data"][CONF_CLOUD_INFO]["user_pool_region"],
                "execute-api",
                session_token=tokens["SessionToken"],
            )
            httpx_session.auth = auth
            api_endpoint = self.hass.data[DOMAIN]["data"][CONF_CLOUD_INFO][
                "api_endpoint"
            ]
            devices_response = await httpx_session.get(api_endpoint + "/v1/devices")
            devices = devices_response.json()
            devices = [
                device for device in devices if device["deviceId"] in device_ids
            ]
            if not devices:
                raise UpdateFailed("No matching devices found")
            obj = {}
            for device in devices:
                url = api_endpoint + "/v1/devices/" + device["deviceId"] + "/values"
                device_values = await httpx_session.get(url)
                device_values_json = device_values.json()
                _LOGGER.debug("device values: %s", device_values_json)
                last_heartbeat = device_values_json.get("lastHeartbeatAt")
                if last_heartbeat:
                    _LOGGER.info(
                        "last heartbeat for device %s: %s",
                        device_values_json.get("name", device["deviceId"]),
                        last_heartbeat,
                    )
                else:
                    _LOGGER.debug(
                        "device %s missing lastHeartbeatAt",
                        device_values_json.get("name", device["deviceId"]),
                    )
                obj[device["deviceId"]] = device_values_json

            _LOGGER.info("Starting websocket task")
            if "mqtt_connection" in self.hass.data[DOMAIN]:
                await wrap_future(
                    self.hass.data[DOMAIN]["mqtt_connection"].disconnect()
                )
                self.hass.data[DOMAIN]["mqtt_connection"] = None
            mqtt_connection = await self.async_websocket_connect()

            def on_message_received(topic, payload, dup, qos, retain, **kwargs):
                # _LOGGER.info("Received message from topic %s: %s", topic, payload)
                # topic userinfo/eu-central-1:6af4f4fc-fc76-4916-babe-47c9f93b3d29/devices/c3c45f32-ca01-4498-bd88-318323af1517/reported: b'{"10":100,"31":21.08,"33":36}'
                device_id = topic.split("/")[3]
                _LOGGER.info("device id: %s", device_id)
                payload_json = payload.decode("utf-8")
                _LOGGER.info("payload: %s", payload_json)
                # self.data[device_id]["30"] = target_temperature
                # map payload to self.data[device_id]
                for key, value in json.loads(payload_json).items():
                    self.data[device_id][key] = value
                self.async_update_listeners()

            subscribe_future, _ = mqtt_connection.subscribe(
                topic=f"userinfo/{self.hass.data[DOMAIN]['data']['identity_id']}/#",
                qos=mqtt.QoS.AT_LEAST_ONCE,
                callback=on_message_received,
            )
            res = await wrap_future(subscribe_future)
            _LOGGER.info("res: %s", res)

            self.hass.data[DOMAIN]["mqtt_connection"] = mqtt_connection
            return obj
        except ConnectionError as err:
            raise UpdateFailed(f"Error communicating with API: {err}") from err

    async def async_set_target_temperature(
        self, device_id: str, target_temperature: float
    ) -> None:
        """Set the target temperature of the given heat area."""
        _LOGGER.debug(
            "Setting target temperature of device id %s to %0.1f",
            device_id,
            target_temperature,
        )
        httpx_session = httpx_client.get_async_client(self.hass)
        tokens = await self.async_get_auth()
        auth = AWS4Auth(
            tokens["AccessKeyId"],
            tokens["SecretKey"],
            self.hass.data[DOMAIN]["data"]["cloud_info"]["user_pool_region"],
            "execute-api",
            session_token=tokens["SessionToken"],
        )
        httpx_session.auth = auth
        cloud_info = self.hass.data[DOMAIN]["data"][CONF_CLOUD_INFO]
        api_endpoint = cloud_info["api_endpoint"]
        url = api_endpoint + "/v1/devices/" + device_id + "/values"
        payload = {"30": target_temperature}
        await httpx_session.put(url, json=payload)
        self.data[device_id]["30"] = target_temperature
        self.async_update_listeners()

    async def async_websocket_connect(self) -> None:
        """Connect to the websocket."""
        _LOGGER.info("Connecting to websocket")
        tokens = await self.async_get_auth()
        cred_provider = auth.AwsCredentialsProvider.new_static(
            access_key_id=tokens["AccessKeyId"],
            secret_access_key=tokens["SecretKey"],
            session_token=tokens["SessionToken"],
        )

        def on_connection_failure(connection, callback_data):
            _LOGGER.error("Connection failed with error %s", callback_data.error)

        def on_connection_interrupted(connection, error, **kwargs):
            _LOGGER.error("Connection interrupted with error %s", error)

        def on_connection_resumed(connection, return_code, session_present, **kwargs):
            _LOGGER.info("Connection resumed with return code %s", return_code)

        def on_connection_success(connection, **kwargs):
            _LOGGER.info("Connection success")

        cloud_info = self.hass.data[DOMAIN]["data"][CONF_CLOUD_INFO]
        client_id = cloud_info["client_id"]
        identity_id = self.hass.data[DOMAIN]["data"]["identity_id"]
        mqtt_connection = mqtt_connection_builder.websockets_with_default_aws_signing(
            region=cloud_info["user_pool_region"],
            credentials_provider=cred_provider,
            endpoint=cloud_info["mqtt_broker_endpoint"],
            client_id=f"{identity_id}/{client_id}",
            on_connection_interrupted=on_connection_interrupted,
            on_connection_failure=on_connection_failure,
            on_connection_resumed=on_connection_resumed,
            on_connection_success=on_connection_success,
            clean_session=False,
        )
        connect_future = mqtt_connection.connect()
        res = await wrap_future(connect_future)
        _LOGGER.info("Connected to websocket")
        _LOGGER.info("res: %s", res)

        return mqtt_connection

    async def _async_full_reauth(self, cloud_info: dict) -> dict:
        """Perform full SRP login with stored username/password."""
        from pycognito.aws_srp import AWSSRP

        username = self.hass.data[DOMAIN]["data"].get(CONF_USERNAME)
        password = self.hass.data[DOMAIN]["data"].get(CONF_PASSWORD)
        if not username or not password:
            raise UpdateFailed(
                "Refresh token expired and no username/password stored. "
                "Reconfigure the integration to store credentials."
            )

        user_pool_id = cloud_info["user_pool_id"]
        user_pool_region = cloud_info["user_pool_region"]
        client_id = cloud_info["client_id"]

        _LOGGER.warning("Refresh token expired — performing full SRP re-authentication")

        def get_idp_client():
            return client("cognito-idp", region_name=user_pool_region)

        idp_client = await self.hass.async_add_executor_job(get_idp_client)

        def do_srp_login():
            aws_srp = AWSSRP(
                username,
                password,
                user_pool_id,
                client_id,
                client=idp_client,
            )
            return aws_srp.authenticate_user()

        auth_result = await self.hass.async_add_executor_job(do_srp_login)
        new_tokens = auth_result["AuthenticationResult"]

        # Persist new tokens
        self.hass.data[DOMAIN]["data"][CONF_TOKENS].update(
            {
                "IdToken": new_tokens["IdToken"],
                "AccessToken": new_tokens["AccessToken"],
                "RefreshToken": new_tokens.get(
                    "RefreshToken",
                    self.hass.data[DOMAIN]["data"][CONF_TOKENS].get("RefreshToken"),
                ),
            }
        )
        _LOGGER.info("Full re-authentication successful, tokens updated")
        return new_tokens

    async def _async_persist_tokens(self, entry: ConfigEntry) -> None:
        """Persist updated tokens to the config entry so they survive restarts."""
        new_data = dict(entry.data)
        new_data[CONF_TOKENS] = dict(self.hass.data[DOMAIN]["data"][CONF_TOKENS])
        self.hass.config_entries.async_update_entry(entry, data=new_data)

    async def async_get_auth(self):
        """Renews the auth token if necessary and returns the new credentials."""
        cloud_info = self.hass.data[DOMAIN]["data"][CONF_CLOUD_INFO]
        user_pool_id = cloud_info["user_pool_id"]
        user_pool_region = cloud_info["user_pool_region"]
        client_id = cloud_info["client_id"]
        tokens = self.hass.data[DOMAIN]["data"][CONF_TOKENS]

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
        except Exception:
            _LOGGER.info("Refreshing tokens")
            _LOGGER.info("old id token: %s", tokens["IdToken"])
            try:
                await self.hass.async_add_executor_job(u.check_token)
                mergedTokens = self.hass.data[DOMAIN]["data"]["tokens"]
                mergedTokens.update({"IdToken": u.id_token, "AccessToken": u.access_token})
                self.hass.data[DOMAIN]["data"]["tokens"].update(mergedTokens)
                _LOGGER.info(
                    "new id token: %s",
                    self.hass.data[DOMAIN]["data"]["tokens"]["IdToken"],
                )
            except Exception as refresh_err:
                _LOGGER.warning(
                    "Token refresh failed (%s), attempting full SRP re-auth", refresh_err
                )
                await self._async_full_reauth(cloud_info)
                tokens = self.hass.data[DOMAIN]["data"][CONF_TOKENS]
                u = await self.hass.async_add_executor_job(get_cognito_client)

                # Find our config entry and persist the new tokens
                for entry in self.hass.config_entries.async_entries(DOMAIN):
                    await self._async_persist_tokens(entry)

        await self.hass.async_add_executor_job(u.verify_tokens)

        def get_identity_client():
            return client("cognito-identity", region_name=user_pool_region)

        identity_client = await self.hass.async_add_executor_job(get_identity_client)

        def temp_credentials():
            return identity_client.get_credentials_for_identity(
                IdentityId=self.hass.data[DOMAIN]["data"]["identity_id"],
                Logins={
                    f"cognito-idp.{user_pool_region}.amazonaws.com/{user_pool_id}": self.hass.data[
                        DOMAIN
                    ]["data"]["tokens"]["IdToken"]
                },
            )

        credentials = await self.hass.async_add_executor_job(temp_credentials)
        _LOGGER.info("credentials: %s", credentials)
        return {
            "AccessKeyId": credentials["Credentials"]["AccessKeyId"],
            "SecretKey": credentials["Credentials"]["SecretKey"],
            "SessionToken": credentials["Credentials"]["SessionToken"],
        }
