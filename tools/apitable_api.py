import json
import httpx
from typing import Any, Dict, Union

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage

class ApitableApiTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Union[ToolInvokeMessage, list[ToolInvokeMessage]]:
        credentials = self.runtime.credentials
        api_token = credentials.get("api_token")
        api_base_url = credentials.get("api_base_url", "https://api.apitable.com/fusion/v1").rstrip("/")
        read_only = credentials.get("read_only", "no") == "yes"

        action = tool_parameters.get("action")
        datasheet_id = tool_parameters.get("datasheet_id", "").strip()
        payload_str = tool_parameters.get("payload", "").strip()
        custom_method = tool_parameters.get("custom_method", "GET")
        custom_endpoint = tool_parameters.get("custom_endpoint", "").strip()

        payload = {}
        if payload_str:
            try:
                payload = json.loads(payload_str)
            except json.JSONDecodeError:
                return self.create_text_message(f"Error: Invalid JSON format in payload: {payload_str}")

        headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        }

        method = "GET"
        endpoint = ""
        params = None
        json_data = None

        if action == "list_records":
            if not datasheet_id:
                return self.create_text_message("Error: datasheet_id is required for list_records.")
            method = "GET"
            endpoint = f"/datasheets/{datasheet_id}/records"
            params = payload
        elif action == "create_records":
            if not datasheet_id:
                return self.create_text_message("Error: datasheet_id is required for create_records.")
            method = "POST"
            endpoint = f"/datasheets/{datasheet_id}/records"
            json_data = payload
        elif action == "update_records":
            if not datasheet_id:
                return self.create_text_message("Error: datasheet_id is required for update_records.")
            method = "PATCH"
            endpoint = f"/datasheets/{datasheet_id}/records"
            json_data = payload
        elif action == "delete_records":
            if not datasheet_id:
                return self.create_text_message("Error: datasheet_id is required for delete_records.")
            method = "DELETE"
            endpoint = f"/datasheets/{datasheet_id}/records"
            if isinstance(payload, list):
                # APITable API expects delete payload to be an array or query recordsIds string
                # Since payload from user might be a list of string ids, let's map it.
                # Actually, the APITable API delete endpoint: DELETE /fusion/v1/datasheets/{datasheetId}/records
                # Query param array: ?recordIds=rec1&recordIds=rec2 or json body `{"recordIds": [...]}`
                pass
            json_data = payload
        elif action == "custom_api_call":
            if not custom_endpoint:
                return self.create_text_message("Error: custom_endpoint is required for custom_api_call.")
            method = custom_method
            endpoint = custom_endpoint
            if method == "GET" or method == "DELETE":
                params = payload
            else:
                json_data = payload
        else:
            return self.create_text_message(f"Error: Unknown action {action}")

        if not endpoint.startswith("/"):
            endpoint = "/" + endpoint

        url = f"{api_base_url}{endpoint}"

        if read_only and method != "GET":
            return self.create_text_message("Error: Tool is configured in Read-Only mode. Write operations are disabled.")

        try:
            with httpx.Client(timeout=30.0) as client:
                request_kwargs = {"headers": headers}
                if params:
                    # Filter out None and complex objects if requests can't handle them for params
                    request_kwargs["params"] = {k: v if not isinstance(v, (dict, list)) else json.dumps(v) for k, v in params.items() if v is not None}
                if json_data:
                    request_kwargs["json"] = json_data

                response = client.request(method, url, **request_kwargs)
                
                try:
                    response_json = response.json()
                    # format properly to return to agent
                    return self.create_json_message(response_json)
                except json.JSONDecodeError:
                    return self.create_text_message(f"Status: {response.status_code}\nResponse: {response.text}")

        except Exception as e:
            return self.create_text_message(f"Error executing API call: {str(e)}")
