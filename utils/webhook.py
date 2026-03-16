# utils/webhook.py

import requests


def send_gs1_pipeline_webhook(dbutils, data: dict) -> int:
    """
    Lähettää GS1 FULL+IMAGES -ajon yhteenvedon Logic Appin webhookiin.

    Odottaa 'data'-dictiltä ainakin:
      - started_at_utc
      - finished_at_utc
      - all_keys
      - kesko_stats: {rows_written, rows_categorized}
      - product_duration_human
      - images_stats: {ok}
      - images_duration_human
    """

    # Webhook-URL
    webhook_url = "https://prod-49.northeurope.logic.azure.com:443/workflows/dae36ace98b54604ac371ef2406dfe57/triggers/When_an_HTTP_request_is_received/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FWhen_an_HTTP_request_is_received%2Frun&sv=1.0&sig=pDBEuHL1Tnq0tlJGsPSVZ8uKsTF4bCzGKTznOQFNZCU"

    payload = data  # 👈 ei muotoilua, lähetetään sellaisenaan

    print("Lähetettävä payload:")
    print(payload)

    response = requests.post(webhook_url, json=payload)
    print(f"📤 GS1 webhook lähetetty. Status code: {response.status_code}")
    return response.status_code
