import os
import requests

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def send_telegram_alert(message: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram config is missing")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()

        print("Telegram status:", response.status_code)
        print("Telegram response:", response.text)

    except requests.RequestException as exc:
        print(f"Telegram alert failed: {exc}")


def notify_success(context):
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]

    message = (
        f"✅ Airflow DAG Success\n"
        f"DAG: {dag_id}\n"
        f"Run ID: {run_id}"
    )
    send_telegram_alert(message)


def task_fail_alert(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]

    message = (
        f"❌ Airflow Task Failed\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Run ID: {run_id}"
    )
    send_telegram_alert(message)