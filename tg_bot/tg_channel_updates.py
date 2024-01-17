import requests

def send_tg_message(message):
    BOT_TOKEN = "6719020665:AAG9wfbiG-eAa8TP3ZNPdTFw2qI2vt4FcL8"
    CHANNEL_ID = "-1002095900488"
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": CHANNEL_ID, "text": message, "parse_mode": "HTML"}
    response = requests.post(url, data=data)


def get_one_item_message(row):
    message = f"""
    <b>{row['name']}</b>
    <i>rating</i>:             <b>{row['rating']}</b>
    <i>prediction</i>:     {row['pred']}
    <i>rating_date</i>:   {row['rat_date']}
    <i>observation</i>:  {row['observation']}
    <i>agency</i>:           {row['agency']}
    """
    return message
