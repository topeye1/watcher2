from datetime import datetime, timedelta

import pytz


def setTimezoneDateTime():
    # Server Timezone
    utc_now = datetime.utcnow()
    # Shanghai Timezone.
    shanghai_timezone = pytz.timezone('Asia/Shanghai')
    date_time = utc_now.replace(tzinfo=pytz.utc).astimezone(shanghai_timezone)
    return date_time
