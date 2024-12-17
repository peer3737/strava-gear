import logging
import os
from supporting.strava import Strava
from database.db import Connection
import uuid
from supporting import aws


class CorrelationIdFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        # Generate a new correlation ID
        self.correlation_id = str(uuid.uuid4())

    def filter(self, record):
        # Add correlation ID to the log record
        record.correlation_id = self.correlation_id
        return True


# Logging formatter that includes the correlation ID
formatter = logging.Formatter('[%(levelname)s] [%(asctime)s] [Correlation ID: %(correlation_id)s] %(message)s')

# Set up the root logger
log = logging.getLogger()
log.setLevel("INFO")
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

# Remove existing handlers
for handler in log.handlers:
    log.removeHandler(handler)

# Add a new handler with the custom formatter
handler = logging.StreamHandler()
handler.setFormatter(formatter)
log.addHandler(handler)

# Add the CorrelationIdFilter to the logger
correlation_filter = CorrelationIdFilter()
log.addFilter(correlation_filter)


def lambda_handler(event, context):
    activity_id = event.get("activity_id")
    log.info(f"Start handling laps for activity {activity_id}")
    database_id = os.getenv('DATABASE_ID')
    database_settings = aws.dynamodb_query(table='database_settings', id=database_id)
    db_host = database_settings[0]['host']
    db_user = database_settings[0]['user']
    db_password = database_settings[0]['password']
    db_port = database_settings[0]['port']
    db = Connection(user=db_user, password=db_password, host=db_host, port=db_port, charset="utf8mb4")
    strava = Strava(db)
    log.info(f'Update gear')
    gear_id = db.get_specific(table='activity', where=f'id = {activity_id}', order_by_type='desc')[0][15]
    if gear_id is not None:
        strava_gear = strava.getgear(gear_id=gear_id)
        log.info(f'Update {strava_gear["name"]}')
        update_data = {
            "is_primary": strava_gear["primary"],
            "name": strava_gear["name"],
            "nickname": strava_gear["nickname"],
            "resource_state": strava_gear["resource_state"],
            "is_retired": strava_gear["retired"],
            "distance": strava_gear["distance"],
            "converted_distance": strava_gear["converted_distance"],
            "brand_name": strava_gear["brand_name"],
            "model_name": strava_gear["model_name"],
            "description": strava_gear["description"],
            "notification_distance": strava_gear["notification_distance"],
        }

        check_gear = db.get_specific(table='gear', where=f"id = '{gear_id}'", order_by_type='desc')
        if len(check_gear) == 0:
            update_data['id'] = gear_id
            db.insert(table="gear", json_data=update_data)

        else:
            db.update(table="gear", json_data=update_data, record_id=gear_id)
