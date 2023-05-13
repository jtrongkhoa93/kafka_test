"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        json_unpacked = json.loads(message.value())
        self.temperature = json_unpacked.get("temperature")
        self.status = json_unpacked.get("status")
