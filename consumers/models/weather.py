#references :
# 1. https://github.com/jahyun-dev/udacity-optimizing-public-transportation
# 2. https://github.com/peter-de-boer/optimizing-public-transportation
# 3. https://github.com/Kshankar94/Optimizing-Public-Transportation--Kafka-Streaming--Udacity-
"""Contains functionality related to Weather"""
import logging
import json

logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"


    def process_message(self, message):
        """Handles incoming weather data"""
      
       # TODO: Process incoming weather messages. Set the temperature and status.
        message_value = json.loads(message.value())
        try:
            self.temperature = message_value['temperature']
            self.status = message_value['status']
        except:
            logger.debug(f"weather process_message is incomplete - skipping {message.topic()}")