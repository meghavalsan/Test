"""Script to publish Json data to Kafka.

The script should take Kafka topic, key, Json file folder as input and
publish JSONs mentioned in the design document to Kafka.
"""

# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Vinakar Singh' <vinakar.singh@auto-grid.com>

import sys
import os
import json
import time
from autogrid.foundation.messaging.kafkaproducer import KafkaProducer
from autogrid.foundation.util.Logger import Logger


class PublishToKafka(object):

    """Send Json data to Kafka.

    Reads Json files and send them line by line to kafka.
    """

    def __init__(self):
        self.__logger = Logger.get_logger(self.__class__.__name__)

    def publish_json_to_kafka(self, topic, key, data_dir):
        """Publish Json data to Kafka.

        Parameters
        ----------
        topic : str
            Topic, to which messages will be published
        key : str
            Key will be used for partitioning messages
        data_dir : str
            Json data dir path
        """
        kafka = KafkaProducer(props={'url': '10.11.8.41:6667'})
        try:
            n = 0
            for file in os.listdir(data_dir):
                fpath = os.path.join(data_dir, file)
                json_string = ' '.join([line.strip() for line in open(fpath, 'r')])
                print json_string
                print '\n'
                json_object = json.loads(json_string)
                data = {'event_type': 'pam.incoming.message', 'payload': json_object}
                data['payload']['start_time_utc'] = '2015-01-15T05:00:00Z'
                data['payload']['end_time_utc'] = '2017-01-15T05:00:00Z'
                print kafka.publish(topic=topic, message=str(json.dumps(data)))
                n += 1
                time.sleep(0.05)
            print 'Count: ' + str(n)

        except Exception:
            self.__logger.error('Error in publishing data to kafka')
            raise


if __name__ == '__main__':
    print "main"
    if len(sys.argv) < 4:
        print 'Usage: ' + sys.argv[0] + '<topic> <key> <input_data_dir>'
        exit(-1)
    TOPIC = sys.argv[1]
    KEY = sys.argv[2]
    DATA_DIR = sys.argv[3]
    PUBLISH_KAFKA = PublishToKafka()
    PUBLISH_KAFKA.publish_json_to_kafka(TOPIC, KEY, DATA_DIR)
