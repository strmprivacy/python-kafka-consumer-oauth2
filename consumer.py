import json
import threading
import time
import kafka
import typedconfig.source
import requests_oauthlib
from kafka.oauth.abstract import AbstractTokenProvider
from oauthlib.oauth2 import BackendApplicationClient


# for oauth debugging
# import logging
# import sys
#  log = logging.getLogger('requests_oauthlib')
#  log.addHandler(logging.StreamHandler(sys.stdout))
#  log.setLevel(logging.DEBUG)

@typedconfig.section("kafka")
class KafkaConfig(typedconfig.Config):
    """ A data definition class that is used to interpret the 'config.ini' file

    See the config-example.ini file for typical content
    """
    bootstrap_servers = typedconfig.key(cast=str)
    topic = typedconfig.key(cast=str)
    client_id = typedconfig.key(cast=str)
    secret = typedconfig.key(cast=str)
    token_uri = typedconfig.key(cast=str)
    group = typedconfig.key(cast=str)


class TokenProvider(threading.Thread, AbstractTokenProvider):
    """
    The implementation of the AbstractTokenProvider that handles token refresh.
    #token() -> the access token
    """

    def __init__(self, config: KafkaConfig):
        super().__init__()
        self.setDaemon(True)
        self.config = config
        self.delay = 0  # the number of seconds until the refresh
        self.access_token = None  # the retrieved access token for refresh
        self.fetch_token()

    def fetch_token(self, token=None):
        """
        interact with the OAuth 2 token endpoint.

        :param token:  existing token (if any) for refresh purposes
        :return:
        """
        c = self.config
        if token is not None:  # we're refreshing
            token['expires_at'] = time.time() - 10
        client = BackendApplicationClient(client_id=c.client_id)
        oauth = requests_oauthlib.OAuth2Session(client=client, token=token,
                                                auto_refresh_url=c.token_uri)
        self.access_token = oauth.fetch_token(token_url=c.token_uri,
                                              client_id=c.client_id,
                                              client_secret=c.secret)
        self.delay = self.access_token['expires_in'] - 55  # one minute early

    def token(self):
        """
        the interface specified by the ABC
        :return:  the access token. Note that this will explode if there's no
                  token available.
        """
        return self.access_token['access_token']

    def run(self):
        while True:
            time.sleep(self.delay)
            self.fetch_token(self.access_token)


def run(config: KafkaConfig):
    token_provider = TokenProvider(config)
    token_provider.start()
    consumer = kafka.KafkaConsumer(config.topic,
                                   bootstrap_servers=config.bootstrap_servers,
                                   group_id=config.group,
                                   security_protocol="SASL_SSL",
                                   sasl_mechanism="OAUTHBEARER",
                                   sasl_oauth_token_provider=token_provider)
    for msg in consumer:
        key_link = msg.key.decode('utf-8')
        event = json.loads(msg.value.decode('utf-8'))
        # here you have key the event keyLink (as string) and data the full
        # event data
        print(event)


if __name__ == '__main__':
    run(KafkaConfig(sources=[typedconfig.source.IniFileConfigSource("config.ini")]))
