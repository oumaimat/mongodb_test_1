__author__ = 'OTurki'

from DAO.GenericDAO import GenericDAO, ConnectionToDatabase
from threading import Thread
from pubnub import Pubnub
import os

class manageGPSLocation(Thread) :

    test = os.environ
    pubnub_publish_key = os.environ['PUBNUB_PUBLISH_KEY']
    pubnub_subscribe_key = os.environ['PUBNUB_SUBSCRIBE_KEY']

    mongodb_connection = None
    collection = None

    def __init__(self):
        Thread.__init__(self)
        mongodb_connection = ConnectionToDatabase()
        manageGPSLocation.collection = mongodb_connection.getCollection("steeds")
        self.pubnub_settings = Pubnub(publish_key=manageGPSLocation.pubnub_publish_key,subscribe_key=manageGPSLocation.pubnub_subscribe_key)
        self.pubnub_channel = "channel_test"
        self.genericDAO = GenericDAO()

    def subscriber_callback(self, message, channel):
        print(message)
        criteriaDict = {}
        criteriaDict["_id"]=message["_id"]

        updateDict = {}
        subUpdateDict = {}

        subUpdateDict["type"]="Point"
        subUpdateDict["coordinates"] = [message["lng"],message["lat"]]

        updateDict["location"]=subUpdateDict

        self.genericDAO.updateObjects(manageGPSLocation.collection, criteriaDict, updateDict)


    def subscriber_error(self, message):
            print("ERROR : "+message)

    def connect(self, message):
        print("CONNECTED")

    def reconnect(self, message):
        print("RECONNECTED")

    def disconnect(self, message):
        print("DISCONNECTED")

    def subscribe(self):
        # souscrire au channel
        self.pubnub_settings.subscribe(channels=self.pubnub_channel, callback=self.subscriber_callback, error=self.subscriber_error
                                       ,connect=self.connect, reconnect=self.reconnect, disconnect=self.disconnect)

    def publish(self, message):
        self.pubnub_settings.publish(channel=self.pubnub_channel, message=message)

    def unsubscribe(self):

        # se desinscire du channel
        self.pubnub_settings.unsubscribe(self.pubnub_channel)



if __name__ == "__main__":
    manageGPS = manageGPSLocation()

    # manageGPS.publish("Test message from Pubnub before connection")
    manageGPS.subscribe()
    # manageGPS.publish("Test message from Pubnub")
    #manageGPS.unsubscribe()