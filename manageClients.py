__author__ = 'OTurki'

from DAO.GenericDAO import GenericDAO, ConnectionToDatabase
from threading import Thread
from pubnub import Pubnub
import os
import manageSteeds

class manageClients(Thread) :

    test = os.environ
    pubnub_publish_key = os.environ['PUBNUB_PUBLISH_KEY']
    pubnub_subscribe_key = os.environ['PUBNUB_SUBSCRIBE_KEY']

    mongodb_connection = None
    collection = None

    def __init__(self):
        Thread.__init__(self)
        mongodb_connection = ConnectionToDatabase()
        manageClients.deliveries_collection = mongodb_connection.getCollection("temp_deliveries")
        self.pubnub_settings = Pubnub(publish_key=manageClients.pubnub_publish_key,subscribe_key=manageClients.pubnub_subscribe_key)
        # Rename to location channel
        self.pubnub_channel = "clients_channel"
        self.genericDAO = GenericDAO()
        self.manageSteeds = manageSteeds()

    def subscriber_callback(self, message, channel):
        print(message)

        # Inserer la demande de livraison faite par le client
        if(message["msg_code"] == "XX") :
            # inserer la demande de livraison dans une collection temporaire
            id_delivery = self.genericDAO.insertObject(manageClients.deliveries_collection, message)




    def subscriber_error(self, message):
            print("ERROR : "+message)

    def connect(self, message):
        print("CONNECTED TO LOCATION CHANNEL")

    def reconnect(self, message):
        print("RECONNECTED TO LOCATION CHANNEL")

    def disconnect(self, message):
        print("DISCONNECTED FROM LOCATION CHANNEL")

    def subscribe(self):
        # souscrire au channel
        self.pubnub_settings.subscribe(channels=self.pubnub_channel, callback=self.subscriber_callback, error=self.subscriber_error
                                       ,connect=self.connect, reconnect=self.reconnect, disconnect=self.disconnect)

    def publish(self, message):
        self.pubnub_settings.publish(channel=self.pubnub_channel, message=message)

    def unsubscribe(self):

        # se desinscire du channel
        self.pubnub_settings.unsubscribe(self.pubnub_channel)




