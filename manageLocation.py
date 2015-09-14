__author__ = 'OTurki'

import pymongo
from threading import Thread
from pubnub import Pubnub
import os

class ConnectionToDatabase :

    db_conn=None
    database = None

    #penser a creer un pool de connections reutilisables vers la base
    #solution actuelle non envisageable en prod

    def __init__(self):
        self.connectToDatabase()


    def connectToDatabase(self) :
        # Connexion au serveur de Mongo DB

        try:
            # db_conn=pymongo.MongoClient("mongodb://admin:3usRy1SmJ8gH@127.0.0.1:37391/")
            db_conn=pymongo.MongoClient()
            print("Connected successfully!!!")
            print(db_conn)
        except pymongo.errors.ConnectionFailure :
            print("Could not connect to MongoDB: %s")


        # Connexion a la base du projet
        #db = db_conn["mongodbtest0"]
        db = db_conn["GPSLocationDB"]


        #Initialisation des variables globales
        ConnectionToDatabase.database = db


    def getCollection(self, collection_name):

        #Connexion a la collection
        collection = ConnectionToDatabase.database[collection_name]

        #function result
        return collection

class GenericDAO :

    # Mettre a jour un enregistrement dans une collection et inserer si pas d'enregistrement trouve
    def updateObjects(self, collection, objectCriteria, objectUpdate):

        updateResult = collection.update(objectCriteria, objectUpdate, upsert = True)

        return updateResult

class manageGPSLocation(Thread) :

    test = os.environ
    pubnub_publish_key = os.environ['PUBNUB_PUBLISH_KEY']
    pubnub_subscribe_key = os.environ['PUBNUB_SUBSCRIBE_KEY']

    mongodb_connection = None
    collection = None

    # def __init__(self,server,port,queueRef,pnb):
    def __init__(self):
        Thread.__init__(self)
        mongodb_connection = ConnectionToDatabase()
        collection = mongodb_connection.getCollection("steeds")
        self.pubnub_settings = Pubnub(publish_key=manageGPSLocation.pubnub_publish_key,subscribe_key=manageGPSLocation.pubnub_subscribe_key)
        self.pubnub_channel = "channel_test"
        # self.clientQueue = queueRef
		# self.pnb = pnb



    def subscriber_callback(self, message, channel):
        print(message)

    def subscriber_error(self, message):
            print("ERROR : "+message)

    def connect(self, message):
        print("CONNECTED")
        self.pubnub_settings.publish(channel=self.pubnub_channel, message="Connected to pubnub")

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