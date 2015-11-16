__author__ = 'OTurki'


from DAO.GenericDAO import GenericDAO, ConnectionToDatabase
from threading import Thread
from pubnub import Pubnub
import os
import sched


class manageSteeds(Thread) :

    test = os.environ
    pubnub_publish_key = os.environ['PUBNUB_PUBLISH_KEY']
    pubnub_subscribe_key = os.environ['PUBNUB_SUBSCRIBE_KEY']

    mongodb_connection = None
    collection = None

    def __init__(self):
        Thread.__init__(self)
        mongodb_connection = ConnectionToDatabase()
        manageSteeds.steeds_collection = mongodb_connection.getCollection("available_steeds")
        manageSteeds.deliveries_collection = mongodb_connection.getCollection("temp_deliveries")
        self.pubnub_settings = Pubnub(publish_key=manageSteeds.pubnub_publish_key,subscribe_key=manageSteeds.pubnub_subscribe_key)
        # Rename to location channel
        self.pubnub_channel = "steeds_channel"
        self.genericDAO = GenericDAO()
        self.sceduler = sched.scheduler() # how to instansiate a scheduler

    def subscriber_callback(self, message, channel):
        x = 1

    def subscriber_error(self, message):
            print("ERROR : "+message)

    def connect(self, message):
        print("CONNECTED TO STEEDS CHANNEL")

    def reconnect(self, message):
        print("RECONNECTED TO STEEDS CHANNEL")

    def disconnect(self, message):
        print("DISCONNECTED FROM STEEDS CHANNEL")

    def subscribe(self):
        # souscrire au channel
        self.pubnub_settings.subscribe(channels=self.pubnub_channel, callback=self.subscriber_callback, error=self.subscriber_error
                                       ,connect=self.connect, reconnect=self.reconnect, disconnect=self.disconnect)

    def publish(self, message):
        self.pubnub_settings.publish(channel=self.pubnub_channel, message=message)

    def unsubscribe(self):

        # se desinscire du channel
        self.pubnub_settings.unsubscribe(self.pubnub_channel)



    #Out of Pubnub functions

    def manageClientDeliveryRequest(self, iddelivery):

        # Search delivery by id (criteria)
        delivery_criteria = {}
        delivery_criteria["_id"] = iddelivery

        temp_delivery = self.genericDAO.getOneObject(self, self.deliveries_colllection, delivery_criteria)

        #Extract pickup coordinates from temporary delivery
        client_coordinates = [temp_delivery["pickup_lng"], temp_delivery["pickup_lat"]]

        # Get nearest steeds to pickup location
        nearest_steeds = self.genericDAO.getNearestSteeds(self, self.steeds_collection, client_coordinates)

        import sched



    def sendDeliveryRequestToSeeder(self, available_seeders, iddelivery):
        self.publish("XXXX test message")

        scheduler.enter(2, 1, print_event)
        scheduler.enter(3, 1, print_event)

        scheduler.run()

    def sendRequestsToSteeds(self,nearestSteed):

        steedRequest = {}
        steedRequest["msg_code"] = "00"





