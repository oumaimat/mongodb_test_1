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

        self.steeds_collection = mongodb_connection.getCollection("available_steeds")
        self.deliveries_collection = mongodb_connection.getCollection("temp_deliveries")
        self.deliveries_steeds_collection = mongodb_connection.getCollection("temp_deliveries_steeds")

        self.pubnub_settings = Pubnub(publish_key=manageSteeds.pubnub_publish_key,subscribe_key=manageSteeds.pubnub_subscribe_key)
        # Rename to location channel
        self.pubnub_channel = "steeds_channel"

        self.genericDAO = GenericDAO()

        self.scheduler = sched.scheduler() # Instansiate a scheduler

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
# how to instansiate a scheduler


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

        # Save available steeds for delivery
        delivery_steeds = {}
        delivery_steeds["iddelivery"] = iddelivery
        delivery_steeds["available_steeds"] = nearest_steeds

        self.genericDAO.insertObject(self.deliveries_steeds_collection, delivery_steeds)

        #Send delivery request to seeder
        self.sendDeliveryRequestToSteed(iddelivery)

    def sendDeliveryRequestToSteed(self, iddelivery):

        # Search delivery by id (criteria)
        delivery_criteria = {}
        delivery_criteria["_id"] = iddelivery

        temp_delivery = self.genericDAO.getOneObject(self, self.deliveries_colllection, delivery_criteria)

        #Check received_positive_response field
        received_response = temp_delivery["received_positive_response"]

        if(not(received_response)) :

            #Search available steeds for delivery
            delivery_steeds_criteria = {}
            delivery_steeds_criteria["iddelivery"] = iddelivery

            available_steeds = self.genericDAO.getOneObject(self, self.deliveries_steeds_collection, delivery_steeds_criteria)


            #Send steed delivery request
            self.publish("XXXX"+iddelivery+available_steeds[0]["_id"]+" "+"Other delivery details")

            #Delete 1st steed from available steeds list
            new_available_steeds = available_steeds[1:len(available_steeds)-1]

            #Update delivery available steeds

            update_search_criteria = {}
            update_search_criteria["iddelivery"] = iddelivery

            update_field = {}
            update_field["available_steeds"] = new_available_steeds


            #Add update function to Generic DAO from old projects


            #Schedule method call
            self.scheduler.enter(10, 1, self.sendDeliveryRequestToSteed(iddelivery) )
            self.scheduler.run()







