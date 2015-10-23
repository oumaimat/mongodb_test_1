__author__ = 'OTurki'

from manageLocation import manageGPSLocation

if __name__ == "__main__":
    manageGPS = manageGPSLocation()

    manageGPS.subscribe()
