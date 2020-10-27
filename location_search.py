import google.auth
import os
import google.cloud.bigquery as bigquery
import json
import logging
from flask import Flask, request, jsonify
import googlemaps
import datetime
import requests

app = Flask(__name__)

global cred_file
cred_file = "./credentials.json"

global apiKey_file
apiKey_file = "./api.txt"

class Authenticate:
    def __init__(self, gcp_file, gmap_file):
        self.cred_file = gcp_file
        self.apiKey_file = open(gmap_file, "r")

    def authenticate(self):
        try:
            rel_path = os.path.relpath(self.cred_file) 
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = rel_path
            credentials, your_project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
            return credentials, your_project_id
        except Exception as e:
            logger.error("Error occured in authentication:{}".format(e))

    def bq_client(self):
        try:
            credentials, your_project_id = self.authenticate()
            bqclient = bigquery.Client(credentials=credentials,project=your_project_id)
            return bqclient
        except Exception as e:
            logger.error("Error occured in creating gcp client:{}".format(e))
            
    def gmap_client(self):
        try:
            apiKey_string = self.apiKey_file.read()
            gmapclient = googlemaps.Client("{}".format(apiKey_string))
            return gmapclient
        except Exception as e:
            logger.error("Error occured in creating gmap client:{}".format(e))

class QueryFormatter:
    def __init__(self,keyword):
        self.keyword = keyword

    def query_selector(self):
        try:
            selector = {
                "GET_PLACE_DETAILS":

                "SELECT * FROM `trim-odyssey-292510.ConnectingSportsDataset.PlacesDB` where placeId = '$placeId'"
                }
            return selector.get(self.keyword)
        except Exception as e:
            logger.error("Error occured in selecting query:{}".format(e))

class Services:
    def __init__(self, BqClient, GmapClient):
        self.BqClient = BqClient
        self.GmapClient = GmapClient
        self.apiKey_string = open(apiKey_file, "r")
        
    def getGPSLocation(self):
        try:
            coordinates = self.GmapClient.geolocate()
            reverse_geocode_result = self.GmapClient.reverse_geocode((coordinates['location']['lat'], coordinates['location']['lng']),result_type="street_address")
            formatted_adress = reverse_geocode_result[0]['formatted_address']
            place_id = reverse_geocode_result[0]['place_id']
            coordinates.update({'formatted_adress':formatted_adress,'place_id':place_id})
            return {"Result":coordinates}
        except Exception as e:
            logger.error("Error occured in getGPSLocation:{}".format(e))
            
    def getUserLocation(self, UserAddressString):
        try:
            result = {}
            GPSLocation = self.getGPSLocation()["Result"]
            if UserAddressString == "No_ADDRESS" or int(GPSLocation["accuracy"]) <= 500:
                result = GPSLocation
            else:
                geocode_result = self.GmapClient.geocode(str(UserAddressString))
                geocode_result = geocode_result[0]
                coordinates = geocode_result["geometry"]["location"]
                formatted_adress = geocode_result["formatted_address"]
                place_id = geocode_result["place_id"]
                result.update({'location':coordinates,'accuracy':100,'formatted_adress':formatted_adress,'place_id':place_id})
            return {"Result":result}
        except Exception as e:
            logger.error("Error occured in getUserLocation:{}".format(e))

    def searchPlaces(self, UserAddressString, searchString = "Sports Complexes And Games Facilities"):
        try:
            if UserAddressString == "No_ADDRESS":
                UserCoordinates = self.getGPSLocation()["Result"]["location"]
            else:
                UserCoordinates = self.getUserLocation(UserAddressString)["Result"]["location"]
            PlacesSearch = self.GmapClient.places_nearby(keyword=searchString, location = UserCoordinates, rank_by = 'distance')
            '''URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
            PARAMS = {}
            PARAMS["key"] = "{}".format(self.apiKey_string.read())
            PARAMS["location"] = str(UserCoordinates["lat"]) + "," + str(UserCoordinates["lng"])
            PARAMS["keyword"] = searchString
            PARAMS["rankby"] = "distance"
            r = requests.get(url = URL, params = PARAMS) 
            PlacesSearch = r.json() '''
            PlacesResult = PlacesSearch['results']
            for places in PlacesResult:
                if "photos" in places.keys():
                    places.pop("photos")
                if "plus_code" in places.keys():
                    places.pop("plus_code")
                if "types" in places.keys():
                    places.pop("types")
                if "reference" in places.keys():
                    places.pop("reference")
                if "scope" in places.keys():
                    places.pop("scope")
                if "geometry" in places.keys():
                    places["location"] = places["geometry"]["location"]
                    places.pop("geometry")
                if "vicinity" in places.keys():
                    places["formatted_address"]=places.pop("vicinity")
            return {"Result":PlacesResult}
            
        except Exception as e:
            logger.error("Error occured in searchPlaces:{}".format(e))

    def SelectPlace(self,keyword,place_id):
        try:
            PlaceDetails = self.GmapClient.place(place_id = place_id, fields = ['formatted_address', 'geometry', 'icon', 'name', 'permanently_closed', 'place_id', 'url', 'formatted_phone_number', 'opening_hours', 'website', 'rating'])["result"]
            if "geometry" in PlaceDetails.keys():
                PlaceDetails["location"] = PlaceDetails["geometry"]["location"]
                PlaceDetails.pop("geometry")
            if "opening_hours" in PlaceDetails.keys():
                PlaceDetails["opening_hours"].pop("periods")
            query = QueryFormatter(keyword)
            query_string = query.query_selector()
            query_string=query_string.replace("$placeId", place_id)
            df = (
                self.BqClient.query(query_string)
                .result()
                .to_dataframe()
            )
            df_data = df[['placeId','registered']]
            df_result = df_data.to_json(orient="records")
            df_result = json.loads(df_result)
            if "placeId" in df_result[0].keys() and df_result[0]["placeId"] == PlaceDetails["place_id"]:
                PlaceDetails["registered"] = df_result[0]["registered"]
            else:
                PlaceDetails["registered"] = False
            return {"Result":PlaceDetails}
        except Exception as e:
            logger.error("Error occured in SelectPlace:{}".format(e))

    def PlaceDistance(self,place_id, UserAddressString):
        try:
            DestinationDetails = self.GmapClient.place(place_id = place_id, fields = ['geometry'])["result"]
            DestinationLocation = DestinationDetails["geometry"]["location"]
            if UserAddressString == "No_ADDRESS":
                SourceDetails = self.getGPSLocation()["Result"]
            else:
                SourceDetails = self.getUserLocation(UserAddressString)["Result"]
            SourceLocation =  SourceDetails["location"]
            DistanceDetails = self.GmapClient.distance_matrix(origins = SourceLocation,destinations = DestinationLocation,mode="transit",departure_time=datetime.datetime.now())
            result = {}
            result['destination_addresses'] = DistanceDetails['destination_addresses'][0]
            result['origin_addresses'] = DistanceDetails['origin_addresses'][0]
            result['distance'] = DistanceDetails["rows"][0]["elements"][0]["distance"]["text"]
            result['duration'] = DistanceDetails["rows"][0]["elements"][0]["duration"]["text"]
            return {"Result":result}
        except Exception as e:
            logger.error("Error occured in PlaceDistance:{}".format(e))

@app.route('/get_user_location/data/gps_detected_address')
def gps_detected_address():
    try:
        Result = Services(BqClient,GmapClient).getGPSLocation()
        logger.info("The result is:",str(Result))
        return Result
    except Exception as e:
        logger.error("Error occured in gps detection address service as: " + str(e))

@app.route('/get_user_location/data/user_input_address')
def user_input_address():
    try:
        AddressString = str(request.args.get('AddressString',default= 'No_ADDRESS'))
        Result = Services(BqClient,GmapClient).getUserLocation(UserAddressString=AddressString)
        logger.info("The result is:",str(Result))
        return Result
    except Exception as e:
        logger.error("Error occured in user input address service as: " + str(e))

@app.route('/get_user_location/data/places_search')
def places_search():
    try:
        AddressString = str(request.args.get('AddressString',default= 'No_ADDRESS'))
        Result = Services(BqClient,GmapClient).searchPlaces(UserAddressString=AddressString)
        logger.info("The result is:",str(Result))
        return Result
    except Exception as e:
        logger.error("Error occured in places search service as: " + str(e))

@app.route('/get_user_location/data/places_details')
def places_details():
    try:
        place_id = str(request.args['placeId'])
        Result = Services(BqClient,GmapClient).SelectPlace(keyword = "GET_PLACE_DETAILS",place_id=place_id)
        logger.info("The result is:",str(Result))
        return Result
    except Exception as e:
        logger.error("Error occured in places details service as: " + str(e))

@app.route('/get_user_location/data/place_distance')
def place_distance():
    try:
        place_id = str(request.args['placeId'])
        AddressString = str(request.args.get('AddressString',default= 'No_ADDRESS'))
        Result = Services(BqClient,GmapClient).PlaceDistance(place_id=place_id, UserAddressString = AddressString)
        logger.info("The result is:",str(Result))
        return Result
    except Exception as e:
        logger.error("Error occured in place distance service as: " + str(e))

@app.errorhandler(500)
def internal_error(error):
    utc_datetime = datetime.datetime.utcnow()
    utc_datetime = utc_datetime.strftime("%Y-%m-%d %H:%M:%SZ")
    error_500 = jsonify({
        'timestamp': utc_datetime,
        'error': error.code,
        'error status': str(error),
        'Message': 'The server encountered an unexpected condition that prevented it from fulfilling the request.',
        'path': '/get_user_location/data/'
    })
    logger.error(error_500)
    return error_500

@app.errorhandler(404)
def not_found(error):
    utc_datetime = datetime.datetime.utcnow()
    utc_datetime = utc_datetime.strftime("%Y-%m-%d %H:%M:%SZ")
    error_404 = jsonify({
        'timestamp': utc_datetime,
        'error': error.code,
        'error status': str(error),
        'Message': 'The request was valid, but no results were returned.',
        'path': '/get_user_location/data/'
    })
    logger.error(error_404)
    return error_404

@app.errorhandler(403)
def not_found(error):
    utc_datetime = datetime.datetime.utcnow()
    utc_datetime = utc_datetime.strftime("%Y-%m-%d %H:%M:%SZ")
    error_403 = jsonify({
        'timestamp': utc_datetime,
        'error': error.code,
        'error status': str(error),
        'Message': 'You have exceeded the request limit that you configured in the Google Cloud Platform Console.',
        'path': '/get_user_location/data/'
    })
    logger.error(error_403)
    return error_403

@app.errorhandler(400)
def not_found(error):
    utc_datetime = datetime.datetime.utcnow()
    utc_datetime = utc_datetime.strftime("%Y-%m-%d %H:%M:%SZ")
    error_400 = jsonify({
        'timestamp': utc_datetime,
        'error': error.code,
        'error status': str(error),
        'Message': 'Your API key is not valid for the Geolocation API or The request body is not a valid JSON.',
        'path': '/get_user_location/data/'
    })
    logger.error(error_400)
    return error_400

if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    BqClient = Authenticate(cred_file, apiKey_file).bq_client()
    GmapClient = Authenticate(cred_file, apiKey_file).gmap_client()
    app.run(host='0.0.0.0', port=7643, debug=True)