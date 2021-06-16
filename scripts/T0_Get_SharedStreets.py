# Name: T0_Get_SharedStreets.py
# Purpose: Geoprocessing tool that requests SharedStreets roadway features
# within a given geographic extent and converts those features to an Esri
# polyline feature class.
# Author: Stewart Rouse, Esri and Fehr & Peers
# Last Modified: 9/18/2020
# Python Version: 3.6
# --------------------------------
# Copyright 2020 FHWA
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# --------------------------------

import arcpy
import os
import json
import urllib.request as urlopen
import urllib.parse as urlencode
import urllib.request as request

#The shared streets api requires the coordinates to be in geographic coordinates AKA decimal degrees
#this variable is created to be used when a spatial reference needs to be defined as such
wgs84 = arcpy.SpatialReference(4326)

def getPolygon(poly):
    #location for a temporary file if the data needs to be projected into a geographic coordinate system
    # output = os.path.join(os.path.dirname(outputFC),"temp")
    output = os.path.join(arcpy.env.scratchGDB,"temp")

    #The search cursor is used to get the geometry from the polygon that was drawn on the map.
    #Information about the polygon is obtained using the Describe method. The spatial reference of the polygon is checked.
    #If the spatial reference of the geometry is a projected coordinate system, it is changed to a geographic coordinate system
    #The extent of the polygon is obtained then sent to the sendRequst() function
    for row in arcpy.da.SearchCursor(poly, ["SHAPE@"]):
            queryPolyInfo = arcpy.Describe(poly)
            sr = queryPolyInfo.spatialReference
            if sr.type == "Projected":
                arcpy.Project_management(poly,output,wgs84)
                for row in arcpy.da.SearchCursor(output,["SHAPE@"]):
                    queryPolyExtent = row[0].extent
                    sendRequest(queryPolyExtent)
                arcpy.Delete_management(output)
                    
            else:
                queryPolyExtent = row[0].extent
                sendRequest(queryPolyExtent)
    
#This function sends a request to the SharedStreets API to get the centerlines that intersect with the extent of the input polygon
def sendRequest(poly):
    #The url parameters are created. For more info: https://github.com/sharedstreets/sharedstreets-api
    data = urlencode.urlencode({'authKey':ssKey,'bounds':str(poly.XMin) + "," + str(poly.YMin) + "," + str(poly.XMax) +"," + str(poly.YMax)})

    fullURL = "https://api.sharedstreets.io/v0.1.0/geom/within?" + data
    arcpy.AddMessage(fullURL)

    #The request is sent, and the data is parsed from string to json.  Only the features are sent to the createFC() function.
    #Additional information is provided, but that information is not relevant to this process
    req = request.Request(fullURL)
    response = urlopen.urlopen(req)
    response_bytes = response.read()
    ssFeatures = json.loads(response_bytes.decode('UTF-8'))["features"]
    createFC(ssFeatures)

#This function interates over the JSON representations of lines to create polylines which are subsequently inserted into a new feature class
def createFC(features):
    #Create feature class with a polyline geometry type
    arcpy.CreateFeatureclass_management(os.path.dirname(outputFC),os.path.basename(outputFC),"POLYLINE","","","",wgs84)
    #Add field to the feature class that will contain the Shared Streets ID of the polyline
    arcpy.AddField_management(outputFC,"ssID","TEXT")

    #Create insert cursor that will be used to add data to the feature class
    insertCursor = arcpy.da.InsertCursor(outputFC,["ssID","SHAPE@"])
    #Loop through the JSON features to create proper polylines
    for feature in features:
        stringCoords = feature["geometry"]["coordinates"]
        polylineArray = arcpy.Array()
        for stringCoord in stringCoords:
            #Each polyline is an array of Point geometry objects.  Each point has a latitude and longitude value.
            polylineArray.append(arcpy.Point(float(stringCoord[0]),float(stringCoord[1])))
        #The polyline is created from the array of Point geometry objects
        polyline = arcpy.Polyline(polylineArray,wgs84)
        #The Shared Streets ID obtained from the JSON representation of the feature
        id = feature["properties"]["id"]
        #The polyline and Shared Streets ID are added to the feature class
        insertCursor.insertRow((id,polyline))
    del insertCursor




# This test allows the script to be used from the operating system 
# command prompt (stand-alone), in a Python IDE, as a geoprocessing 
# script tool, or as a module imported in another script.
if __name__ == '__main__':
    # Define inputs
    inputPolygon = arcpy.GetParameterAsText(0)
    ssKey = arcpy.GetParameterAsText(1)
    outputFC = arcpy.GetParameterAsText(2)

    getPolygon(inputPolygon)