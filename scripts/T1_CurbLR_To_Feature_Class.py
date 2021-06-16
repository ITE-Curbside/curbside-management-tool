# Name: 1_CurbLR_To_Feature_Class.py
# Purpose: Geoprocessing tool that reads in a CurbLR dataset (in JSON 
# format) and exports an ESRI feature class of polylines corresponding 
# to each unique combination of geometry, daysOfWeek/timesOfDay, and 
# restriction.
# Author: Fehr & Peers
# Last Modified: 1/30/2020
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
# Import Modules
import arcpy
import pandas as pd
from pandas.io.json import json_normalize
import numpy as np
import os
import json
# import curbsidelib as csl

# functions from former curbsidelib
def arc_print(string, progressor_bool=False):
    """ This function is used to simplify using arcpy reporting for tool creation;
    if progressor bool is true it will create a tool label.
    :param - string - string to print out
    :param - progressor_bool - if true, also emits a progressor update in tool"""
    casted_string = str(string)
    if progressor_bool:
        arcpy.SetProgressorLabel(casted_string)
    arcpy.AddMessage(casted_string)
    print(casted_string)

sr_wgs = arcpy.SpatialReference(4326) # GeoJSON coordinates are always in WGS 84 https://tools.ietf.org/html/rfc7946#section-4

def convert_curblr_to_feature_class(json_path, out_path, out_sr=sr_wgs):
    """This tool reads in a CurbLR-compliant JSON file and converts it into a 
    temporally enabled stacked feature class of curbside regulations.
    @:param json_path - file path to .json file containing input CurbLR data
    @:param out_path - path at which to write output feature class
    @:param out_sr - spatial reference of output feature class. Defaults to WGS 84"""
    # Set parameters and options
    arcpy.env.overwriteOutput = True # not necessary?
    out_dir = os.path.dirname(out_path)
    out_fc_name = os.path.basename(out_path)
    if out_sr == '': # if no spatial reference is specified...
        try: # extract it from out_dir if that is a feature dataset
            out_sr = arcpy.Describe(out_dir).spatialReference
        except Exception: # if out_dir is not a feature dataset, fall back to WGS 84
            out_sr = sr_wgs

    # Read in JSON object
    with open(json_path) as json_file:
        data = json.load(json_file)

    # Use pandas json_normalize to create DataFrame
    df = json_normalize(data['features'], ['properties', 'regulations', 'timeSpans'], 
                        ['geometry', ['properties', 'location'], 
                        ['properties', 'regulations', 'rule'], 
                        ['properties', 'regulations', 'payment']],
                        errors = 'ignore')
    arc_print('Read in {} features from CurbLR JSON...'.format(len(df)))

    # Define helper functions for DataFrame manipulation
    def xy_to_polyline(xys, sr):
        return arcpy.Polyline(arcpy.Array([arcpy.Point(*coords) for coords in xys]), sr)

    def get_item_zero(column): # just here to handle errors when data are missing
        try:
            return column[0]
        except:
            return None

    def extract(column, field):
        try:
            return column[field]
        except:
            return None

    # Apply several vectorized functions to extract desired fields
    # This first group of extractions is mandatory: without these fields, the output would be useless
    # Therefore we do not wrap them in try/except expressions
    df['coordinates'] = df['geometry'].apply(extract, args = ['coordinates'])
    df['geom'] = df['coordinates'].apply(xy_to_polyline, args = [sr_wgs])
    for field in ['activity', 'priorityCategory', 'maxStay']:
        df[field] = df['properties.regulations.rule'].apply(extract, args = [field])
    df['maxStay'] = df['maxStay'].fillna(-1) # fill missing maxStay with magic value -1

    # The following extractions are optional: the tool should run even if they're missing from the input data
    # Therefore they get try/except expressions, one for each expected field
    try:
        df['timesOfDay_from'] = df['timesOfDay'].apply(lambda x: extract(get_item_zero(x), 'from'))
        df['timesOfDay_to'] = df['timesOfDay'].apply(lambda x: extract(get_item_zero(x), 'to'))
    except KeyError:
        df['timesOfDay_from'] = None
        df['timesOfDay_to'] = None
    try:
        df['effectiveDates_from'] = df['effectiveDates'].apply(lambda x: extract(get_item_zero(x), 'from'))
        df['effectiveDates_to'] = df['effectiveDates'].apply(lambda x: extract(get_item_zero(x), 'to'))
    except KeyError:
        df['effectiveDates_from'] = None
        df['effectiveDates_to'] = None
    try:
        df['SharedStreetID'] = df['properties.location'].apply(extract, args = ['shstRefId'])
    except KeyError:
        df['SharedStreetID'] = None
    try:
        df['payment'] = df['properties.regulations.payment'].apply(lambda x: 1 if x is not np.nan else 0)
    except KeyError:
        df['payment'] = 0 # Assume missing payment information means no payment is required

    # Prepare a string-formatted, cleaned-up version of daysOfWeek
    # See CurbLR_to_FC.ipynb for exploratory analysis used to design this approach
    # Create string version of column of lists
    if 'daysOfWeek.days' in df.columns: # different versions of pandas unpack via json_normalize differently!
        df['daysOfWeek'] = df['daysOfWeek.days'].astype(str)
    else:
        df['daysOfWeek'] = df['daysOfWeek'].apply(extract, args = ['days'])
        df['daysOfWeek'] = df['daysOfWeek'].astype(str)
    # Remove characters other than quotation marks, letters, and commas
    df['daysOfWeek'] = df['daysOfWeek'].str.strip('[]')
    df['daysOfWeek'] = df['daysOfWeek'].str.replace(" ", '')
    # Perform four replacements that together address the nine "problem patterns" found in the LA CurbLR dataset
    df['daysOfWeek'] = df['daysOfWeek'].str.replace("'m'", "'mo'")
    df['daysOfWeek'] = df['daysOfWeek'].str.replace("'mo','t'", "'mo','tu'")
    df['daysOfWeek'] = df['daysOfWeek'].str.replace("'we','t'", "'we','th'")
    df['daysOfWeek'] = df['daysOfWeek'].str.replace("'s'", "'su'")
    # Remove quotation marks
    df['daysOfWeek'] = df['daysOfWeek'].str.replace("'", '')

    # Document and handle missingness in date/time fields
    df['warnings'] = '' # start with blank field to house warnings
    df.loc[df['daysOfWeek'].isin(['nan', 'None']), 'warnings'] += 'daysOfWeek missing;'
    df.loc[df['daysOfWeek'].isin(['nan', 'None']), 'daysOfWeek'] = 'mo,tu,we,th,fr,sa,su' # set missing daysOfWeek to be all days

    df.loc[df['timesOfDay_from'].isna(), 'warnings'] += 'timesOfDay missing;'
    df['timesOfDay_from'] = df['timesOfDay_from'].fillna('0:00') # set missing timesOfDay_from to 0:00
    df['timesOfDay_to'] = df['timesOfDay_to'].fillna('23:59') # set missing timesOfDay_to to 23:59

    # Create duplicate features for each dayOfWeek (so that time sliders work well on all days)
    # Approach adapted from https://medium.com/@sureshssarda/pandas-splitting-exploding-a-column-into-multiple-rows-b1b1d59ea12e
    # In pandas 0.25 and above, df.explode() would also work well
    new_df = pd.DataFrame(df['daysOfWeek'].str.split(',').tolist()).stack()
    new_df = new_df.reset_index(level = 1) # stack() creates a MultiIndex; this kicks one MultiIndex level out into a column
    new_df.columns = ['primary', 'dayOfWeek'] # rename columns appropriately
    new_df['primary'] = (new_df['primary'] == 0).astype(int) # set only the zeroth duplicate feature as primary
    df = df.join(new_df) # join columns onto original DataFrame

    # Prepare datetime fields
    # 'mm/dd/yyyy hh:mm' parses effectively in ArcGIS, so we need to convert daysOfWeek into dd
    # Then we can just concatenate timesOfDay onto that
    # We use a dict to convert the first (string) entry in daysOfWeek into an int (1-7)
    # We use year 1900 because it started on a Monday and I (Drew) believe Monday is the first day of the week :)
    # See https://en.wikipedia.org/wiki/Common_year_starting_on_Monday

    day_dict = {'mo': '1',
                'tu': '2',
                'we': '3',
                'th': '4',
                'fr': '5',
                'sa': '6',
                'su': '7'}

    # Use dayOfWeek for the date of both timesOfDay_from_dt and timesOfDay_to_dt
    df['timesOfDay_from_dt'] = '1/' + df['dayOfWeek'].map(day_dict) + '/1900 '+ df['timesOfDay_from']
    df['timesOfDay_to_dt'] = '1/' + df['dayOfWeek'].map(day_dict) + '/1900 '+ df['timesOfDay_to']

    arc_print('Ready to export {} features (including duplicates) to feature class...'.format(len(df)))

    # Create feature class and add desired fields
    if arcpy.Exists(out_path): # manually delete existing data if necessary; theoretically arcpy.env.overwriteOutput should cover this but it often fails
        arcpy.management.Delete(out_path)
        
    arcpy.management.CreateFeatureclass(out_dir, out_fc_name, 'POLYLINE', spatial_reference = out_sr)

    fields = [['SharedStreetID', 'TEXT'],
              ['effectiveDates_from', 'TEXT'], # was DATE but we want time slider functionality to default to timesOfDay_from_dt
              ['effectiveDates_to', 'TEXT'], # was DATE
              ['daysOfWeek', 'TEXT'], # will be a comma-separated list of two-letter day abbreviations
              ['timesOfDay_from', 'TEXT'],
              ['timesOfDay_to', 'TEXT'],
              ['timesOfDay_from_dt', 'DATE'],
              ['timesOfDay_to_dt', 'DATE'],
              ['activity', 'TEXT'],
              ['priorityCategory', 'TEXT'],
              ['maxStay', 'SHORT'],
            #   ['noReturn', 'SHORT'], # dropped this field from our schema because it is almost never used and almost useless
              ['payment', 'SHORT'],
              ['warnings', 'TEXT'],
              ['primary', 'SHORT']]

    for field, dtype in fields:
        arcpy.management.AddField(out_path, field, dtype)
    arc_print('Created output feature class {}...'.format(out_fc_name))

    # Clean up DataFrame fields
    columns = [field[0] for field in fields]
    df = df[['geom'] + columns]

    # Use InsertCursor to write DataFrame to feature class
    with arcpy.da.InsertCursor(out_path, ['SHAPE@'] + columns) as cursor:
        i = 0
        for index, row in df.iterrows(): # .iterrows() is "slow" but plenty fast for this application
            values = [row[column] for column in columns]
            cursor.insertRow([row['geom']] + values)
            i += 1
            if not i % 10000: # every 10000 original rows, print update message
                arc_print('Wrote {} features...'.format(i))
                
    arc_print('Wrote {} features total...'.format(len(df)))
    arc_print("Script complete...")

# This test allows the script to be used from the operating system 
# command prompt (stand-alone), in a Python IDE, as a geoprocessing 
# script tool, or as a module imported in another script.
if __name__ == '__main__':
    # Define inputs
    json_path = arcpy.GetParameterAsText(0)
    out_path = arcpy.GetParameterAsText(1)
    out_sr = arcpy.GetParameterAsText(2)

    convert_curblr_to_feature_class(json_path, out_path, out_sr)

