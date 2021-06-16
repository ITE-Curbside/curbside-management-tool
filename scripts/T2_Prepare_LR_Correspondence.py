# Name: T2_Prepare_LR_Correspondence.py
# Purpose: Geoprocessing tool that establishes correspondences between
# an input curbside regulation feature class and an input centerline
# feature class. Exports two route feature classes (curb features and
# centerlines).
# Author: Fehr & Peers
# Last Modified: 3/11/2020
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
import math
import arcpy
import pandas as pd
import numpy as np

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

def field_exist(featureclass, fieldname):
    """ArcFunction
     Check if a field in a feature class field exists and return true it does, false if not.- David Wasserman"""
    fieldList = arcpy.ListFields(featureclass, fieldname)
    fieldCount = len(fieldList)
    if (fieldCount >= 1) and fieldname.strip():  # If there is one or more of this field return true
        return True
    else:
        return False

def add_new_field(in_table, field_name, field_type, field_precision="#", field_scale="#", field_length="#",
                  field_alias="#", field_is_nullable="#", field_is_required="#", field_domain="#"):
    """ArcFunction
    Add a new field if it currently does not exist. Add field alone is slower than checking first.- David Wasserman"""
    if field_exist(in_table, field_name):
        arc_print(field_name + " Exists")
        return False # False because field was not added
    else:
        arc_print("Adding " + field_name)
        arcpy.AddField_management(in_table, field_name, field_type, field_precision, field_scale,
                                  field_length,
                                  field_alias,
                                  field_is_nullable, field_is_required, field_domain)
        return True # True because field was added

def create_route_field(in_table, route_field, field_type='LONG'):
    if add_new_field(in_table, route_field, field_type):
        oid_field = arcpy.Describe(in_table).OIDFieldName
        arcpy.management.CalculateField(in_table, route_field, "!{}!".format(oid_field))
        arc_print('Populated field {} with content of field {}'.format(route_field, oid_field))
        return True # True because field was created
    arc_print('Did not modify content of field {}'.format(route_field))
    return False # False because field was not created

def arcgis_table_to_df(in_fc, input_fields=None, query=""):
    """Function will convert an arcgis table into a pandas dataframe with an object ID index, and the selected
    input fields using an arcpy.da.SearchCursor.
    :param - in_fc - input feature class or table to convert
    :param - input_fields - fields to input to a da search cursor for retrieval
    :param - query - sql query to grab appropriate values
    :returns - pandas.DataFrame"""
    OIDFieldName = arcpy.Describe(in_fc).OIDFieldName
    if input_fields:
        final_fields = [OIDFieldName] + input_fields
    else:
        final_fields = [field.name for field in arcpy.ListFields(in_fc)]
    data = [row for row in arcpy.da.SearchCursor(in_fc, final_fields, where_clause=query)]
    fc_dataframe = pd.DataFrame(data, columns=final_fields)
    fc_dataframe = fc_dataframe.set_index(OIDFieldName, drop=True)
    return fc_dataframe


def prepare_lr_correspondence(in_centerlines, in_curb_features,
                              out_route_centerlines, out_route_curb_features,
                              centerline_route_field='corridor_ID', 
                              curb_feature_route_field='curb_feature_ID',
                              bandwidth='100 Feet', angle_threshold=20):
    """This tool establishes correspondences between an input curbside 
    regulation feature class and an input centerline feature class. Exports 
    two route feature classes (curb features and centerlines).
    @:param in_centerlines - file path to input centerline feature class
    @:param in_curb_features - file path to input curbside regulation feature class
    @:param out_route_centerlines - path at which to write output centerline routes
        feature class
    @:param out_route_curb_features - path at which to write output curbside
        regulation routes feature class
    @:param centerline_route_field - name of field to use as centerline route IDs
        (will create and populate with OIDs if it does not exist)
    @:param curb_feature_route_field - name of field to use as curbside regulation
        route IDs (will create and populate with OIDs if it does not exist)
    @:param bandwidth - maximum distance between curbside regulation feature
        endpoints and centerline routes
    @:param angle_threshold - maximum effective "angle" (in degrees) between
        centerlines and curbside regulation features"""
    # Set parameters and options
    mem_curb_features = 'mem_curb_features'
    mem_curb_features_matched = 'mem_curb_features_matched'
    mem_table = 'memory/mem_table' # just 'mem_table' doesn't work: "invalid table name" for some reason
    mem_vertices = 'mem_vertices'
    mem_located = 'mem_located'
    arcpy.env.overwriteOutput = True
    arcpy.env.workspace = 'memory'

    # Convert centerlines FC to routes FC
    if arcpy.Exists(out_route_centerlines) and arcpy.env.overwriteOutput:
        arcpy.management.Delete(out_route_centerlines)
    # Create route_field if it doesn't already exist, and populate it with the FC's OIDs
    create_route_field(in_centerlines, centerline_route_field)
    arcpy.lr.CreateRoutes(in_centerlines, centerline_route_field, out_route_centerlines)
    arc_print("Created centerline routes...")

    # Join fields from the input centerlines FC
    # First get input centerlines FC field names and remove would-be-duplicate field
    in_centerline_fields = [field.name for field in arcpy.ListFields(in_centerlines)]
    in_centerline_fields.remove(centerline_route_field)
    arcpy.management.JoinField(out_route_centerlines, centerline_route_field, 
                               in_centerlines, centerline_route_field,
                               in_centerline_fields)

    # At the very outset, populate a curb_feature_route_field to preserve
    # the original OIDs through the workflow (this modifies the input FC...)
    create_route_field(in_curb_features, curb_feature_route_field)

    # If the curbside FC has a 'primary' field, copy only primary features to in_mem FC
    if field_exist(in_curb_features, 'primary'):
        arcpy.analysis.Select(in_curb_features, mem_curb_features, 'primary = 1')
        arc_print('Copied primary curbside features to memory...')
    else: # Otherwise copy them all
        arcpy.management.CopyFeatures(in_curb_features, mem_curb_features)
        arc_print('Copied all curbside features to memory...')

    # Derive length_unit from bandwidth
    # This is necessary because we screen candidate associations by d_over_l,
    # the ratio of Distance_delta to curb_feature_length, so these two lengths
    # must be in the same linear units. Distance_delta is always in the units
    # specified in bandwidth, and we can specify the Length_Unit when calling
    # arcpy.management.AddGeometryAttributes(), but the unit names are different
    # in these two contexts.
    if bandwidth.split(' ')[1] == 'Feet':
        length_unit = 'FEET_US' # arcpy.Describe().spatialReference.linearUnitName == 'Foot_US'
    elif bandwidth.split(' ')[1] == 'Meters':
        length_unit = 'METERS' # arcpy.Describe().spatialReference.linearUnitName == 'Meter'
    else:
        raise ValueError('Bandwidth must be in either Feet or Meters')
    
    # Add LENGTH field containing curb features' length in the appropriate linear unit
    # If there is already a field named LENGTH, this will overwrite that field!
    arcpy.management.AddGeometryAttributes(mem_curb_features, 'LENGTH', length_unit) 
    arc_print("Added LENGTH to curb features...")

    # Create points at start and end vertices of each curb feature
    arcpy.management.FeatureVerticesToPoints(mem_curb_features, mem_vertices, 'BOTH_ENDS')
    arc_print("Created start and end vertex points...")

    # Locate the vertex points along centerline routes
    arcpy.lr.LocateFeaturesAlongRoutes(mem_vertices, out_route_centerlines, 
                                       centerline_route_field, bandwidth, mem_located, 
                                       centerline_route_field + ' Point m_located', 'ALL')
    arc_print("Located start and end points along centerline routes...")

    # Read the located events table in as a DataFrame
    located = arcgis_table_to_df(mem_located)

    # Group by curb_feature_ID and corridor_ID and aggregate key fields
    df = located.groupby([curb_feature_route_field, centerline_route_field]) \
                .agg({'m_located': [min, max, 'count'], 
                      'Distance': [min, max],
                      'LENGTH': 'first'}).reset_index()
    # Flatten multi-level columns
    df.columns = ['_'.join(x) if len(x[1]) else x[0] for x in df.columns.ravel()]
    
    # Filter df down to the important entries
    df = df[df['m_located_count'] > 1] # keep only centerlines on which both start and end vertices were located
    # this has a tiny chance of retaining curb features where one vertex was located twice and the other vertex
    # was not located at all, but we can probably catch those edge cases in other ways
    df = df[df['m_located_min'] != df['m_located_max']] # keep only centerlines where start and end vertices had different m_values

    # Calculate some key metrics we will use to identify the correct centerline to associate each curb feature with
    df['Distance_delta'] = (df['Distance_max'] - df['Distance_min']).abs() # we want the absolute difference between the two distances
    df['Distance_mean'] = (df['Distance_max'].abs() + df['Distance_min'].abs()) / 2 # we want the mean of the absolute distances to the route
    df['d_over_l'] = df['Distance_delta'] / df['LENGTH_first']

    # Filter out "bad" rows based on d_over_l and then use groupby to identify the closest nearly parallel centerline
    df = df[df['d_over_l'] < math.sin(math.radians(angle_threshold))]
    temp = pd.DataFrame(df.groupby(curb_feature_route_field)['Distance_mean'].idxmin())
    my_df = temp.join(df[[centerline_route_field, 'm_located_min', 'm_located_max']], on = 'Distance_mean')
    my_df = my_df.drop(columns = 'Distance_mean')
    my_df = my_df.reset_index() # bring curb_feature_route_field back into the columns
    my_df.columns = [curb_feature_route_field, centerline_route_field, 'm_from', 'm_to']

    # Export centerline and m_fields to ArcGIS table in memory
    # drawing on https://my.usgs.gov/confluence/display/cdi/pandas.DataFrame+to+ArcGIS+Table
    x = np.array(np.rec.fromrecords(my_df.values))
    names = my_df.columns.tolist()
    x.dtype.names = tuple(names)
    if arcpy.Exists(mem_table):
        arcpy.management.Delete(mem_table)
    arcpy.da.NumPyArrayToTable(x, mem_table)
    arc_print("Completed pandas processing of located events table...")
    
    # Join centerline and m_ fields to mem_curb_features
    names.remove(curb_feature_route_field)
    fields_to_join = ';'.join(names)
    arcpy.management.JoinField(mem_curb_features, curb_feature_route_field, 
                            mem_table, curb_feature_route_field, fields_to_join)

    # Select only those curb features with m-values (i.e. with an associated centerline)
    arcpy.analysis.Select(mem_curb_features, mem_curb_features_matched, 'm_from IS NOT NULL')

    # Delete output FC if necessary
    if arcpy.Exists(out_route_curb_features) and arcpy.env.overwriteOutput:
        arcpy.management.Delete(out_route_curb_features)
    # Convert in-memory line FC into routes FC in geodatabase
    # Note that Create Routes preserves ONLY the curb_feature_route_field
    # so we will need to later join the rest of the fields onto the routes FC
    arcpy.lr.CreateRoutes(mem_curb_features_matched, curb_feature_route_field,
                          out_route_curb_features, 'TWO_FIELDS', 'm_from', 'm_to')
    arc_print("Created curb feature routes...")

    # And join fields from the scratch curb features FC, because without those fields, these routes are useless!
    fields_to_exclude = [curb_feature_route_field, 'LENGTH', 'Shape__Length', 'Shape_Length']
    fields_to_join = [field.name for field in arcpy.ListFields(mem_curb_features_matched) \
                      if field.name not in fields_to_exclude]
    arcpy.management.JoinField(out_route_curb_features, curb_feature_route_field, 
                               mem_curb_features_matched, curb_feature_route_field,
                               fields_to_join)
    arc_print("Script complete...")

# This test allows the script to be used from the operating system 
# command prompt (stand-alone), in a Python IDE, as a geoprocessing 
# script tool, or as a module imported in another script.
if __name__ == '__main__':
    # Define inputs
    in_centerlines = arcpy.GetParameterAsText(0)
    in_curb_features = arcpy.GetParameterAsText(1)
    out_route_centerlines = arcpy.GetParameterAsText(2)
    out_route_curb_features = arcpy.GetParameterAsText(3)
    centerline_route_field = arcpy.GetParameterAsText(4)
    curb_feature_route_field = arcpy.GetParameterAsText(5)
    bandwidth = arcpy.GetParameterAsText(6)
    angle_threshold = float(arcpy.GetParameterAsText(7))

    prepare_lr_correspondence(in_centerlines, in_curb_features,
                              out_route_centerlines, out_route_curb_features,
                              centerline_route_field, curb_feature_route_field,
                              bandwidth, angle_threshold)
