# Name: T4_Recommend_Curbside_Treatments.py
# Purpose: This tool will take in fields identifying modal priorities, built environment contexts, ROW information,
#  and other factors in order to identify curbside treatment options.
# Author: Fehr & Peer
# Last Modified: 1/20/2020
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

import pandas as pd
import arcpy, os
import numpy as np
# import curbsidelib as csl
from arcgis.features import GeoAccessor, GeoSeriesAccessor

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


def modal_priority_sieve(row):
    """
    This function takes a row and checks where the modal priority applicable matches with the value of the modal priority columns. 
    When it matches, it further checks out if the modal priority column number falls withun appropriate priority rank range. If it 
    passes the test, it will return 1 otherwise 0. 
    :param - row
    :return - boolean array (1s and 0s)
    """
    mode = row["Mode Priority Applicable"]
    # Initialize rank to a value that will never satisfy the rank_range criterion below.
    # This way even if the modal priority applicable is not ranked for a certain segment,
    # this sieve will simply return 0 rather than crashing.
    rank = -1
    for i in range(1, 8):
        if row["Modal_Priority_" + str(i)] == mode:
            rank = i
    rank_range = row["Appropriate Priority Rank Range"].split("-")
    if rank >= int(rank_range[0]) and rank <= int(rank_range[1]):
        return 1
    else:
        return 0

def land_use_sieve(row):
    """This function takes a row and checks if a treatments land use orientation is one of those identified as a
    predominant land use on a corridor."""
    land_uses = [i.strip() for i in row["Appropriate Land Use"].split(",")]
    if row["Land_Use"] in land_uses:
        return 1
    else:
        return 0

def place_type_sieve(row):
    """This function takes a row and checks if a placetype meets the minimum multimodal orientation and density
    required for a treatment. This is done by checking for the place types number and making sure the treatment is
    in a place type as dense or denser as the used floor. """

    place_types = int("".join(filter(str.isdigit, row["Place Types"])))
    data_place_type = int("".join(filter(str.isdigit, row["Place_Type"])))
    if data_place_type <= place_types:
        return 1
    else:
        return 0

def row_available_sieve(row):
    """This function takes a row and checks if the right-of-way available meets the minimum identified for the treatment."""
    necessary_row = int("".join(filter(str.isdigit, row["Total ROW Necessary Per Direction"]))) * 0.3048 # convert feet (treatment table unit) to meters (shared-row unit)!!!
    if row["Available_ROW"]>= necessary_row:
        return 1
    else:
        return 0

def success_metric_sieve(row):
    """This function takes a row and checks if a success metric for a corresponding modal priority is already met. If a
    success metric is a 1, then all treatments that relate to that modal priority are not considered."""
    mode = str(row["Mode Priority Applicable"])
    success_col =  "{0}_Success_Metric".format(mode)
    try:
        success_value = row[success_col]
    except:
        success_value = 0
    return 1 - success_value # 0 if success_value == 1, 1 otherwise

# Global sieves dict makes it easier to work with arbitrarily many sieves later.
# The key:value format is field_name:function. Each function must accept one argument (row).
sieves = {'Modal_Priority_Sieve': modal_priority_sieve,
          'Land_Use_Sieve': land_use_sieve,
          'Place_Type_Sieve': place_type_sieve,
          'Row_Available_Sieve': row_available_sieve,
          'Success_Metric_Sieve': success_metric_sieve,
          }


def create_intermediate_table(data_df, treatment_df, segment_id_col, treatment_id_col="Treatment_ID", treatment_name_col = "Treatments"):
    """
    This function generates a skeleton for intermediate table by combining each treatment ID with each object ID.
    :param - data_df - a dataframe which has segments with different modal priorities
    :param - treatment_df - a separate csv file which has individual treatments and associated applicable modal priority, 
    modal priority ranking range, and MOEs.
    :return -  treatment_intermediate - intermediate output file where for each segment, 26 treatments are stored and a boolean sieve
    checks out if the treatment is applicable contingent upon each criterion.
    """
    ## First generate the Cartesian product (all possible combinations) of segments and treatments
    # There are probably more compact ways to do this (see https://gist.github.com/internaut/5a653317688b14fd0fc67214c1352831 )
    # but this works just fine and controls which columns are preserved in the intermediate table
    treatment_intermediate = {segment_id_col: [], treatment_id_col: [], treatment_name_col : []}
    for oid in data_df[segment_id_col]:
        for i in range(len(treatment_df)):
            treatment_intermediate[segment_id_col].append(oid)
            treatment_intermediate[treatment_id_col].append(treatment_df.loc[i,treatment_id_col])
            treatment_intermediate[treatment_name_col].append(treatment_df.loc[i,treatment_name_col])

    treatment_intermediate = pd.DataFrame(treatment_intermediate)

    # Now we gradually construct a list of columns to select from data_df
    # Start with the segment ID field
    cols_to_select = [segment_id_col]

    # We will make reasonable assumptions about what missing columns mean:
    # Missing modal priorities become "None"
    modal_priorities = ['Modal_Priority_{}'.format(i) for i in range(1, 8)]
    for field in modal_priorities:
        if field not in data_df.columns:
            data_df[field] = 'None'
    cols_to_select.extend(modal_priorities)

    # There aren't any reasonable assumptions to make about what some missing columns mean
    cols_to_select.extend(["Place_Type", "Available_ROW", "Land_Use"])

    # Missing success metrics become 0 (success metric not met)
    success_metrics = ['{}_Success_Metric'.format(mode) for mode in ['Bicycle', 'Pedestrian', 'Transit', 'Commerce',
                                                                     'Storage', 'Automobility', 'Ridehail']]
    for field in success_metrics:
        if field not in data_df.columns:
            data_df[field] = 0
    cols_to_select.extend(success_metrics)

    data_sliced = data_df[cols_to_select]
    treatment_intermediate = pd.merge(treatment_intermediate, data_sliced, on=segment_id_col, how="left")
    treatment_sliced = treatment_df[
        [treatment_id_col, "Mode Priority Applicable", "Appropriate Priority Rank Range", "Place Types",
         "Appropriate Land Use", "Total ROW Necessary Per Direction"]]
    treatment_intermediate = pd.merge(treatment_intermediate, treatment_sliced, on=treatment_id_col, how="left")
    
    ## Apply All Sieve Functions
    # Each sieve considers, row-wise, certain columns in treatment_intermediate (i.e. 
    # certain fields that came from centerline_feature_class and treatment_table_file) 
    # and returns 0 or 1 depending on whether the sieve's criterion is satisfied, i.e.
    # whether the row's treatment is applicable for the row's centerline segment.
    for sieve_name, sieve in sieves.items():
        treatment_intermediate[sieve_name] = treatment_intermediate.apply(sieve, axis=1)

    # Identify Fields to Drop & Clean Up
    treatment_intermediate = treatment_intermediate.drop(modal_priorities + 
        ["Mode Priority Applicable", "Appropriate Priority Rank Range", "Place Types",
         "Appropriate Land Use", "Total ROW Necessary Per Direction"], axis=1)
    # Sum All Boolean Sieve Fields, Filter if Sum < number of criteria
    sieve_columns = sieves.keys()
    treatment_intermediate["Sum_Sieve"] = treatment_intermediate[sieve_columns].sum(axis=1)
    treatment_intermediate["Treatment_Applied"] = np.where(len(sieves) == treatment_intermediate["Sum_Sieve"], 1, 0)
    return treatment_intermediate


def identify_curbside_treatments_by_segment(treatment_df, treatment_intermediate, segment_id_col,
                                            treatment_id_col="Treatment_ID", treatment_name_col = "Treatments"):
    """
    This function takes the treatment csv file and the intermediate output file to create final output where for each segment all 
    the possible treatment IDs are stored and associated MOEs are saved as well. 
    :param - treatment_df -  a separate csv file which has individual treatments and associated applicable modal priority, 
    modal priority ranking range, and MOEs.
    :param - treatment_intermediate - intermediate output file where for each segment, 26 treatments are stored and a boolean sieve
    checks out if the treatment is applicable contingent upon each criterion. 
    :return - output_df - treatment ids for each segment, possible MOEs for each treatment. 
    """
    treatment_moes = {}
    print("Processing treatment MOEs...")
    for i in range(len(treatment_df)):
        treatment_moes[treatment_df.loc[i, treatment_id_col]] = treatment_df.loc[i, "Measures of Effectiveness"].split(",")
    output_df = {segment_id_col: [], "TreatmentID": [], "Treatments": []}

    for treatment_number in range(1, 27):
        col_moe = "Treatment_{0}_MOEs".format(str(treatment_number))
        output_df[col_moe] = []
    print("Creating identified segments...")
    grouped = treatment_intermediate.groupby(segment_id_col)
    for name, group in grouped:
        applied_df = group[group["Treatment_Applied"] == 1]
        treatments = list(applied_df[treatment_id_col])
        treatment_string = ";".join(map(str, treatments))
        treatment_names = list(applied_df[treatment_name_col])
        treatment_names_string = ";".join(treatment_names)
        output_df[segment_id_col].append(name)
        output_df["TreatmentID"].append(treatment_string)
        output_df["Treatments"].append(treatment_names_string)
        for i in range(1, 27):
            if i in treatments:
                output_df["Treatment_{0}_MOEs".format(str(i))].append(";".join(treatment_moes[i]))
            else:
                output_df["Treatment_{0}_MOEs".format(str(i))].append(np.nan)
    output_df = pd.DataFrame(output_df)
    return output_df


# Main Function
def identify_curbside_treatments_to_feature_class(centerline_feature_class, output_feature_class,
                                                  explode_by_treatment, treatment_table_file,
                                                  output_intermediate_csv):
    """This function acts as the main function for the FHWA Identify Curbside Treatments tool. This tool will take in
    fields identifying modal priorities, built environment contexts, ROW information, and other factors in order to
    identify recommended treatments based on demand information, policy data, and the right-of-way.
    :param centerline_feature_class - Input consolidated centerline file that uses attributes from the previous tools,
    built environment attributes, right-of-way information, and modal priorities that treatment
    fields will be added to in the output feature class.
    :param explode_by_treatment- This boolean value indicates whether treatments are represented as
    individual features copying line geometry for each treatment recommended (true), or associates all treatments
    to a single centerline (false).
    :param output_feature_class - This output feature class is a copy of the centerline file provided but with
    treatments and corresponding MOEs for them associated with the feature class.
    :param treatment_table_file - This CSV file includes all the possible treatments by mode.
    By default one is already created and configured.
    :param - output_intermediate_csv - This optional output CSV will provide an intermediate and inspectable output
    table that identifies exactly why different treatments were identified and selected given its treatment selection
    criteria in the treatment table."""
    data_df = pd.DataFrame().spatial.from_featureclass(centerline_feature_class).reset_index()
    dirname = os.path.dirname(__file__) # Get path from python file
    treatment_table_path = str(treatment_table_file)
    arc_print("Reading in treatment table...", True)
    treatment_df = pd.read_excel(treatment_table_path)
    arc_print("Identifying contextually appropriate treatments...", True)
    oid_col = str(arcpy.Describe(centerline_feature_class).OIDFieldName)
    treatment_intermediate = create_intermediate_table(data_df, treatment_df, oid_col)
    if arcpy.Exists(os.path.dirname(output_intermediate_csv)):
        arc_print("Exporting out intermediate CSV which reviews the logic of treatment selections...", True)
        treatment_intermediate_export = treatment_intermediate.rename(columns={oid_col: "Segment_ID"})
        treatment_intermediate_export.to_csv(output_intermediate_csv, index=False)
    output_df = identify_curbside_treatments_by_segment(treatment_df, treatment_intermediate, oid_col)
    arc_print("Joining output results to primary feature dataframe...")
    if explode_by_treatment:
        applied_treatments = treatment_intermediate[treatment_intermediate["Treatment_Applied"]==1]
        treatment_col_kept = ['OBJECTID','Treatment_ID', 'Treatments']
        data_df_w_output = data_df.merge(applied_treatments[treatment_col_kept], how="outer", on=oid_col)
        arc_print("Exporting output treatments by segment dataframe to output feature class...", True)
    else:
        data_df_w_output = data_df.merge(output_df, how="left", on=oid_col)
        arc_print("Exporting output single centerline dataframe to output feature class...", True)
    data_df_w_output.spatial.to_featureclass(output_feature_class)
    return output_treatment_feature_class, output_intermediate_csv


# This test allows the script to be used from the operating system
# command prompt (stand-alone), in a Python IDE, as a geoprocessing
# script tool, or as a module imported in another script.
if __name__ == '__main__':
    # read inputs and call functions to produce output dataframe.
    centerline_feature_class = arcpy.GetParameterAsText(0)
    output_treatment_feature_class = arcpy.GetParameterAsText(1)
    explode_by_treatment = bool(arcpy.GetParameterAsText(2))
    treatment_table_file = arcpy.GetParameterAsText(3)
    output_intermediate_table = arcpy.GetParameterAsText(4)
    arc_print("Parameters specified...")
    identify_curbside_treatments_to_feature_class(centerline_feature_class, output_treatment_feature_class,
                                                  explode_by_treatment, treatment_table_file, output_intermediate_table)

