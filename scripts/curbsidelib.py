# --------------------------------
# Name: itecurbsidelib.py
# Purpose: This file serves as a function library for the ITE Curbside Management GIS Tools. Import as csl.
# Current Owner: David Wasserman
# Author: Fehr & Peers
# Last Modified: 11/6/2019
# ArcGIS Version:   ArcGIS Pro/10.6
# Python Version:   3.5/2.7
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
import numpy as np
import os
import datetime
import math
try:
    import pandas as pd
except:
    arcpy.AddError("This library requires Pandas installed in the ArcGIS Python Install."
                   " Might require installing pre-requisite libraries and software.")


# General Function Definitions
def arc_print(string, progressor_bool=False):
    """ This function is used to simplify using arcpy reporting for tool creation,if progressor bool is true it will
    create a tool label.
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

def validate_df_names(dataframe, output_feature_class_workspace):
    """Returns pandas dataframe with all col names renamed to be valid arcgis table names."""
    new_name_list = []
    old_names = dataframe.columns.names
    for name in old_names:
        new_name = arcpy.ValidateFieldName(name, output_feature_class_workspace)
        new_name_list.append(new_name)
    rename_dict = {i: j for i, j in zip(old_names, new_name_list)}
    dataframe.rename(index=str, columns=rename_dict)
    return dataframe


def construct_index_dict(field_names, index_start=0):
    """This function will construct a dictionary used to retrieve indexes for cursors.
    :param - field_names - list of strings (field names) to load as keys into a dictionary
    :param - index_start - an int indicating the beginning index to start from (default 0).
    :return - dictionary in the form of {field:index,...}"""
    dict = {str(field): index for index, field in enumerate(field_names, start=index_start)}
    return dict


def retrieve_row_values(row, field_names, index_dict):
    """This function will take a given list of field names, cursor row, and an index dictionary provide
    a tuple of passed row values.
    :param - row - cursor row
    :param - field_names -list of fields and their order to retrieve
    :param - index_dict - cursors dictionary in the form of {field_name : row_index}
    :return - list of values from cursor"""
    row_values = []
    for field in field_names:
        index = index_dict.get(field, None)
        if index is None:
            print("Field could not be retrieved. Passing None.")
            value = None
        else:
            value = row[index]
        row_values.append(value)
    return row_values

def join_df_columns_to_feature_class(df, target_feature_class, df_join_field,
                                     feature_class_join_field="@OID", join_columns = []):
    """Will join chosen columns  to a feature class from a dataframe using temporary csvs and in_memory tables.
    @:param - df - dataframe with data to join
    @:param - target_feature_class - the feature class to join data to
    @:param - df_join_field - field in dataframe to base join
    @:param - feature_class_join_field - feature class with the field to base the join. If the
    feature class object ID is chosen with the @OID tag, describe objects will find it.
    @:param - join_columns - columns to join to the feature class"""
    if feature_class_join_field == "@OID":
        feature_class_join_field = arcpy.Describe(target_feature_class).OIDFieldName
    temp_folder = arcpy.env.scratchFolder
    temp_csv = os.path.join(temp_folder,"join_csv.csv")
    df.to_csv(temp_csv)
    temp_table = "temp_table"
    arcpy.TableToTable_conversion(temp_csv,"in_memory",temp_table)
    temp_table_path = os.path.join("in_memory", temp_table)
    arcpy.JoinField_management(target_feature_class,feature_class_join_field,temp_table_path,df_join_field,join_columns)
    print("Dataframe Fields Joined to Feature Class.")
    try:
        os.remove(temp_csv)
        os.removedirs(temp_folder)
    except:
        print("Could not complete a full clean up of the scratchFolder.")

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
# Curbside Management Function Definitions

def parse_time(HMS):
    '''Convert HH:MM:SS to seconds since midnight, for comparison purposes.
    @:param - HMS - a text string in the form HH:MM:SS. If one section is missing, it is assumed to be zero. '''
    values = [i for i in HMS.split(':')]
    while len(values) < 3:
        values.append(0)
    H , M , S = values
    seconds = (float(H) * 3600) + (float(M) * 60) + float(S)
    return seconds

def construct_datetime_ranges(time_period_dictionary,days_analyzed, base_date_stamp ="1900-01-01"):
    """This function will create a dictionary of time period/day combinations names with values equal
    to tuples of datetime ranges for data filtering/summary.
    Format would be {time_period_day_names:(from_datetime_stamp,to_datetime_stamp)}.
    Adjusts for day overruns by checking if a to-time in seconds is < a from-time. However, as we only have 7 days of data,
    the edge case we do not address is if an analyst wants to know about regulations sunday, spilling into Monday.
    time_period names are in the form '{0}_{1}'.format(tp_name,day)
    :param - time_period_dictionary - dictionary in the form time_period_name:(start HH:MM,end HH:MM)
    :param - days_analyzed - days to analyze in the form of ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    :param - base_date_stamp - this base date is used to determine which ranges to construct from.
    :returns - time_period_datetimes - the time period dictionary keys that are tuples of day and time period, and values of start
    end datetime tuples. {(period,day):(start_dt,end_dt)}
    """
    arc_print("Constructing time period dictionary to manage times...")
    time_period_dict_seconds = {}
    for tp in time_period_dictionary:
        tp_vals = time_period_dictionary.get(tp,None)
        tp_seconds = [parse_time(i) for i in tp_vals]
        time_period_dict_seconds.update({tp:tp_seconds})
    arc_print("Constructing day-time period ranges...")
    days_text = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    day_of_week_codes = {i:j for i,j in zip(days_text,range(7))}
    new_dt_dict = {}
    for day in days_analyzed:
        base_date = datetime.datetime.strptime(base_date_stamp, "%Y-%m-%d")
        datetime_delta_7_days = datetime.timedelta(days=7)
        future_date = base_date + datetime_delta_7_days
        days = pd.date_range(base_date_stamp,future_date, freq='D',closed="left").to_list()
        days_dict = {i.weekday():i for i in days}
        date_time_day = days_dict.get(day_of_week_codes.get(day),0)
        for period in time_period_dict_seconds:
            start, end = time_period_dict_seconds.get(period)
            start_tdt , end_tdt = datetime.timedelta(seconds=start) ,datetime.timedelta(seconds=end)
            start_dt, end_dt = date_time_day + start_tdt , date_time_day + end_tdt
            new_key = (period,day)
            new_dt_dict[new_key] = (start_dt, end_dt)
    return new_dt_dict
# ROW Function Definitions

class shared_row_data_evaluator(object):
    """
    This class manages methods and assumptions related to the shared-row specification for right of way reporting.
    """

    def __init__(self, shared_row_df):
        """This initializer will an input dataframe to validate what available shared-row fields are available for reporting. 
        :param - self - object
        :param - shared_row_df - pandas dataframe representing the shared-row specification fields.
        :var - sr_df - shared row dataframe - the original from passed data will numeric na values filled with 0
        :var - scenario_dfs  - a dict of dataframes where each key is the name of a scenario and the value is a
        dataframe of shared row spec
        :var - added_columns - a dict of lists where each key is the name of a scenario and the value is added columns
        to it. Method calling it defaults to row and lanes if added to the main.
        :var - LTL_List - names of left through lanes
        :var - RTL_List - names of right through lanes
        :var - left_sidewalk_slices - names of sidewalk slices PRESENT in the current dataframe in typical order
        :var - left_spec_slices - names of fields for other multimodal/curb slice fields
        :var - center_width_field - field denoting field width of the center lane
        :var - right_spec_slices - - names of fields for other multimodal/curb slice fields
        :var - right_sidewalk_slices - names of sidewalk slices PRESENT in the current dataframe in typical order
        :var - row_spec_fields - all row fields and slices for a main street centerline (not including trails)
        :var - row_slices - all PRESENT in the current dataframe
        """
        print("Sanitizing Data....")
        numeric_fields = shared_row_df.select_dtypes(include="number")
        shared_row_df[numeric_fields.columns] = shared_row_df[numeric_fields.columns].fillna(0)
        assert shared_row_df.index.is_unique, print(
            "Error: Classes depends on a dataframe that has unique index values.")
        self.sr_df = shared_row_df
        self.scenario_dfs = {}
        self.added_columns = {"BASELINE_ROW": ["Total_ROW_Width", "Base_Total_ROW_Width", "Unused_ROW_Width",
                                               "Left_Sidewalk_Width", "Right_Sidewalk_Width", "Curb_To_Curb_Width"]}
        self.LTL_List = [f for f in self.sr_df.columns if "Left_Through_Lane" in f]
        self.RTL_List = [f for f in self.sr_df.columns if "Right_Through_Lane" in f]
        self.LTL_List.sort()
        self.RTL_List.sort()
        print("Developing ROW Statistics...")
        self.sr_df["Right_Thru_Lane_Widths"] = self.sr_df[self.RTL_List].sum(axis=1).fillna(0)
        self.sr_df["Left_Thru_Lane_Widths"] = self.sr_df[self.LTL_List].sum(axis=1).fillna(0)
        self.left_sidewalk_slices = [i for i in ["Left_Sidewalk_Frontage_Zone", "Left_Sidewalk_Through_Zone",
                                                 "Left_Sidewalk_Furniture_Zone"] if i in self.sr_df]
        self.left_spec_slices = ["Left_Bike_Lane", "Left_Bike_Buffer", "Left_Parking_Lane", "Left_Transit_Lane"]
        self.center_width_field = ["Center_Lane"]
        self.right_spec_slices = ["Right_Transit_Lane", "Right_Parking_Lane", "Right_Bike_Buffer", "Right_Bike_Lane", ]
        self.right_sidewalk_slices = [i for i in ["Right_Sidewalk_Furniture_Zone", "Right_Sidewalk_Through_Zone",
                                                  "Right_Sidewalk_Frontage_Zone"] if i in self.sr_df]
        self.spec_row_fields = self.left_sidewalk_slices + self.left_spec_slices + self.LTL_List + \
                               self.center_width_field + self.RTL_List + self.right_spec_slices \
                               + self.right_sidewalk_slices
        self.row_slices = [i for i in self.spec_row_fields if i in self.sr_df.columns]
        self.sr_df["Total_ROW_Width"] = self.sr_df[self.row_slices].sum(axis=1)
        self.sr_df["Base_Total_ROW_Width"] = self.sr_df["Total_ROW_Width"]
        self.sr_df["Unused_ROW_Width"] = 0
        self.sr_df["Left_Sidewalk_Width"] = self.sr_df[self.left_sidewalk_slices].sum(axis=1)
        self.sr_df["Right_Sidewalk_Width"] = self.sr_df[self.right_sidewalk_slices].sum(axis=1)
        self.sr_df["Curb_To_Curb_Width"] = self.sr_df["Total_ROW_Width"] - (self.sr_df["Left_Sidewalk_Width"] +
                                                                            self.sr_df["Right_Sidewalk_Width"])

    def __str__(self):
        return "shared_row_data_evaluator"

    def get_row_df(self, scenario=None):
        """Returns the original shared row dataframe. If a scenario is passed, that dataframe is returned
        @:param - scenario - name of scenario to retrieve from dictionary. If it does not exist, the main row df is
        passed."""
        return self.scenario_dfs.get(scenario, self.sr_df)

    def clear_sceanarios(self):
        """Calling this function will clear all scenario dictionaries from the class."""
        self.scenario_dfs = {}
        return None

    def get_added_columns(self, scenario=None):
        """Get added columns present in the current scenario.
        :param scenario - name of the current scenario"""
        if scenario in self.scenario_dfs:
            added_columns = self.added_columns.get(scenario)
        else:
            added_columns = self.added_columns.get("BASELINE_ROW", [])
        return added_columns

    def right_size_row(self, scenario_key):
        """Create a scenario or modify an existing scenario by setting all lane widths to a max of 10 feet for
        through lanes (11 feet for the curbside lane), and parking lanes to a max of 8 feet.
        :param scenario_key - name of scenario as a string or other hashable object"""
        scenario_df = self.scenario_dfs.setdefault(scenario_key, self.sr_df.copy())

        curb_side_lanes = [i for i in self.RTL_List + self.LTL_List if "1" in i]
        other_thru_lanes = [i for i in self.RTL_List + self.LTL_List if "1" not in i]
        parking_lanes = [i for i in self.row_slices if "Parking" in i and "Meta" not in i]
        for oth_thru_lane in curb_side_lanes:
            curb_side_lane_max = 11 * 0.3048 # fixed conversion from feet to meters!
            scenario_df[oth_thru_lane] = np.where(scenario_df[oth_thru_lane] > curb_side_lane_max, curb_side_lane_max,
                                                  scenario_df[oth_thru_lane])
        for oth_thru_lane in other_thru_lanes:
            lane_max = 10 * 0.3048 # fixed conversion from feet to meters!
            scenario_df[oth_thru_lane] = np.where(scenario_df[oth_thru_lane] > lane_max, lane_max,
                                                  scenario_df[oth_thru_lane])
        for parking_lane in parking_lanes:
            park_max = 8 * 0.3048 # fixed conversion from feet to meters!
            scenario_df[parking_lane] = np.where(scenario_df[parking_lane] > park_max, park_max,
                                                 scenario_df[parking_lane])
        self.update_row(scenario_key)
        return scenario_df

    def apply_road_diet(self, scenario_key, lane_removal_count = 2,lock_last_lane = True, add_twltl=True):
        """This function will remove thru lanes closest to the centerline, and if add_twltl is true and no
        center_lane width wider than 10 ft already exists it will add a twltl to the street with one of the removed
        lanes widths.
        @scenario_key - the new or existing key for a scenario to modify or create from the base
        @lock_last-lane - if this is true, no road diet can remove the last lane left for a street
        @lane_removal_count = number of centerline near lanes to remove, if odd, starts with right lanes
        @add_twltl = indicates whether to add a two way left turn lane if no existing center allocation of significance
        exists.  """
        #TODO implement twltl
        scenario_df = self.scenario_dfs.setdefault(scenario_key, self.sr_df.copy())
        right_temp_tracker = "Right_Most"
        left_temp_tracker = "Left_Most"
        scenario_df[right_temp_tracker] = 0
        scenario_df[left_temp_tracker] = 0
        def remove_centermost_lane(row, lock_last_lane = lock_last_lane):
            """This worker function will remove the left most lane (towards center) assuming a list of lane
             values are passed in 1,2,3,4 order. """
            if lock_last_lane:
                lane_check = 0
            else:
                lane_check = -1
            for idx, value in enumerate(row):
                if value <= 0 and (idx-1) != lane_check:
                    row[idx-1] = 0
                elif idx == len(row)+1:
                    row[idx] = 0
                else:
                    pass
            return row
        right_lanes_to_remove = int(round(lane_removal_count/2.0,0))
        left_lanes_to_remove = int(lane_removal_count/2.0)
        for i in range(right_lanes_to_remove):
            scenario_df[self.RTL_List] = scenario_df[self.RTL_List].apply(remove_centermost_lane,axis=1)
        for i in range(left_lanes_to_remove):
            scenario_df[self.LTL_List] = scenario_df[self.LTL_List].apply(remove_centermost_lane,axis=1)
        self.update_row(scenario_key)
        return scenario_df

    def remove_parking_lanes(self, scenario_key, right_lane_removed=True, left_lane_removed=True):
        """Removes the parking lanes and updates row calcs for the chosen scenario after doing so.
        @:param - scenario_key - the new or existing key for a scenario to modify or create from the base
        @:param - right_lane_removed - is the right parking lane removed for the scenario
        @:param - left_lane_removed - is the left parking lane removed for the scenario"""
        scenario_df = self.scenario_dfs.setdefault(scenario_key, self.sr_df.copy())
        parking_lanes = [i for i in self.row_slices if "Parking_Lane" in i and "Meta" not in i]
        right_parking_lane = [i for i in parking_lanes if "Right" in i]
        left_parking_lane = [i for i in parking_lanes if "Left" in i]
        if right_parking_lane and right_lane_removed:
            parking_field = right_parking_lane[0]
            scenario_df[parking_field] = 0
        if left_parking_lane and left_lane_removed:
            parking_field = left_parking_lane[0]
            scenario_df[parking_field] = 0
        self.update_row(scenario_key)
        return scenario_df

    def add_lane_statistics_to_df(self, scenario=None, right_lane_name="Right_Lanes", left_lane_name="Left_Lanes",
                        total_lanes_name="Total_Lanes"):
        """Add lane counts (right,left, total) to the main dataframe. If a scenario is chosen, then lanes are added
        to that scenario
        @:param - right_lane_name - name of output lane field
        @:param - left_lane_name - name of output lane field
        @:param - total_lane_name - name of output lane field"""
        right_lanes_df, left_lanes_df = None, None
        if scenario in self.scenario_dfs:
            main_df = self.scenario_dfs.get(scenario)
            added_columns = self.added_columns.get(scenario, [])
            added_columns.extend([right_lane_name, left_lane_name, total_lanes_name])
        else:
            main_df = self.sr_df
            added_columns = self.added_columns.get("BASELINE_ROW", [])
            added_columns.extend([right_lane_name, left_lane_name, total_lanes_name])
        print("Identifying lanes...")
        for l_lane, r_lane in zip(self.LTL_List, self.RTL_List):
            right_binary_column = pd.Series(np.where(main_df[r_lane] > 0, 1, 0), name=r_lane)
            left_binary_column = pd.Series(np.where(main_df[l_lane] > 0, 1, 0), name=l_lane)
            if right_lanes_df is None or left_lanes_df is None:
                right_lanes_df = right_binary_column
                left_lanes_df = left_binary_column
            else:
                right_lanes_df = pd.concat([right_lanes_df, right_binary_column], axis=1)
                left_lanes_df = pd.concat([left_lanes_df, left_binary_column], axis=1)
        main_df[right_lane_name] = right_lanes_df.sum(axis=1).fillna(0)
        main_df[left_lane_name] = left_lanes_df.sum(axis=1).fillna(0)
        main_df[total_lanes_name] = main_df[left_lane_name] + main_df[right_lane_name]
        del right_lanes_df, left_lanes_df

    def update_row(self, scenario):
        """Update the ROW Calculations for a chosen scenario. Used anytime an editor function is called.
        :param - scenario in scenarion_dataframes dict"""
        if scenario in self.scenario_dfs:
            scen_df = self.scenario_dfs.get(scenario)
            self.added_columns.setdefault(scenario, self.added_columns.get("BASELINE_ROW"))
        else:
            scen_df = self.sr_df
        scen_df["Total_ROW_Width"] = scen_df[self.row_slices].sum(axis=1)
        scen_df["Unused_ROW_Width"] = scen_df["Base_Total_ROW_Width"] - scen_df["Total_ROW_Width"]
        scen_df["Left_Sidewalk_Width"] = scen_df[self.left_sidewalk_slices].sum(axis=1)
        scen_df["Right_Sidewalk_Width"] = scen_df[self.right_sidewalk_slices].sum(axis=1)
        scen_df["Curb_To_Curb_Width"] = (scen_df["Total_ROW_Width"] + scen_df["Unused_ROW_Width"]) - \
                                        (scen_df["Left_Sidewalk_Width"] + scen_df["Right_Sidewalk_Width"])

    def retrieve_columns_from_scenarios(self, columns=[]):
        """This function will add columns to the main dataframe that are retrieved from all the scenarios. All
        scenario keys will be cast as strings and have spaces replaced with underscores, and made into a prefix for
        the selected columns. Those columns will be concatenated column wise to the main df and returned.
        :param - columns - fields to extract from each scenario df and to add prefixes to the main df for"""
        temp_dataframe = None
        for scenario in self.scenario_dfs:
            try:
                scenario_df = self.scenario_dfs.get(scenario)
                new_columns = ["{0}_{1}".format(str(scenario).replace(" ", "_"), i) for i in columns]
                extracted_columns = scenario_df[columns]
                extracted_columns.columns = new_columns
                if extracted_columns is not None and temp_dataframe is None:
                    temp_dataframe = extracted_columns
                    temp_dataframe.columns = new_columns
                else:
                    temp_dataframe = pd.concat([temp_dataframe, extracted_columns], axis=1)
            except:
                print("Could not process columns for scenario {0}.".format(scenario))
        return temp_dataframe

def calculate_line_bearing(in_fc, out_field='bearing', azimuth=False):
    '''Given a line feature class, calculates the bearing of the Euclidean 
    line between the first and last points of each feature, and writes 
    this value to the specified field in the feature class's attribute 
    table, creating a new field where necessary. Returns a dict of 
    OID:bearing key:value pairs.
    :param in_fc: path to input line feature class
    :param out_field: name of field to contain bearing data (default 'bearing')
    :param azimuth: whether to convert bearings from trigonometric angles to 
        azimuths (default False)'''
    # Instantiate a dict we can return
    out_dict = {}

    # Create necessary new fields
    add_new_field(in_fc, out_field, 'DOUBLE')

    # Traverse features via UpdateCursor
    with arcpy.da.UpdateCursor(in_fc, ['OID@', 'SHAPE@', out_field]) as cursor:
        for row in cursor:
            # Extract first and last points
            first_point = row[1].firstPoint
            last_point = row[1].lastPoint
            
            # Skip features whose first point and last point are identical
            # (we cannot calculate bearing or sinuosity in this case)
            if first_point.equals(last_point):
                continue

            # Calculate bearing via arctangent
            dx = last_point.X - first_point.X
            dy = last_point.Y - first_point.Y
            rads = math.atan2(dy, dx)
            angle = math.degrees(rads)
            if azimuth:
                angle = convert_to_azimuth(angle)

            # Add to dict
            out_dict[row[0]] = angle

            # Update this feature
            row[2] = angle
            cursor.updateRow(row)
        arc_print("Updated line feature class bearing field.")
    
    # Return populated dict of OID:bearing pairs
    return out_dict

def calculate_line_sinuosity(in_fc, out_field='sinuosity'):
    '''Given a line feature class, calculates the sinuosity of each feature
    (defined as the ratio of the length of the feature to the length of the
    Euclidean line between the first and last points of the feature), and 
    writes this value to the specified field in the feature class's 
    attribute table, creating a new field where necessary. Returns a dict 
    of OID:sinuosity key:value pairs.
    :param in_fc: path to input line feature class
    :param out_field: name of field to contain sinuosity data (default 'sinuosity')'''
    # Instantiate a dict we can return
    out_dict = {}

    # Create necessary new fields
    add_new_field(in_fc, out_field, 'DOUBLE')

    # Traverse features via UpdateCursor
    with arcpy.da.UpdateCursor(in_fc, ['OID@', 'SHAPE@', out_field]) as cursor:
        for row in cursor:
            # Extract first and last points
            first_point = row[1].firstPoint
            last_point = row[1].lastPoint
            
            # Skip features whose first point and last point are identical
            # (we cannot calculate bearing or sinuosity in this case)
            if first_point.equals(last_point):
                continue

            # Calculate sinuosity via Pythagorean theorem
            dx = last_point.X - first_point.X
            dy = last_point.Y - first_point.Y
            direct_distance = (dx**2 + dy**2)**0.5
            sinuosity = row[1].length / direct_distance

            # Add to dict
            out_dict[row[0]] = sinuosity

            # Update this feature
            row[2] = sinuosity
            cursor.updateRow(row)
        arc_print("Updated line feature class sinuosity field.")
    
    # Return populated dict of OID:sinuosity pairs
    return out_dict

def find_smallest_angle(angle1, angle2):
    '''Given two angles/azimuths (in degrees), returns the smallest (positive)
    angle between them (in degrees).'''
    diff = (angle1 - angle2) % 180
    diff = min(diff, 180 - diff)
    return diff

def convert_to_azimuth(angle):
    '''Converts a typical trigonometric angle (in degrees, 0 degrees pointing
    rightward along the x-axis, 90 degrees pointing upward along the y-axis) 
    to an azimuth ( [0, 360) degrees, 0 degrees pointing upward along the 
    y-axis, 90 degrees pointing rightward along the x-axis).'''
    azimuth = (90 - angle) % 360
    return azimuth

# End do_analysis function

# This test allows the script to be used from the operating
# system command prompt (stand-alone), in a Python IDE,
# as a geoprocessing script tool, or as a module imported in
# another script
if __name__ == '__main__':
    # Define input parameters
    print("Function library: itecurbsidelib.py")
