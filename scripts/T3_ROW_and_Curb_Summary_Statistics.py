# Name: T3_ROW_and_Curb_Summary_Statistics
# Purpose: This tool will take in the input ROW centerlines, curbside feature classes, and demand & policy tables to
# develop summary statistics that relate to the availability ROW for improvements, curbside allocations by time period,
# and other summary statistics for infographics to inform curbside management.
# Author: Fehr & Peers
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
# Import Modules
import pandas as pd
import arcpy, os
import numpy as np
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


def conduct_row_and_curbside_statistic_analysis(centerline_features,corridor_id, curbside_features,
                                                output_summary_feature_class,
                                                days_of_week_analyzed):
    """This tool will take a series of corridor_IDs associated with a centerline feature class and its corresponding
    curbside regulations and report out ROW Availability Summaries for reallocation and how curbsides are allocated
    along corridors.
    :param - center_features - centerline feature class with corridor IDs and shared row spec compliant fields
    :param - corridor_id - a set of IDs that correspond to a corridor or even area summary for centerline features
    :param - curbside_features - curbside feature class
    :param - output_summary_feature_class - the output copy of the center line features with summaries of ROW availability
    and curbside features allocations by corridor_ID
    :param - days_of_week_analyzed - which days of week are focused on by the analysis tool
    :return output_summary_feature_class path"""
    # Validate Days Selected and Time Stamps
    days_of_week_possible = set(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"])
    day_dict = {'Monday': 'mo', 'Tuesday': 'tu', 'Wednesday': 'we', 'Thursday': 'th', 'Friday': 'fr', 'Saturday': 'sa',
                'Sunday': 'su'}
    days_of_week_analyzed_set = set(days_of_week_analyzed)
    if days_of_week_analyzed_set.issubset(days_of_week_possible):
        arc_print("Days selected are valid options...")
    else:
        arcpy.AddWarning("The Days of {0} selected are not valid options...".format(days_of_week_analyzed))
    # ## Review Data
    arcpy.env.overwriteOutput = True
    arcpy.CopyFeatures_management(centerline_features, output_summary_feature_class)
    oid_field_for_df = arcpy.Describe(output_summary_feature_class).OIDFieldName
    arc_print("Output Feature Class Copied...")
    feet_of_curb = "Linear_Feet"
    arcpy.AddField_management(curbside_features, feet_of_curb, "DOUBLE")
    arcpy.CalculateField_management(curbside_features, feet_of_curb, expression="!shape.length@FEET!",
                                    expression_type="PYTHON")
    arc_print("Converting Tables to Dataframes...")
    row_df = arcgis_table_to_df(output_summary_feature_class)
    curb_df = arcgis_table_to_df(curbside_features)
    # ## ROW Statistics & Scenarios
    # This section is dedicated to calling a ROW Manager class and determining how much ROW could be made available
    # under the assumptions related to a road diet, removal of parking lanes, and the right sizing of through
    # and parking lanes.
    try:
        arc_print(
            "Evaluating ROW availability for reallocation under the assumptions that a) Parking is removed b) ROW is "
            "'right-sized' with parking lanes set to 8 feet wide, curbside lanes 11 feet, and other through lanes 10 feet "
            "or c) application of a road diet or lane removal (does not impact two lane streets).")
        row_manager = shared_row_data_evaluator(row_df)
        row_df = row_manager.get_row_df()
        updated_df = row_manager.right_size_row("Right_Sized")
        parking_removed_df = row_manager.remove_parking_lanes("No Parking")
        road_diet_df = row_manager.apply_road_diet("Road Diet")
        summarized_row = row_manager.retrieve_columns_from_scenarios(["Total_ROW_Width", "Unused_ROW_Width"])
        summarized_row_join_fields = [i for i in summarized_row.columns]
        summarized_row = summarized_row.reset_index()
        join_df_columns_to_feature_class(summarized_row, output_summary_feature_class, oid_field_for_df,
                                         join_columns=summarized_row_join_fields)
    except:
        arcpy.AddWarning("The ROW Summaries could not be completed for this tool run. Check to make sure you have"
                         "shared-row compliant field names and types, and that the attributes are appropriately "
                         "filled out.")

    # ## Curbside Data Summaries
    # This section is dedicated to translating input curbside summary parameters and summarizing them by
    # selected corridor IDs. Time stamps of time periods of interest and weekday/weekend booleans to determine
    # summaries are used to identify important summaries related to what types of curbside uses are occuring by
    # chosen corridor ID.
    # try: # taking out try/except statements for debugging
    arc_print("Developing curbside regulation summaries by corridor ID...")
    unique_corridors = curb_df[corridor_id].unique()
    new_field_names = [corridor_id, 'Curb_Alloc_1_Activity', 'Curb_Alloc_1_Reason', 'Curb_Alloc_1_Max_Stay',
                        'Curb_Alloc_1_Paid',
                        'Curb_Alloc_1_Days_Of_Week', 'Curb_Alloc_1_Days_Active', 'Curb_Alloc_1_Begin_Time_Period',
                        'Curb_Alloc_1_End_Time_Period', 'Curb_Alloc_1_Hours_Span', "Curb_Alloc_1_Weekly_Hours",
                        'Curb_Alloc_1_Linear_Feet', 'Curb_Alloc_1_Linear_Feet_Hours']
    final_corridor_df = None
    counter = 0
    arc_print("Processing and combining corridor summaries...", True)
    for corridor in unique_corridors:
        counter += 1
        if counter % 500 == 0:
            arc_print("{0} corridors processed...".format(counter))
        corridor_df = curb_df[curb_df[corridor_id] == corridor]
        # if the curb data have been duplicated for multiple days by T1, filter down to primary features only
        # manually collected curb datasets will not have a 'primary' column and thus we can skip this step
        if 'primary' in corridor_df.columns:
            corridor_df = corridor_df[corridor_df["primary"] == 1].copy()

        # construct a regex pattern for use with Series.str.count() to count
        # how many of the requested days of week each regulation is active on
        days_analyzed_query = "|".join(day_dict.get(i) for i in days_of_week_analyzed)
        corridor_df["Days_Active"] = (corridor_df["daysOfWeek"].str.count(days_analyzed_query))
        corridor_df = corridor_df[corridor_df["Days_Active"]>0].copy()
        
        # manually collected curb datasets will not have the _dt columns so we must create them here
        if 'timesOfDay_from_dt' not in corridor_df.columns:
            corridor_df['timesOfDay_from_dt'] = pd.to_datetime(corridor_df['timesOfDay_from'])
            corridor_df['timesOfDay_to_dt'] = pd.to_datetime(corridor_df['timesOfDay_to'])
        corridor_df["Hours_Span"] = (corridor_df["timesOfDay_to_dt"] - corridor_df["timesOfDay_from_dt"]).dt.total_seconds() / 3600
        corridor_df["Weekly_Hours"] = corridor_df["Hours_Span"] * corridor_df["Days_Active"]
        corridor_df["Linear_Feet_Hours"] = corridor_df["Weekly_Hours"] * corridor_df["Linear_Feet"]
        desired_column_order = [corridor_id, 'activity', 'priorityCategory', 'maxStay', 'payment', 'daysOfWeek', 'Days_Active',
                                'timesOfDay_from', 'timesOfDay_to', 'Hours_Span', 'Weekly_Hours', 'Linear_Feet', 'Linear_Feet_Hours']
        stats = {"Linear_Feet": "sum", "Linear_Feet_Hours": "sum"}
        try:
            corridor_summary = corridor_df.fillna(0).groupby(
                [corridor_id, "activity", "priorityCategory", "maxStay", "payment", "daysOfWeek", "Days_Active", "timesOfDay_from",
                    "timesOfDay_to", "Hours_Span", "Weekly_Hours"]).agg(stats)
        except KeyError:
            arcpy.AddWarning("""Curb features could not be summarized by corridor. Check a few things:
                                1. Make sure the corridor ID field names match between the centerline feature class and the curbside routes created by tool.
                                2. Make sure that the curbside features have the appropriate fields derived from tool 1.
                                3. Check the temporal extent of the data. There maybe be no days selected with the features under study.""")
            return None
            
        corridor_summary = corridor_summary.reset_index()[desired_column_order]
        corridor_summary = corridor_summary.sort_values(by='Linear_Feet_Hours', ascending=False)
        corridor_summary["Summary_ID"] = np.arange(len(corridor_summary)) + 1
        row_numbers = corridor_summary["Summary_ID"].unique()
        corridor_row = None
        for rid in row_numbers:
            try:
                row_df = corridor_summary[corridor_summary["Summary_ID"] == rid].copy()
                final_field_names = [str(i).replace("_1_", "_{0}_".format(str(rid))) for i in new_field_names]
                row_df[final_field_names] = row_df[desired_column_order]
                row_df = row_df.drop(columns=desired_column_order[1:] + ["Summary_ID"])  # Drop all but corridor ID
                if corridor_row is None:
                    row_df.reset_index(drop=True, inplace=True)
                    corridor_row = row_df
                else:
                    row_df = row_df.drop(columns=[corridor_id])
                    row_df.reset_index(drop=True, inplace=True)
                    corridor_row = pd.concat([corridor_row, row_df], axis=1, sort=True)
            except:
                arcpy.AddWarning("Could not identify corridor at corridor id {0}.".format(corridor))
        if final_corridor_df is None:
            final_corridor_df = corridor_row
        else:
            final_corridor_df = pd.concat([final_corridor_df, corridor_row], axis=0, sort=False)
    if final_corridor_df is None:
        arcpy.AddWarning("No curbside features of the appropriate day or extent could be processed.")
    else:
        join_fields = [i for i in final_corridor_df.columns if i != corridor_id]
        join_df_columns_to_feature_class(final_corridor_df, output_summary_feature_class, corridor_id,
                                         feature_class_join_field=corridor_id, join_columns=join_fields)
    # except:
    #     arcpy.AddWarning("""Curbside Features could not be summarized by corridor. Check a few things:
    #                       1. Make sure the corridor ID field names match between the centerline feature class and the curbside routes created by tool.
    #                       2. Make sure that the curbside features have the appropriate fields derived from tool 1.
    #                       3. Check the temporal extent of the data. There maybe be no days selected with the features
    #                       understudy.""")
    arc_print("Summary Statistics Analysis Complete...")

# This test allows the script to be used from the operating
# system command prompt (stand-alone), in a Python IDE,
# as a geoprocessing script tool, or as a module imported in
# another script
if __name__ == '__main__':
    # Define Inputs
    centerline_features = arcpy.GetParameterAsText(0)
    corridor_id = arcpy.GetParameterAsText(1)
    curbside_features = arcpy.GetParameterAsText(2)
    output_summary_feature_class = arcpy.GetParameterAsText(3)  # os.path.join(centerline_fds,
    # "la_osm_consolidated_with_summary_statistics")
    days_of_week_analyzed = arcpy.GetParameterAsText(4).split(";")  # ["Monday", "Saturday"]
    conduct_row_and_curbside_statistic_analysis(centerline_features,corridor_id,curbside_features,
                                                output_summary_feature_class,days_of_week_analyzed)

