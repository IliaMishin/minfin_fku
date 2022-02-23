import re

from copy import copy

from abc import abstractmethod

from datetime import date, timedelta

import pandas as pd

class NoReportsFound(Exception):
    pass

def _parse_date(report_number):
    rep_re = re.compile(r"^\w+?\.\d{2}\-\d{2}\-(20\d{2})"
            r"\.(\d{2})(?:\/(\w+)(?:\-(\d+)|)|)$")
    match = rep_re.match(report_number)
    assert match
    year, month = match.group(1,2)
    repn, reprev = match.group(3,4)
    if reprev:
        reprev = int(reprev)
    return int(year), int(month), reprev

def _make_date(row):
    year, month, reprev = _parse_date(row["report_number"])
    return row, year, month, reprev

class Base:
    def __init__(self, fp_id, db):
        self.fp_id = fp_id
        self.db = db

    def _get_results(self, column_names, query, params):
        cursor = self.db.connection.cursor()
        cursor.execute(query, params)
        cols = column_names
        res =[{k : v for k,v in zip(cols, vals)} for vals in cursor.fetchall()]
        return res

class FPMetric(Base):

    def __init__(self, fp_id, db, extra_params=None):
        super().__init__(fp_id, db)
        self.params = {}
        if extra_params:
            self.params = {k:v for k,v in extra_params.items()}
        self.params["fp_id"] = self.fp_id

    def postprocess(self, df):
        """ Used to do additional processing on the dataframe with
        the data returned by the query. Doesn't do anything by default -
        override it in the child class if needed. """
        return df

    @property
    @abstractmethod
    def query(self):
        """ Holds the query string """

    @property
    @abstractmethod
    def columns(self):
        """ Holds an iterable of column names, in
        the order they appear in the query's select """

    def get_data(self):
        data =  self._get_results(self.columns, self.query, self.params)
        df = pd.DataFrame(data)
        return self.postprocess(df)

    def to_excel(self, file_name, sheet_name=None):
        df = self.get_data()
        df.to_excel(file_name, sheet_name=sheet_name, index=False)


class FPMetric1(FPMetric):
    query = '''
    with type_pairs (result_type_id, cp_type_id,
        result_name, result_type_name, cp_type_name) as (
    select distinct
        MuFpResults.ResultType,
        type_checkpoints.type_check_id,
        MuFpResults.rfp_name,
        type_results.parent_rt_name,
        type_checkpoints.type_check_name
    from MuFpResults
    join type_results on type_results.parent_rt_id = MuFpResults.ResultType
    join MuFpCheckpoints on MuFpCheckpoints.rfp_id = MuFpResults.rfp_id
    join type_checkpoints on type_checkpoints.type_check_id = MuFpCheckpoints.type
    where MuFpResults.fp_id = %(fp_id)s
    )

    select distinct
        type_pairs.result_name,
        summary_table.parent_rt_id as result_type_id,
        type_results.parent_rt_name as result_type_name,
        summary_table.type_check_id as checkpoint_type_id,
        type_checkpoints.type_check_name as checkpoint_type_name
    from 
    (select distinct 
        cp_types.parent_rt_id,
        cp_types.type_check_id,
        type_pairs.cp_type_id
        
    from cp_types
    left join type_pairs on 
        type_pairs.result_type_id = cp_types.parent_rt_id AND
        type_pairs.cp_type_id = cp_types.type_check_id
    where cp_types.parent_rt_id in
        (select distinct type_pairs.result_type_id
            from type_pairs) ) summary_table

    join type_results on type_results.parent_rt_id = summary_table.parent_rt_id 
    join type_checkpoints on type_checkpoints.type_check_id = summary_table.type_check_id
    join type_pairs on type_pairs.result_type_id = summary_table.parent_rt_id
    where summary_table.cp_type_id is NULL
    '''

    columns = ("result_name", "result_type_id", 
            "result_type_name", "checkpoint_type_id", "checkpoint_type_name")

class FPMetric2(FPMetric):
    query = """
    with indicators (rfp_id) as (
        select distinct
            rfp_id
        from fp_result_indicators
        where date > '2020-12-31T00:00:00'
        )
        
    select
        fed_project.fp_name,
        fed_project.fp_code,
        rfp_name,
        count(check_point_name) number_of_checkpoints,
        substr(check_point_end_date, 1, 4) end_year
    from MuFpResults
    join MuFpCheckpoints on MuFpCheckpoints.rfp_id = MuFpResults.rfp_id
    join fed_project on fed_project.fp_id = MuFpResults.fp_id
    where MuFpResults.fp_id = %(fp_id)s and MuFpResults.rfp_id in indicators
    
    group by
        MuFpCheckpoints.rfp_id,
        end_year
    having number_of_checkpoints < 6 
    
    order by
        MuFpCheckpoints.rfp_id,
        end_year
    """

    columns = ("result_name", "num_checkpoints", "start_year",
            "end_year")



