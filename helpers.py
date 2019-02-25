import pandas as pd
from config import PIPE_DATE_FORMAT
import json
from datetime import datetime


class GetNotSucceedException(Exception):
    pass


def reformat_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Formats str:dates in specific format to date. If does not match format, do nothing. Can't use
    pd to_datetime, as it reformats even ints and floats
    :param df: dataframe
    :return: formatted dataframe
    """
    for col in list(df.columns):
        try:
            df[col] = df[col].apply(lambda x: datetime.strptime(x, PIPE_DATE_FORMAT) if x is not None else None)
        except TypeError or ValueError:
            pass
        except:
            pass
    return df


def add_writetime_column(df: pd.DataFrame) -> None:
    """
    adds column with current time inplace
    :rtype: None
    """
    df.loc[:, 'db_write_time'] = datetime.now()
    return


def dict2json(dictionary):
    return json.dumps(dictionary, ensure_ascii=False)


def handle_jsons(df):
    for col in list(df.columns):
        if df[col].apply(lambda x: type(x) is dict or type(x) is list).any():
            # map twice...dict to str, str to str squared
            df.loc[:, col] = df[col].map(dict2json).map(dict2json)
    return df
