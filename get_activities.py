from get_data import get_data
from db_handle import engine
import pandas as pd
from db_handle import get_latest_commit
from config import PIPE_DATE_FORMAT
from datetime import datetime

# TODO duplicate in flow...
last_check = get_latest_commit("activities")
if last_check is None:
    last_check_formatted = datetime.strftime(datetime(2000, 1, 1, 16, 0, 0), PIPE_DATE_FORMAT)
else:
    last_check_formatted = datetime.strftime(last_check, PIPE_DATE_FORMAT)


def get_new_activities():
    df = pd.read_sql_query("SELECT DISTINCT id from users", con=engine)
    ids = set(df.loc[:, 'id'])
    output = None
    for user_id in ids:
        if output is None:
            output = get_data("activities",
                              additional_url_params=f"&user_id={user_id}&start_date={last_check_formatted}")
        else:
            output = pd.concat([output,
                                get_data("activities",
                                         additional_url_params=f"&user_id={user_id}&start_date={last_check_formatted}")
                                ])
    return output.reset_index(drop=True)


if __name__ == "__main__":
    dd = get_new_activities()
