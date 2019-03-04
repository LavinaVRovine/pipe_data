import pandas as pd
from datetime import datetime
from ses import Requester
from config import PIPE_DATE_FORMAT
from get_data import get_data
from db_handle import get_latest_commit


REQ = Requester()

last_check = get_latest_commit("flow")
if last_check is None:
    last_check_formatted = datetime.strftime(datetime(2000, 1, 1, 16, 0, 0), PIPE_DATE_FORMAT)
else:
    last_check_formatted = datetime.strftime(last_check, PIPE_DATE_FORMAT)


def get_deal_ids_recently_updated(since):
    """
    Get ids of deals, which have been updated since last changed_since
    :returns set of ids
    """

    df = get_data("recents", additional_url_params=f"&since_timestamp={since}&items=deal")
    if df.empty:
        return
    if 'id' not in df.columns:
        raise KeyError
    return df.id.unique()


def reformat_updates_df(df: pd.DataFrame) -> pd.DataFrame:
    valid_columns = ["item_id", "user_id", "field_key", "old_value", "new_value", "log_time"]
    data_df = df.loc[df['object'] == 'dealChange', 'data'].apply(pd.Series)
    data_df["item_id"] = pd.to_numeric(data_df["item_id"], errors="coerce")
    data_df["user_id"] = pd.to_numeric(data_df["user_id"], errors="coerce")
    data_df["log_time"] = pd.to_datetime(data_df["log_time"], errors="coerce", format=PIPE_DATE_FORMAT)
    if last_check is None:
        return data_df.loc[:, valid_columns]
    return data_df.loc[
        data_df['log_time'] > last_check,
        valid_columns
    ]


def filter_old_mails(df: pd.DataFrame) -> pd.DataFrame:
    df["message_time"] =\
        pd.to_datetime(df["message_time"], errors="coerce", format=PIPE_DATE_FORMAT).dt.tz_localize(None)
    if last_check is None:
        return df
    return df.loc[
        df['message_time'] > last_check,
        :
    ]


def extract_mail_columns(data_series, deal_id):
    relevant_keys = ["user_id", 'mail_thread_id', 'subject', 'message_time', 'add_time',  'company_id']
    series = pd.Series(
        {key: (data_series[key] if key in data_series else None) for key in relevant_keys},
        index=relevant_keys)
    series["deal_id"] = deal_id
    return series


def handle_deal_flow_and_email():
    updated_deals = get_deal_ids_recently_updated(last_check_formatted)
    if updated_deals is None:
        return
    output = None
    mail_output = None
    for deal_id in sorted(updated_deals):
        if output is None:
            output = get_data(f"deals/{deal_id}/flow")
        else:
            output = pd.concat([output, get_data(f"deals/{deal_id}/flow")])

        if mail_output is None:
            mail_output = get_data(f"deals/{deal_id}/mailMessages")
            if "data" not in mail_output:
                continue
            else:
                mail_output = mail_output["data"].apply(lambda row: extract_mail_columns(row, deal_id))
        else:
            data = get_data(f"deals/{deal_id}/mailMessages")
            if "data" not in data:
                continue
            else:
                d = data["data"].apply(lambda row: extract_mail_columns(row, deal_id))
            mail_output =\
                pd.concat([mail_output, d])
    return reformat_updates_df(output).reset_index(drop=True), filter_old_mails(mail_output.reset_index(drop=True))


if __name__ == "__main__":
    x = handle_deal_flow_and_email()
