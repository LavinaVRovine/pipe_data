import pandas as pd
from ses import Requester
from config import BASE_URL, PIPE_TOKEN, RESPONSE_LIMIT
from helpers import GetNotSucceedException

REQ = Requester()


def get_data(endpoint: str, start: int = 0, data=None, additional_url_params="") -> pd.DataFrame:
    """
    Gets data about all deals in pipe ATM
    :returns pd.Dataframe
    """
    url = f"{BASE_URL}{endpoint}?api_token={PIPE_TOKEN}&start={start}&limit={RESPONSE_LIMIT}" + additional_url_params
    response = REQ.get(url)
    json = response.json()

    if "success" not in json or json["success"] is not True:
        raise GetNotSucceedException

    if data is None:
        data = pd.DataFrame(json['data'])
    else:
        data = data.append(pd.DataFrame(json['data']), ignore_index=True)
    if "additional_data" in json and "pagination" in json["additional_data"]:
        if json["additional_data"]["pagination"]["more_items_in_collection"] is True:
            return get_data(
                endpoint,
                json["additional_data"]["pagination"]["next_start"],
                data,
                additional_url_params=additional_url_params
            )
    return data


def split_df(df: pd.DataFrame, main_columns: list) ->tuple:
    """Splits deal dataframe to two. First containing main deal fields, the other all custom fields
    :returns tuple of DFs, first main, second special fields

    """
    # TODO: hele proc to funguje? nemeo by to byt na copy?
    main_deals_df = df[main_columns]
    df.drop(main_columns, axis=1, inplace=True)
    special_fields = df.unstack().dropna().reset_index().rename(
        {"level_0": "field_id", "level_1": "deal_id", 0: "field_value"}, axis=1)
    special_fields['field_value'] = special_fields['field_value'].astype(str)
    return main_deals_df, special_fields


if __name__ == "__main__":
    # x = get_data("users")
    x = get_data("deals", additional_url_params="&sort=update_time DESC")
