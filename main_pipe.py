from get_data import get_data, split_df
from helpers import reformat_dates,  handle_jsons, add_writetime_column
from db_handle import validate_columns, engine
from deal_flow import handle_deal_flow
from config import MY_LOGGER


def handle_deals():
    endpoint = "deals"
    df = reformat_dates(get_data(endpoint, additional_url_params="&sort=update_time DESC"))
    main_fields_df, special_fields_df = split_df(df)
    main_fields_df = handle_jsons(main_fields_df)

    write_to_db(endpoint, main_fields_df)
    write_to_db(f"{endpoint}_special_fields", special_fields_df)
    add_writetime_column(main_fields_df)
    add_writetime_column(special_fields_df)


def write_to_db(endpoint, df, if_exist='replace'):
    add_writetime_column(df)
    validate_columns(endpoint, df)
    df.to_sql(name=endpoint, if_exists=if_exist, con=engine, index=False)
    return True


def handle_endpoint(endpoint):
    df = reformat_dates(get_data(endpoint))
    df = handle_jsons(df)
    add_writetime_column(df)
    write_to_db(endpoint, df)
    return True


def handle_flow():
    flow_data = handle_deal_flow()
    # No new deal changes
    if flow_data is None:
        return
    if len(flow_data) > 0:
        write_to_db("flow", flow_data, 'append')


def main():
    handle_deals()
    handle_endpoint("dealFields")
    handle_endpoint("users")
    handle_endpoint("stages")
    handle_endpoint("pipelines")
    handle_flow()
    MY_LOGGER.info("Successfully finished data update.")


if __name__ == "__main__":
    main()
