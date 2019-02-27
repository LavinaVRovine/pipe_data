import pandas as pd
from get_data import get_data, split_df
from helpers import reformat_dates,  handle_jsons, add_writetime_column, extract_id
from db_handle import validate_columns, engine
from deal_flow import handle_deal_flow
from config import MY_LOGGER
from get_activities import get_new_activities


def add_deal_ids(df):
    for column in ["creator_user_id", "org_id", "person_id", "user_id"]:
        extracted = extract_id(df, column)
        if extracted is None:
            pass
        else:
            df.loc[:, column+"_id"] = extracted
    return df


def get_formatted_deals(additional_url_params):

    main_columns = [
        'active',
        'activities_count', 'add_time',
        'cc_email', 'close_time',
        'creator_user_id', 'currency', 'deleted', 'done_activities_count',
        'email_messages_count',
        'expected_close_date',
        'files_count',
        'first_won_time', 'followers_count', 'formatted_value',
        'formatted_weighted_value', 'id', 'last_activity_date',
        'last_activity_id', 'last_incoming_mail_time',
        'last_outgoing_mail_time', 'lost_reason', 'lost_time',
        'next_activity_date', 'next_activity_duration', 'next_activity_id',
        'next_activity_note', 'next_activity_subject', 'next_activity_time',
        'next_activity_type', 'notes_count', 'org_hidden', 'org_id', 'org_name',
        'owner_name', 'participants_count', 'person_hidden', 'person_id',
        'person_name', 'pipeline_id', 'probability', 'products_count',
        'reference_activities_count', 'rotten_time', 'stage_change_time',
        'stage_id', 'stage_order_nr', 'status', 'title',
        'undone_activities_count', 'update_time', 'user_id', 'value',
        'visible_to', 'weighted_value', 'weighted_value_currency', 'won_time']

    endpoint = "deals"
    df = reformat_dates(get_data(endpoint, additional_url_params=additional_url_params))
    main_fields_df, special_fields_df = split_df(df, main_columns)
    main_fields_df = add_deal_ids(main_fields_df)
    main_fields_df = handle_jsons(main_fields_df)
    return main_fields_df, special_fields_df


def handle_deals():
    endpoint = "deals"
    main_fields_df, special_fields_df = get_formatted_deals(additional_url_params="&sort=update_time DESC")
    deleted_main_fields_df, deleted_special_fields_df =\
        get_formatted_deals(additional_url_params="&sort=update_time DESC&status=deleted")

    write_to_db(endpoint, pd.concat([main_fields_df, deleted_main_fields_df]))
    write_to_db(f"{endpoint}_special_fields", pd.concat([special_fields_df, deleted_special_fields_df]))


def write_to_db(endpoint, df, if_exist='replace'):
    add_writetime_column(df)
    validate_columns(endpoint, df)
    df.to_sql(name=endpoint, if_exists=if_exist, con=engine, index=False)
    return True


def handle_endpoint(endpoint):
    df = reformat_dates(get_data(endpoint))
    df = handle_jsons(df)
    write_to_db(endpoint, df)
    return True


def handle_activities():
    df = get_new_activities()
    df = reformat_dates(df)
    df = handle_jsons(df)
    write_to_db("activities", df, "append")


def handle_organizations():
    endpoint = "organizations"
    main_columns = ['active_flag', 'activities_count', 'add_time', 'address', 'address_admin_area_level_1',
                    'address_admin_area_level_2', 'address_country',
                    'address_formatted_address', 'address_locality', 'address_postal_code',
                    'address_route', 'address_street_number', 'address_sublocality',
                    'address_subpremise', 'category_id', 'cc_email',
                    'closed_deals_count', 'company_id', 'country_code',
                    'done_activities_count',
                    'email_messages_count', 'files_count', 'first_char', 'followers_count',
                    'id', 'label', 'last_activity_date', 'last_activity_id',
                    'lost_deals_count', 'name', 'next_activity_date', 'next_activity_id',
                    'next_activity_time', 'notes_count', 'open_deals_count', 'owner_id',
                    'owner_name', 'people_count', 'picture_id',
                    'reference_activities_count', 'related_closed_deals_count',
                    'related_lost_deals_count', 'related_open_deals_count',
                    'related_won_deals_count', 'undone_activities_count', 'update_time',
                    'visible_to', 'won_deals_count']

    df = reformat_dates(get_data(endpoint, additional_url_params="&filter_id=238"))
    main_fields_df, special_fields_df = split_df(df, main_columns)
    main_fields_df = add_deal_ids(main_fields_df)
    main_fields_df = handle_jsons(main_fields_df)

    write_to_db(endpoint, main_fields_df)
    write_to_db(f"{endpoint}_special_fields", special_fields_df)


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
    handle_organizations()
    handle_activities()
    MY_LOGGER.info("Successfully finished data update.")


if __name__ == "__main__":
    main()
