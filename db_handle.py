from sqlalchemy import create_engine, exc
import psycopg2
from config import DATABASE_URI, DB_HOST, PIPE_DATABASE, USER, DB_PASSWORD, MY_LOGGER


def test_engine():
    try:
        eng = create_engine(DATABASE_URI)
        eng.connect()
        return eng
    except exc.OperationalError as oe:
        MY_LOGGER.critical(f"Could not connect to db {oe}", exc_info=True)
        raise
    except Exception as e:
        MY_LOGGER.critical(f"Could not connect to db {e}", exc_info=True)
        raise


engine = test_engine()
dtypes_mappings = {'?': 'bool', 'O': 'text', 'i': 'int', 'M': 'timestamp without time zone',
                   'f': 'float'}


def get_new_columns(table_columns, df_columns):
    return set(df_columns) - set(table_columns)


def validate_columns(table_name, df):
    if engine.dialect.has_table(engine, table_name):  # If table exists

        table_column_names = [col["name"] for col in engine.dialect.get_columns(engine, table_name)]

        new_columns = get_new_columns(table_column_names, list(df.columns))
        conn = psycopg2.connect(host=DB_HOST, database=PIPE_DATABASE, user=USER, password=DB_PASSWORD)
        cur = conn.cursor()
        try:
            for col in new_columns:
                dtype = df[col].dtype.kind
                if dtype not in dtypes_mappings:
                    raise ValueError
                cur.execute(f'ALTER TABLE %s ADD COLUMN %s %s' % (table_name, col, dtypes_mappings[dtype]))
        except psycopg2.ProgrammingError as pr:
            MY_LOGGER.error(f"Invalid type mapping {pr}")
            return
        except Exception as e:
            MY_LOGGER.error(e)
            return
        finally:
            conn.commit()
            return True
    return


def get_latest_commit(table_name):
    if engine.dialect.has_table(engine, table_name):
        conn = psycopg2.connect(host=DB_HOST, database=PIPE_DATABASE, user=USER, password=DB_PASSWORD)
        cur = conn.cursor()
        try:
            cur.execute(f'SELECT max(db_write_time) FROM %s ' % (table_name,))
            max_date = cur.fetchone()[0]
            cur.close()
            return max_date
        except psycopg2.ProgrammingError as pr:
            MY_LOGGER.error(f"{pr}")
            cur.close()
            return
    else:
        return


if __name__ == "__main__":
    get_latest_commit("flow")
