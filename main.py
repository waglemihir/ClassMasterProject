import sys
import psycopg2
from psycopg2 import OperationalError

#Do I need a config file for this?
host_name = 'localhost'
port = '5432'
dbname = 'postgres'
user_name = 'postgres'
password = 'admin'
csv_full_file_path = 'C:\\Users\\mwagle\\Downloads\\Interview_exercise_Data_Engineering\\Interview_exercise_Data_Engineering\\data.csv'
table_name = 'masterclass_data'


# 393 null user_ids and 24 null course_ids
def process_data():
    try:
        conn = psycopg2.connect(host=host_name, port=port, dbname=dbname, user=user_name, password=password)
    except OperationalError as err:
        # pass exception to function
        print_psycopg2_exception(err)

        # set the connection to 'None' in case of error
        conn = None

    if conn != None:
        cur = conn.cursor()

        try:
            create_table(cur)
            import_data(cur)
            generate_metrics(cur, 'platform', 'number_of_users_per_platform')
            generate_metrics(cur, 'course_id', 'number_of_users_per_courses')
            # does it make sense to group by chapter_id without grouping by course_id as well?
            generate_metrics(cur, 'chapter_id', 'number_of_users_per_chapters')
        except Exception as err:
            # pass exception to function
            print_psycopg2_exception(err)

            # rollback the previous transaction before starting another
            conn.rollback()
        finally:
            conn.commit()
            conn.close()


def generate_metrics(cur, group_by_field, generated_table_name):
    part_of_day = """CASE
                        WHEN EXTRACT(hour from timestamp) between 0 and 12 THEN 'morning'
                        WHEN EXTRACT(hour from timestamp) between 12 and 17 THEN 'afternoon'
                        WHEN EXTRACT(hour from timestamp) between 17 and 24 THEN 'evening'
                    END"""
    generate_metrics_sql = f"""SELECT COUNT({group_by_field}), {group_by_field}, {part_of_day}
                                INTO {generated_table_name}
                                FROM {table_name}
                                GROUP BY {group_by_field}, {part_of_day}
                                ORDER BY COUNT({group_by_field}) DESC"""
    cur.execute(generate_metrics_sql)


# I dont have a primary key
def create_table(cur):
    create_table_sql = f"""CREATE TABLE IF NOT EXISTS {table_name}
        (
            "timestamp" timestamp without time zone NOT NULL,
            user_id integer,
            platform text,
            course_id smallint,
            chapter_id smallint
        )"""
    cur.execute(create_table_sql)


def import_data(cur):
    csv_to_postgres_sql = f"COPY {table_name} FROM STDIN DELIMITER ',' CSV HEADER"
    cur.copy_expert(csv_to_postgres_sql, open(csv_full_file_path, "r"))


# define a function that handles and parses psycopg2 exceptions
def print_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()

    # get the line number when exception occurred
    line_num = traceback.tb_lineno

    # print the connect() error
    print(f"psycopg2 ERROR: {err}, on line number: {line_num}")
    print(f"psycopg2 traceback: {traceback} -- type: {err_type}")

    # psycopg2 extensions.Diagnostics object attribute
    print(f"extensions.Diagnostics: {err.diag}")

    # print the pgcode and pgerror exceptions
    print(f"pgerror: {err.pgerror}")
    print(f"pgcode: {err.pgcode}")


if __name__ == '__main__':
    process_data()