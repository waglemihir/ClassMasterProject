import psycopg2
from psycopg2 import OperationalError
from utils import print_psycopg2_exception

class MasterClass:
    def __init__(self, db_configs):
        self.db_configs = db_configs

    # 393 null user_ids and 24 null course_ids
    def process_data(self):
        try:
            conn = psycopg2.connect(host=self.db_configs['host_name'],
                                    port=self.db_configs['port'],
                                    dbname=self.db_configs['dbname'],
                                    user=self.db_configs['user_name'],
                                    password=self.db_configs['password'])
        except OperationalError as err:
            # pass exception to function
            print_psycopg2_exception(err)

            # set the connection to 'None' in case of error
            conn = None

        if conn != None:
            cur = conn.cursor()

            try:
                self.create_table(cur)
                self.import_data(cur)
                self.generate_metrics(cur, 'platform', 'number_of_users_per_platform')
                self.generate_metrics(cur, 'course_id', 'number_of_users_per_courses')
                self.generate_metrics(cur, 'chapter_id', 'number_of_users_per_chapters')
            except Exception as err:
                # pass exception to function
                print_psycopg2_exception(err)

                # rollback the previous transaction before starting another
                conn.rollback()
            finally:
                conn.commit()
                conn.close()

    def generate_metrics(self, cur, group_by_field, generated_table_name):
        part_of_day = """CASE
                            WHEN EXTRACT(hour from timestamp) between 0 and 12 THEN 'morning'
                            WHEN EXTRACT(hour from timestamp) between 12 and 17 THEN 'afternoon'
                            WHEN EXTRACT(hour from timestamp) between 17 and 24 THEN 'evening'
                        END"""
        generate_metrics_sql = f"""SELECT COUNT({group_by_field}), {group_by_field}, {part_of_day}
                                    INTO {generated_table_name}
                                    FROM {self.db_configs['table_name']}
                                    GROUP BY {group_by_field}, {part_of_day}
                                    ORDER BY COUNT({group_by_field}) DESC"""
        self.drop_table(cur, generated_table_name)
        cur.execute(generate_metrics_sql)
        return True

    @staticmethod
    def drop_table(cur, table_to_drop):
        drop_table_sql = f"""DROP TABLE IF EXISTS {table_to_drop}"""
        cur.execute(drop_table_sql)
        return True

    def create_table(self, cur):
        create_table_sql = f"""CREATE TABLE {self.db_configs['table_name']}
            (
                timestamp timestamp without time zone NOT NULL,
                user_id integer,
                platform text,
                course_id smallint,
                chapter_id smallint,
                id serial primary key
            )"""
        self.drop_table(cur, self.db_configs['table_name'])
        cur.execute(create_table_sql)
        return True

    def import_data(self, cur):
        csv_to_postgres_sql = f"COPY {self.db_configs['table_name']} " \
                              f"(timestamp, user_id, platform, course_id, chapter_id) " \
                              f"FROM STDIN DELIMITER ',' CSV HEADER"
        cur.copy_expert(csv_to_postgres_sql, open(self.db_configs['csv_full_file_path'], "r"))
