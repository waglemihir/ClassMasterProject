# define a function that handles and parses psycopg2 exceptions
import sys


def print_psycopg2_exception(self, err):
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