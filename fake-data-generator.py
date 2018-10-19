# =========================================
# Script Name:  Generate_FakeDataFile.py
# Written By:   Ahsanul Hadi | E: adil.gt@gmail.com
# Date:         Sep 17, 2018
# Purpose:
# Generate Fake data. Initially we will have limited datasets with fixed columns.
# But in future we can customize more and add more options and flexibility.
# Currently works on Linux/Mac OS.
# =========================================

from argparse import ArgumentParser
from pathlib import Path
from faker import Faker
from random import randint
from multiprocessing import Pool
import multiprocessing
#from multiprocessing import freeze_support
from itertools import repeat
import random
import datetime
import os, errno
import math
import time

# =========================================
# GLOBAL VARIABLE
# =========================================
start_datetime = datetime.datetime.now().replace(microsecond=0).strftime('%Y%m%d_%H%M%S')
id_datetime = datetime.datetime.now().replace(microsecond=0).strftime('%y%m%d%H%M')

# Product is fixed for now.
product_list = ['Macbook-Pro', 'Lenovo', 'Dell', 'Hp', 'Asus', 'Acer', 'Macbook-Air', 'Microsoft-Surface', 'Samsung', 'Google-Chrome-Book', 'Omen-GL']

# =========================================
# FUNCTIONS
# =========================================

def get_current_time():
    return datetime.datetime.now().replace(microsecond=0)

# -------------------
def set_target_dir():
    home = str(Path.home())  
    target_dir = home + "/sample-data-sources/record-count"

    if not os.path.exists(target_dir):
        try:
            os.makedirs(target_dir)
            return target_dir
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
    else:
        return target_dir

# -------------------
def parse_arguments():
    parser = ArgumentParser()
    parser.add_argument("-n", "--num_of_rows", help="Specify the number of rows. Only accepts INT.", type=int, default=10)
    parser.add_argument("-d", "--delimiter", help="Specify the delimiter. For example: ',' / '|'. ", default=",")
    parser.add_argument("-t", "--dataset", help="Specify the dataset/table name. Currently allowed: orders, product_list", choices=['orders', 'product_list'], required=True)
    parser.add_argument("-o", "--outputdir", help="Define output directory. Default will be ~/sample-data-sources/record-count", default=set_target_dir())
    parser.add_argument("-p", "--printdata", help="yes = Print on screen / no = Will not be displayed/printed.", choices=['yes', 'no'], default='no')
    parser.add_argument("-w", "--writetofile", help="yes = Write to File / no = do not write.", choices=['yes', 'no'], default='no', required=True)
    parser.add_argument("-m", "--multiprocess", help="yes = Enable multiprocessing / no = Disable multiprocessing.", choices=['yes', 'no'], default='no')

    # Parse the arguments
    args = parser.parse_args()
    return args

# -------------------
def generate_data(column_name):
    fake = Faker()
    membership_status = ['Gold', 'Silver', 'Platinum']

    """
    # Another way to do it.
    p = fake.profile(fields=['name', 'sex', 'mail'])
    for key, value in p.items():
        print(key + ": " + value)
    """
    if column_name is None:
        print("(X) Column Name is not provided. Abort application.")
        quit()
    if column_name == 'id': # Not required
        column_value = None
    if column_name == 'customer_name':
        column_value = fake.profile()["name"]  # fake.name()
    if column_name == 'sex':
        column_value = fake.profile()["sex"]
    if column_name == 'birthdate':
        column_value = fake.profile()["birthdate"]
    if column_name == 'email':
        column_value = fake.profile()["mail"]
    if column_name == 'address':
        t = fake.profile()["residence"]
        column_value =  t.replace(',', '\,').replace('\r', ' ').replace('\n', ' ')
    if column_name == 'membership_status':
        column_value = random.choice(membership_status)
    if column_name == 'product_id':
        num_of_products = len(product_list)
        column_value = randint(1, num_of_products)


    return column_value

# -------------------
def populate_table(args, proc_id, row_start_num, delta):
    process = multiprocessing.current_process()
    pid = process.pid
    start = get_current_time()

    dataset = args.dataset
    delim = args.delimiter
    printdata = args.printdata
    write_to_file = args.writetofile
    output_dir = args.outputdir

    if pid < 10:
        id_prefix = id_datetime + '0' + str(pid)
    else:
        id_prefix = id_datetime + str(pid)

    if int(proc_id) < 10:
        file_id = '0' + str(proc_id)
    else:
        file_id = str(proc_id)

    target_filename = args.dataset + "__" + start_datetime + "_" + str(file_id) + ".csv"
    temp_filename = args.dataset + "__" + start_datetime + "_" + str(file_id) + ".tmp"
    output_file = output_dir + "/" + temp_filename
    final_output_file = output_dir + "/" + target_filename

    if proc_id == num_of_process:  # it is the last assigned process.
        row_end_num = total_rows
    else:  # it is not the last process.
        row_end_num = (row_start_num + delta) -1

    print('Process#: ' + str(proc_id) + \
         '| Process PID: ' + str(pid) + \
         '| Row start num: ' + str(row_start_num) + \
         '| Row end num: ' + str(row_end_num))
        # -------------------------------------

    if dataset == "orders":
        #columns = {}.fromkeys(['id', 'name', 'sex', 'birthdate', 'email', 'address', 'job_role', 'dept_id'], '')
        column_list = ['id', 'customer_name', 'sex', 'birthdate', 'email', 'membership_status', 'product_id']
        num_of_cols = len(column_list)

        # -- Print Column Header --
        col_pos = 0
        while col_pos < num_of_cols:
            colname = column_list[col_pos]

            if col_pos == 0:
                header = colname + delim # First column name
            elif col_pos == num_of_cols -1:
                header = header + colname # Last column name, no delim at the end.
            else:
                header = header + colname + delim
            col_pos += 1

        if printdata == 'yes':
            print(header)
        if write_to_file == 'yes':
            with open(output_file, 'w') as fp:
                fp.write(header + '\n')

        # Populate Data/ Rows.
        i = row_start_num

        while i <= row_end_num:
            col_pos = 0
            while col_pos < num_of_cols:
                colname = column_list[col_pos]

                if colname == 'id':
                    # column_value = str(i) # row_num
                    column_value = id_prefix + str(i)
                else:
                    column_value = str(generate_data(colname))
                    #column_value = column_value.replace(',', '\,').replace('\r', ' ').replace('\n', ' ')

                if col_pos == 0:
                    record = column_value + delim
                elif col_pos == num_of_cols -1:
                    record = record + column_value
                else:
                    record = record + column_value + delim
                col_pos += 1

            if printdata == 'yes':
                print(record)
            if write_to_file == 'yes':
                with open(output_file, 'a') as fp:
                    fp.write(record + '\n')
            i += 1

    elif dataset == "product_list":
        column_list = ['id', 'product_name']
        num_of_cols = len(column_list)
        row_end_num = len(product_list)

        # -- Print Column Header --
        col_pos = 0
        while col_pos < num_of_cols:
            colname = column_list[col_pos]

            if col_pos == 0:
                header = colname + delim # First column name
            elif col_pos == num_of_cols -1:
                header = header + colname # Last column name, no delim at the end.
            else:
                header = header + colname + delim
            col_pos += 1

        if printdata == 'yes':
            print(header)
        if write_to_file == 'yes':
            with open(output_file, 'w') as fp:
                fp.write(header + '\n')

        # Populate Data/ Rows.
        i = row_start_num

        while i <= (row_end_num - 1):
            col_pos = 0
            while col_pos < num_of_cols:
                colname = column_list[col_pos]

                if colname == 'id':
                    column_value = str(i) # row_num
                else:
                    #column_value = str(generate_data(colname))
                    column_value = product_list[i]
                    #column_value = column_value.replace(',', '\,').replace('\r', ' ').replace('\n', ' ')

                if col_pos == 0:
                    record = column_value + delim
                elif col_pos == num_of_cols -1:
                    record = record + column_value
                else:
                    record = record + column_value + delim
                col_pos += 1

            if printdata == 'yes':
                print(record)
            if write_to_file == 'yes':
                with open(output_file, 'a') as fp:
                    fp.write(record + '\n')
            i += 1

    else:
        pass

    os.rename(output_file, final_output_file)
    print('-> Finished writing to file. Check: ' + final_output_file)

    # --- Final task before returning status ---
    end = get_current_time()
    times_taken = end - start

    status = 'Process #: ' + str(proc_id) + \
             '| Process PID: ' + str(pid) + \
             '| Row range: ' + str(row_start_num) + ' - ' + str(row_end_num) + \
             '| Started: ' + start.strftime('%Y.%m.%d %H:%M:%S') + \
             '| Finished: ' + end.strftime('%Y.%m.%d %H:%M:%S') + \
             '| Times Taken: ' + str(times_taken)

    return status

# -------------------
def set_num_of_process(num_of_rows):
    cpu_count = multiprocessing.cpu_count()
    if num_of_rows > 5000:
        num_of_process = cpu_count
    else:
        num_of_process = 1

    return num_of_process

# =========================================
# MAIN PROGRAM
# =========================================

if __name__ == '__main__':

    # Only for Windows OS. https://docs.python.org/dev/library/multiprocessing.html#multiprocessing.freeze_support
    # freeze_support()

    start_main = get_current_time()
    args = parse_arguments() # Parse all arguments and store those in variable: args.
    total_rows = args.num_of_rows if args.dataset != 'product_list' else len(product_list)

    # Set to 1 process only when args.multiprocess = no and it is for Product List which has fixed rows.
    num_of_process = set_num_of_process(total_rows) if (args.multiprocess == "yes" and args.dataset != 'product_list') else 1
    # This will be needed to calculate row range which is distributed to each process.
    delta = math.floor(total_rows/num_of_process)

    temp = list(range(1, total_rows, delta))  # Generate a list based on that delta. example: (1, 20, 5) produce: [1, 6, 11, 16]
    row_start_num = temp[:num_of_process]  # We need to extract based on the Num of process. if num_of_process =3 then we need [1, 6, 11]

    proc_id = list(range(1, (num_of_process + 1), 1))  # generate the serial. for tracking purpose.

    # Start Pooling for Multi Processes.
    with Pool() as pool:
        results = pool.starmap(populate_table, zip(repeat(args), proc_id,  row_start_num, repeat(delta)))
        for result in results:
            print(result)

    end_main = get_current_time()
    times_taken_main = end_main - start_main
    print('-> Total time taken: ' + str(times_taken_main))
