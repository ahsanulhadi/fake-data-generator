# fake-data-generator
This python3 application generates fake data for a predefined dataset (columns, types). More details here:
https://wiki.au.deloitte.com/display/C2/Utility+Script%3A+Generate+Fake+Data

## Features

1. Will be able to generate fake data for two sample data sets. These are: **orders** and **product_list**. Orders will have product_id as a foreign key. There will be 10 sample products.
2. These fake data will be generated using a python package called 'Faker'.
3. You can specify couple of runtime arguments (so that we can run this as a job).
4. You can specify output file location.
5. Faker is really slow (specially when generating names i.e. fake text data). So this app will spin up multiple Processes for larger files and get faster output. If the given number of rows is < 500 then number of process will be set to 2 and if rows are > 5000 then number of process will be 10 and otherwise by default, it will be 5

## To-Do (Future tasks)
1. Add more datasets
2. Load to S3 option

## How to run:

*To get help and see all available argument options.*

    python3 generate_fake_datafiles.py -h
*Run script with multiple process option enabled.*

    python3 generate_fake_datafiles.py -n 1000 -d ',' -t orders -p no -w yes -m yes  # preferred
*Run script without Multiple process. Will be very slow*

    python3 generate_fake_datafiles.py -n 1000 -d ',' -t orders -p no -w yes -m no
*In below scenario, number of rows is not required and it will be a single process.*

    python3 generate_fake_datafiles.py  -d ',' -t product_list -p no -w yes -m no

## Argument description

optional arguments:
  -h, --help            show this help message and exit
  -n NUM_OF_ROWS, --num_of_rows NUM_OF_ROWS. Specify the number of rows. Only accepts INT.
  -d DELIMITER, --delimiter DELIMITER. Specify the delimiter. For example: ',' / '|'.
  -t {orders,product_list}, --dataset {orders,product_list}. Specify the dataset/table name. Currently allowed: orders, product_list
  -o OUTPUTDIR, --outputdir OUTPUTDIR. Define output directory. Default will be ~/sample-data-sources/record-count
  -p {yes,no}, --printdata {yes,no}. yes = Print on screen / no = Will not be. displayed/printed.
  -w {yes,no}, --writetofile {yes,no}. yes = Write to File / no = do not write.
  -m {yes,no}, --multiprocess {yes,no}. yes = Enable multiprocessing / no = Disable multiprocessing.

## Prerequisite

1. Install Python 3
https://realpython.com/installing-python/
check version:
$ python --version
$ python3 --version

2. Install packager: Faker.
https://faker.readthedocs.io/en/master/
$ pip install Faker
(If we don't have pip installed then install pip as well.)
