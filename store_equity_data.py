from BhavUtils.bhav_db import BhavDB
from BhavUtils.bhav_data_files import BhavFiles
import glob
from os import stat
from stat import *

def main(bhav_dir):
    bhav_files = BhavFiles(bhav_dir)
    bhav_db = BhavDB()
    for csv_data, zip_file_name in bhav_files.get_csv_data():
        if csv_data is not None:
            for csv_row in csv_data:
                # we're passing in the zip file name so we can log db errors with the name
                # should be easier to track down where exactly the rogue data was
                bhav_db.insert_bhav_row(csv_row, zip_file_name)


if __name__ == '__main__':
    main(r"C:\Users\barraud\Downloads\bhav\\")
    print("all done!")
    # after everything's done let's do a quick check to see if any logging (errors, maybe) happened
    logs:list = glob.glob("./logs/*.log")
    for log in logs:
        if stat(log)[ST_SIZE] > 0:
            print("Log {} seems to have some stuff in it. You might want to check if errors happened".format(log))
