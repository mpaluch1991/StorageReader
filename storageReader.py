import datetime
import zipfile
import os
import time
import configparser
import logging
import traceback
import shutil

save_method = 0
default_config_section = 'DEFAULT'
config_file_name = 'conf.ini'
log_file_name = 'storageReader_' + time.strftime("%Y%m%d-%H%M%S") + '.log'
logging.basicConfig(filename=log_file_name, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%m/%d/%Y %H:%M:%S %p', level=logging.INFO)


def get_paths_to_read():
    path_to_read = read_properties_ini(config_file_name, default_config_section, 'path_to_read')
    paths = path_to_read.split(',')
    return paths


def read_properties_ini(ini_file, section, properties):
    config = configparser.ConfigParser()
    value = ''

    if os.path.isfile(ini_file):
        config.read(ini_file)
    else:
        logging.error("Properties file %s does not exist", ini_file)
        logging.info('End')
        exit()
    try:
        value = config[section][properties]
        # value = config.get(section, properties)
    except Exception as exception:
        logging.error("Missing properties: %s in section: %s", properties, section)

    return value


def get_modification_time(fp):
    # modification_time1 = datetime.datetime.fromtimestamp(os.path.getctime(fp)) # format 2020-03-24 21:08:22.927857
    # Get file's Last modification time stamp only in terms of seconds since epoch
    mod_timesince_epoc = os.path.getmtime(fp)
    # Convert seconds since epoch to readable timestamp
    modification_time = time.strftime("%Y-%m-%d %H:%M:%S",
                                      time.localtime(mod_timesince_epoc))  # format 2020-03-24 21:08:22

    # print('modification_time1 ', modification_time1)
    # print('modification_time ', modification_time)
    return modification_time


def get_creation_time(fp):
    # creation_time1 = datetime.datetime.fromtimestamp(os.path.getctime(fp))
    # Get file'sCreation time time stamp only in terms of seconds since epoch
    cre_timesince_epoc = os.path.getctime(fp)
    # Convert seconds since epoch to readable timestamp
    creation_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(cre_timesince_epoc))

    # print('creation_time1 ', creation_time1)
    # print('creation_time ', creation_time)
    return creation_time


def get_filename_extension(fn):
    # extension = os.path.splitext(fn)[-1].replace(".","")  # other solution to get extension, return with dot
    extension = fn.split(".")[-1]
    return extension


def read_storage(sv):
    file_counter = 0
    file_in_folder_counter = 0
    print_counter = 0

    # config_file_name = 'conf.ini'
    # default_config_section = 'DEFAULT'
    # log_file_name = 'storageReader_' + time.strftime("%Y%m%d-%H%M%S") + '.log'

    logging.info('Start')
    separator = read_properties_ini(config_file_name, default_config_section, 'separator')
    zip_extensions = read_properties_ini(config_file_name, default_config_section, 'zip_extensions')
    result_file_name = read_properties_ini(config_file_name, default_config_section, 'result_file_name')
    file_count_status = read_properties_ini(config_file_name, default_config_section, 'many_files_to_status')
    paths = get_paths_to_read()

    if sv == 1:
        header = "TYPE" + separator + "SRCFILEPATH" + separator + "SRCFILENAME" + separator + "SRCFULLPATH" + separator + "FILENAME" + separator + "SRCFILESIZE" + separator + "MODIFICATIONDATE" + separator + \
                 "CREATIONDATE" + separator + "EXTENSION" + separator + "MAIN_FILE_EXTENSION" + separator + "MAIN_FILE_SIZE" + separator + "CAN_BE_EXTRACTED"

        f = open(result_file_name, 'w')
        f.write(header + '\n')
        f.close()
    elif sv == 2:
        data = [
            f'TYPE{separator}SRCFILEPATH{separator}SRCFILENAME{separator}SRCFULLPATH{separator}FILENAME{separator}SRCFILESIZE{separator}MODIFICATIONDATE{separator}CREATIONDATE{separator}EXTENSION{separator}MAIN_FILE_EXTENSION{separator}MAIN_FILE_SIZE{separator}CAN_BE_EXTRACTED']
    else:
        print('ERROR')

    for path in paths:
        print("The folder being read: " + path)
        logging.info('Reading folder %s', path)
        # for root, dirs, files in os.walk("D:\\rockhill\\"):
        for root, dirs, files in os.walk(path):
            if path != root:
                print("The sub-folder being read: " + root)
                logging.info('Reading sub-folder %s', root)

            for filename in files:
                file_counter = file_counter + 1
                file_in_folder_counter = file_in_folder_counter + 1

                extension = get_filename_extension(filename)
                file_path = root + "\\" + filename
                file_stats = os.stat(file_path)

                if zipfile.is_zipfile(file_path) and extension.lower() in zip_extensions:

                    try:
                        z = zipfile.ZipFile(file_path, "r")
                    except Exception as exception:
                        logging.error(file_path + ' is corrupted. Data has not been saved to CSV. More info inside ERROR_ file.')
                        f = open('ERROR_' + time.strftime("%Y%m%d-%H%M%S") + '.log', 'a')
                        f.write(file_path + ' is corrupted. \n')
                        f.write(str(traceback.format_exc()) + '\n')
                        f.close()
                        continue

                    for zip_file_elem in z.infolist():
                        # extension_in_zip = zip_file_elem.filename.split('.')[-1]  # extension of file in zip
                        extension_in_zip = get_filename_extension(zip_file_elem.filename)
                        org_size_in_zip = zip_file_elem.file_size
                        # compress_size = zip_file_elem.compress_size # not need compressed size
                        # bytes = len(z.read(zip_file_elem))  # get size for file inside zip; sometimes return error Bad CRC-32 for file
                        filename_in_zip = zip_file_elem.filename  # filename of element in zip
                        modif_and_cre_time = datetime.datetime(
                            *zip_file_elem.date_time)  # modify date and creation date for files inside zip (By default ZIP file stores only the modification date of a file)

                        if sv == 1:
                            record = "ZIP_EXT" + separator + root + separator + filename + separator + file_path + separator + filename_in_zip + separator + str(org_size_in_zip) + separator + str(
                                modif_and_cre_time) + separator + \
                                     str(modif_and_cre_time) + separator + extension_in_zip + separator + extension + separator + str(file_stats.st_size) + separator + "Yes"
                            # print(record)
                            f = open(result_file_name, 'a')
                            f.write(record + '\n')
                            f.close()
                        elif sv == 2:
                            data.append(
                                f'ZIP_EXT{separator}{root}{separator}{filename}{separator}{file_path}{separator}{filename_in_zip}{separator}{org_size_in_zip}{separator}{modif_and_cre_time}{separator}{modif_and_cre_time}{separator}{extension_in_zip}{separator}{extension}{separator}{file_stats.st_size}{separator}Yes')
                        else:
                            print('ERROR')
                else:
                    if zipfile.is_zipfile(file_path):
                        to_extraction = "Yes"
                    else:
                        to_extraction = "No"

                    if sv == 1:
                        record = "NO_ZIP_EXT" + separator + root + separator + filename + separator + file_path + separator + filename + separator + str(file_stats.st_size) + separator + \
                                 str(get_modification_time(file_path)) + separator + str(get_creation_time(file_path)) + separator + extension + separator + extension + separator + \
                                 str(file_stats.st_size) + separator + to_extraction
                        # print(record)
                        f = open(result_file_name, 'a')
                        f.write(record + '\n')
                        f.close()
                    elif sv == 2:
                        data.append(
                            f'NO_ZIP_EXT{separator}{root}{separator}{filename}{separator}{file_path}{separator}{filename}{separator}{file_stats.st_size}{separator}{get_modification_time(file_path)}{separator}{get_creation_time(file_path)}{separator}{extension}{separator}{extension}{separator}{file_stats.st_size}{separator}{to_extraction}')
                    else:
                        print('ERROR')

                print_counter = print_counter + 1

                if print_counter == int(file_count_status):
                    logging.info("STATUS: I have read %s files", file_counter)
                    print_counter = 0

            if file_in_folder_counter != 0:
                logging.info('Count of file in sub-folder %s is %s', root, file_in_folder_counter)
            file_in_folder_counter = 0

    '''
    for elem in data:
        print(elem)
    '''

    # buffering=1 => \n means we finish a line, so this line is wirtten to hard disk
    # no buffering => using DEFAULT_BUFFER_SIZE (8192 bytes)
    if sv == 2:
        f = open(result_file_name, 'w')
        for record in data:
            f.write(record + '\n')
        f.close()

    logging.info('Count of files read: %s', file_counter)
    logging.info("End")


def copy2_verbose(src, dst):
    # print('Copying {0}'.format(src))
    shutil.copy2(src, dst)


def copy_folder():
    paths = read_properties_ini(config_file_name, default_config_section, 'source_path')
    srcs = paths.split(',')
    dest_root = read_properties_ini(config_file_name, default_config_section, 'destination_root_path')

    for src in srcs:
        dest = dest_root + "\\" + os.path.basename(os.path.normpath(src))
        # Copy the content of
        # source to destination
        # using shutil.copy() as parameter
        # shutil.copytree(src, dest, copy_function=shutil.copy)
        shutil.copytree(src, dest, copy_function=copy2_verbose)


def unzip():
    zip_extensions = read_properties_ini(config_file_name, default_config_section, 'zip_extensions')
    curr_dir = read_properties_ini(config_file_name, default_config_section, 'destination_root_path')
    for root, dirs, files in os.walk(curr_dir):
        for filename in files:
            file_path = root + '\\' + filename
            if zipfile.is_zipfile(file_path) and get_filename_extension(filename).lower() in zip_extensions:
                try:
                    zip_obj = zipfile.ZipFile(file_path, 'r')
                except Exception as exception:
                    logging.warning(file_path + ' is corrupted. Data has not been extracted. ZIP has not been removed.')
                    continue

                # with zipfile.ZipFile(file_path, 'r') as zipObj:
                zip_name = filename.split(".")
                folder_name = zip_name.pop(0) + "_ZIP_TMP"
                path_to_unzip = root + "\\" + folder_name

                try:
                    zip_obj.extractall(path=path_to_unzip)
                except Exception as exception:
                    logging.warning('ZIP file ' + file_path + ' extracted, but files inside can be corrupted. ZIP has not been removed.')
                    continue
                zip_obj.close()
                os.remove(file_path)


###############################################################################
# MAIN
###############################################################################

type = input("Select the type of files reading\n"
             "1 - Get info from source paths (Save each line to csv)\n"
             "2 - Get info from source paths (Save data to csv at one shot)\n"
             "3 - Copy source folder to other location\n"
             "4 - Copy source folder to other location and unzip\n"
             "5 - Unzip files from destination folder\n"
             "Exit - Exit app\n"
             "Your choose: ")

# try:

if type == "1":
    logging.info('Start: Save each line to csv, OPTION 1')
    save_method = 1
    print("...::: Save each line to csv :::...\n")
    # startTime = time.perf_counter()
    read_storage(save_method)
    # endTime = time.perf_counter()
    # print(endTime-startTime)
    os.system("pause")
    logging.info('Stop: Save each line to csv')
elif type == "2":
    logging.info('Start: Save data to csv at one shot, OPTION 2')
    save_method = 2
    print("...::: Save data to csv at one shot :::...\n")
    # startTime = time.perf_counter()
    read_storage(save_method)
    # endTime = time.perf_counter()
    # print(endTime-startTime)
    os.system("pause")
    logging.info('Stop: Save data to csv at one shot, OPTION 2')
elif type == "3":
    logging.info('Start: Copy source folder to other location, OPTION 3')
    print("...::: Copy folder :::...")
    copy_folder()
    logging.info('Stop: Copy source folder to other location')
elif type == "4":
    print("...::: Copy and Extract ZIP :::...")
    logging.info('Start: Copy source folder to other location and unzip, OPTION 4')
    copy_folder()
    unzip()
    logging.info('Stop: Copy source folder to other location and unzip')
elif type == "5":
    print("...::: Extract ZIP :::...")
    logging.info('Start: Unzip, OPTION 5')
    unzip()
    logging.info('Stop: Unzip')
elif type.upper() == 'EXIT':
    os._exit(1)
else:
    print("Choose one more time, please!")
# except :
#    f = open('MAIN_ERROR_' + time.strftime("%Y%m%d-%H%M%S") + '.log', 'w')
#    f.write(str(traceback.format_exc()))
#    f.close()
