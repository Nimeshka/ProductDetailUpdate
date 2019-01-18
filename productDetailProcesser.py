#!/usr/bin/env python

import os 
import time
import glob
import logging
import datetime
import ConfigParser
from pymongo import MongoClient, UpdateOne, errors


import json

config = ConfigParser.RawConfigParser()
config.read("config.ini")


try:
    # get logging configs
    logger = logging.getLogger()
    logging.basicConfig(filename=config.get('logging', 'log_file'), format=config.get('logging', 'format'))

    if config.get('logging', 'disabled') == "1":
        logger.disabled = True
    
    level = logging.getLevelName(config.get('logging', 'level'))
    logger.setLevel(level)
    logging.info('Logging configuration loaded from config file.')

except ConfigParser.NoOptionError, e:
    # set default logging params
    logging.basicConfig(filename='log/debug.log', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)
    logging.info('Using default logging configuration')

logging.info('Service Started!')
print('Service Started!')

try:
    con_string = 'mongodb://{0}:{1}@{2}:{3}/{4}'.format(
                                config.get('mongo', 'user'), 
                                config.get('mongo', 'password'), 
                                config.get('mongo', 'host'), 
                                config.get('mongo', 'port'), 
                                config.get('mongo', 'database')
                                )

    client = MongoClient(con_string)
    db = client.dvpdb
    external_users = db.externaluserfacilities
except Exception, e:
    logging.exception(e)
    raise


# start the main process.
def main():
    try:
        os.chdir(config.get('data', 'data_dir'))

        #set processed data dir path.
        processed_dir = config.get('data', 'processed_dir')

        while True:
            
            for file in sorted(glob.glob("*.txt"), key=os.path.getctime):
                with open(file) as fp:  
                    
                    line = fp.readline()
                    logging.info("Recieved file: " + file)
                    
                    operations = []
                    json_obj = new_object()
                    last_ssn = None

                    while line:
                        line = line.strip()
                        product_data = line.split('|')

                        if len(product_data) == 8:

                            ssn = product_data[0].strip()
                            facitlity_type = product_data[2].strip()
                            facility_id = product_data[3].strip()
                            param1 = product_data[4].strip()
                            param2 = product_data[5].strip()
                            param3 = product_data[6].strip()

                            if last_ssn is not None and last_ssn != ssn:
                                # SSN Changed. Append and empty the object.
                                
                                operations.append(
                                    UpdateOne(
                                    { "ssn": json_obj["ssn"] },
                                    { 
                                        "$setOnInsert": { "created_at": datetime.datetime.utcnow() },
                                        "$currentDate": { "updated_at": True },
                                        "$set": json_obj,
                                    }
                                , upsert=True)
                                )
                                json_obj = new_object()

                            # Set the last SSN to the new value
                            last_ssn = ssn
                            json_obj["ssn"] = ssn

                            if facitlity_type not in json_obj["facitlity_type"]:
                                json_obj["facitlity_type"][facitlity_type] = []
                            
                            json_obj["facitlity_type"][facitlity_type].append({
                                                                            "facility_id":facility_id,
                                                                            "param1": param1,
                                                                            "param2":param2,
                                                                            "param3": param3,
                                                                        })
                        
                        line = fp.readline()
                    
                    
                    # End of the file. Append the current data to the opeations array.
                    operations.append(
                                    UpdateOne(
                                    { "ssn": json_obj["ssn"] },
                                    { 
                                        "$setOnInsert": { "created_at": datetime.datetime.utcnow() },
                                        "$currentDate": { "updated_at": True },
                                        "$set": json_obj,
                                    }
                                , upsert=True)
                                )
                    
                    if len(operations) > 0:    
                        logging.info("Executing batch upsert!")

                        try:
                            result = external_users.bulk_write(operations, ordered=False)

                            logging.info("Result: Total Records found = {}, Matched = {}, Inserted = {}, Modified = {}, Upserted = {}".format(
                                len(operations), result.matched_count, result.inserted_count, result.modified_count, result.upserted_count
                            ))
                            
                        except errors.BulkWriteError as be:
                            logging.debug("Bulk operation completed with some errors:")
                            logging.info(be.details)
                        
                    else:
                        logging.info("No records to update.")

                    # move the current file to the processed directory
                    logging.info("Moving {} to {}".format(file, processed_dir + "/" + file + "._" + str(time.time())))
                    os.rename(file, processed_dir + "/" + file + "._" + str(time.time()))

            time.sleep(10)

    except Exception as e:
        logging.exception(e)
        raise

def new_object():
    return {
            "ssn": "",
            "facitlity_type": {}
        }

if __name__ == "__main__":
    main()