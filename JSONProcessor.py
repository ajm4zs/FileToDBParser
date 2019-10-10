# Authors: Alex Mulchandani and Nick Kharas

from pandas.io.json import json_normalize
from file_integrity_checks import pathExists
import json

class JSONProcessor:

    def __init__(self, file_path='', file_name=''):
        self.file_path = file_path
        self.file_name = file_name 

    # Extracts the contents of a file and returns the JSON
    # TO-DO: we may need to mod this to incorporate some sort of stream process
    def extract_file_contents(self):

        # throw error if path doesn't exist
        if (not pathExists(self.file_path)):
            raise Exception('The file path does not exist.')
        # store entire file path and name in filePath
        filePath = self.file_path + self.file_name

        # try to open the file
        try:
            fileContents = open(filePath)
        except FileNotFoundError as fnf_error:
            raise Exception(fnf_error)

        # try to parse file contents as JSON
        try:
            jsonData = json.load(fileContents)
        except:
            raise Exception('Error in parsing the file as JSON.')
        finally:
            # close the input file
            fileContents.close()

        return jsonData

    # Normalizes json and returns the associated dataframe
    def get_dataframe_from_json (self, json):
        # try to normalize the json into a data frame
        try:
            data_normalized = json_normalize(json)
        except:
            raise Exception('Unable to convert JSON into a dataframe.')
        
        return data_normalized
