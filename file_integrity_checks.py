import os


# throw exception if file already exists
def checkFileExistence(fullFilePath):
  if (os.path.isfile(fullFilePath)):
    raise Exception('The file you want to write to already exists.')

# return True/False if file exists
def fileExists(fullFilePath):
  return os.path.isfile(fullFilePath)

# return True/False if path exists
def pathExists(path):
  return os.path.exists(path) # comment