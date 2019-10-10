from XMLProcessor import XMLProcessor
from JSONProcessor import JSONProcessor
from DFProcessor import DFProcessor

import argparse

if __name__ == '__main__':

    # parse args

    parser = argparse.ArgumentParser(
        description='This procedure is used to write a JSON file to a table in SQL.')
    parser.add_argument('--filePath', '-fp', required=True, help='Enter the file path of your file.')
    parser.add_argument('--fileName', '-fn', required=True, help='Enter the name of the file.')
    parser.add_argument('--server', '-s', required=True, help='Enter the destination server.', default='file') # default='RICKY' default='file'
    parser.add_argument('--database', '-d', required=True, help='Enter the destination database.') # default='DALabA_Scratch' default=''
    parser.add_argument('--tableName', '-tn', required=True, help='Enter the destination table.')
    parser.add_argument('--dropTableIfExists', '-dt', required=False,
                        help='[Optional] 1 if we want to drop table if already exists.  If 0 and table already exists, rows will be appended.', choices=range(0, 2), default=1, type=int)
    parser.add_argument('--fileType', '-ft', required=False,
                        help='[Optional] What is the type of file we are processing? Default value is JSON.', choices=['XML', 'JSON'], default='JSON')
    parser.add_argument('--xmlTag', '-xt', required=False, help='[Optional] Which XML tag under root should we pull data from.', default=None)
    args = parser.parse_args()

    print('Check XML tag here')
    print(args.xmlTag)

    # TO-DO: sort out how to output to flat file

    if(args.fileType == 'JSON'):
        json_processor = JSONProcessor(args.filePath, args.fileName)
        json_data = json_processor.extract_file_contents()
        df = json_processor.get_dataframe_from_json(json_data)
    elif(args.fileType == 'XML'):
        xml_processor = XMLProcessor(args.filePath, args.fileName)
        if args.xmlTag is None:
            json_data = xml_processor.xml_to_dict()
        else:
            json_data = xml_processor.xml_to_dict(args.xmlTag)
        json_processor = JSONProcessor()
        df = json_processor.get_dataframe_from_json(json_data)
    else:
        raise Exception('Invalid file format.')


    df_processor = DFProcessor(args.server, args.database, args.tableName, args.dropTableIfExists)

    #engine = df_processor.get_engine()
    #connection = df_processor.connect_engine(engine)
	
    if(args.server == 'file'):
        engine = args.filePath
        connection = 'file'
    else:
        engine = df_processor.get_engine()
        connection = df_processor.connect_engine(engine)
    
    df_processor.process_dataframe(df, engine, connection)
