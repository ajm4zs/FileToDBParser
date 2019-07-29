# Authors: Alex Mulchandani and Nick Kharas

from xml.etree import ElementTree as ET
import os  # File handling
import gc  # Garbage collection
import csv  # To convert the dictionary key value pairs into a CSV format


class XMLProcessor:

    def __init__(self, file_path='', file_name=''):
        self.file_path = file_path
        self.file_name = file_name

    def recurse_xml_tag(self, element):
        """
        Recurse through the XML element to extract data from all levels int he XML hierarchy
        arg1 (XML element) : Element to recurse through in the XML file
        """
        contents = list()  # Array of all rows
        for item in element:
            column = {}  # Dictionary for row
            if item.text is not None:
                column[item.tag] = item.text.replace('\n', ' ')
            column.update(item.attrib)
            sub_row = self.recurse_xml_tag(item)
            if sub_row is not None and len(sub_row) > 0:
                column[item.tag] = sub_row
            contents.append(column)
        return contents

    def parse_xml(self, xmltag):
        """
        Parse thorugh the XML tag and extract the data
        arg1 - XML tag / element name to recurse through in the XML file
        """
        file_contents = []
        row_list = self.recurse_xml_tag(xmltag)
        # Without nested elements, row_list is sufficient for output.
        # With nested elements, row_list produces a list of dictionaries with uneven fields. This needs to be combined to produce a list of dictionaries with same fields.
        if set(row_list[0]) == set(row_list[1]):
            file_contents = row_list
        else:
            row_dict = dict((key,d[key]) for d in row_list for key in d)
            file_contents.append(row_dict)
        return(file_contents)

    # Read data from each child node under root
    def xml_to_dict(self, *args):
        """
        Convert the XML file into a JSON compatible dictionary.
        If the optional parameter is not specified, then all contents of the XML file will be pulled
        
        *args (Optional parameter) - Give the user an option to pull data appearing only in a particular tag under the root element
        """
        arglen = len(args)
        
        # Open the XML file
        xml_file = open(self.file_path + '\\' + self.file_name,'rb')  # Read the XML file
        xmldoc = ET.parse(xml_file)  # Parse the XML file
        root = xmldoc.getroot()

        file_contents = []
        for child in root:
            if arglen == 0:
                # Pull all data regardless
                file_contents = self.parse_xml(child)
            else:
                # Only pull data from tag that is input ignore the rest
                if child.tag == args[0]:
                    file_contents = self.parse_xml(child)
        return(file_contents)
