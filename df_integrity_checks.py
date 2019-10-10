def isValueList(value):

    return isinstance(value, list)

# Checks if value is a dict.  If yes, return True.  Otherwise, return false.
def isValueDict(value):

    return isinstance(value, dict)

# Parses the first x rows of a column and returns true if any of the x rows is a list
def isColumnList(df, column, iterations):

    i = 0

    while i < iterations:
        if (isValueList(df[column][i])):
            if (isValueDict(df[column][i][0])):
                return True
            else:
                raise Exception('There is a list that does not contain a dict object.  This is bad JSON and cannot be processed.')
        i += 1

    return False

# Finds all columns that are lists within a dataframe and returns a list of the column names
def checkForLists (df):
    listColumns = []

    # we will sample the first n rows of the data frame to find if a column is a list

    df_size = len(df.index)
    sample_size = 100
    iterations = min(df_size, sample_size)

    for col in df.columns:
        if (isColumnList(df, col, iterations)):
            listColumns.append(col)

    return listColumns

# Removes list columns from a df and returns the df
def removeListColumnsFromDataframe(df, listColumns):
    dfWithoutColumns = df.drop(columns=listColumns)
    return dfWithoutColumns
