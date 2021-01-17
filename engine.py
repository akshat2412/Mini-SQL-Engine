import csv
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Comparison
from sqlparse.tokens import Keyword, DML
from collections import OrderedDict


# a = sqlparse.parse("SELECT col1, col2, col3, * from table1, table2 where condition1 = 2 and condition2 = 1 group by col1 having count(col1) > 2")[0].tokens
# for item in a:
#     print(item.ttype)
#     if isinstance(item, Where):
#         print("Where: ", item.value)
#         for t in item.tokens:
#             if isinstance(t, Identifier):
#                 print("identifier: ", t.value)
#             if isinstance(item, IdentifierList):
#                 print("Identifierlist")
#                 for tt in item.get_identifiers():
#                     print("IdentifierList: ", tt.value)
#     if isinstance(item, Identifier):
#         print("identifier: ", item.value)
#         # print(item.value)
#     if isinstance(item, IdentifierList):
#         print("Identifierlist")
#         for t in item.get_identifiers():
#             print("IdentifierList: ", t.value)
#     if item.ttype is Keyword:
#         print("Keyword: ", item)
#     print("\n")
#         # print(item)
#     # print(a.get_identifiers())
# # print(a[0].get_identifiers())
# # print(sqlparse.sql.Where)
# print("=======================")
# koistring = sqlparse.sql.IdentifierList(a).get_identifiers()
# # print(koistring)
# for token in koistring:
#     print(token)

# Constants
METADATA_FILE = "metadata.txt"

# Global variables
tables_info = OrderedDict()
columns_info = {}

# Load tables info from metadata
def init():
    f = open(METADATA_FILE, "r")

    table_name = False
    column_name = False
    curr_table_name = None
    for line in f:
        if line.strip() == '<begin_table>':
            table_name = True
            continue
        if line.strip() == '<end_table>':
            column_name = False
            continue
        if table_name:
            tables_info[line.strip()] = OrderedDict()
            curr_table_name = line.strip()
            column_name = True
            table_name = False
            continue
        if column_name:
            tables_info[curr_table_name][line.strip()] = []
            columns_info[line.strip()] = curr_table_name
        
def table_exists(table_names_list):
    for table_name in table_names_list:
        if table_name not in tables_info:
            return False
    return True

def column_exists(column_name, temp_table):
    if column_name in temp_table[0]:
        return True
    return False

def build_header(column_names_list, temp_table):
    header = []
    for column_name in column_names_list:
        if column_name.strip() == "*":
            return build_header(temp_table[0], temp_table)
        header.append(columns_info[column_name] + "." + column_name)
    
    return header

# Extract select statement parameters
def extract_select_params(query_tokens):
    select_seen = False
    select_params_list = []
    for token in query_tokens:
        if token.ttype is DML and token.value.lower() == 'select':
            select_seen = True
            continue
        if select_seen and token.ttype is Keyword:
            return select_params_list
        if select_seen and isinstance(token, IdentifierList):
            for tt in token.get_identifiers():
                select_params_list.append(tt.value)
            continue
        if select_seen and isinstance(token, Identifier):
            select_params_list.append(token.value)
            continue
    return select_params_list

# Extract from parameters
def extract_from_params(query_tokens):
    from_seen = False
    from_params_list = []
    for token in query_tokens:
        if token.ttype is Keyword and token.value.lower() == 'from':
            from_seen = True
            continue
        if from_seen and token.ttype is Keyword:
            return from_params_list
        if from_seen and isinstance(token, IdentifierList):
            for tt in token.get_identifiers():
                from_params_list.append(tt.value)
            continue
        if from_seen and isinstance(token, Identifier):
            from_params_list.append(token.value)
            continue
    return from_params_list

# Extract where conditions
def extract_where_conditions(query_tokens):
    where_conditions_list = []

    for token in query_tokens:
        # print(token)
        if isinstance(token, Where):
            for sub_token in token.tokens:
                if isinstance(sub_token, Comparison):
                    where_conditions_list.append(sub_token.value)
        return where_conditions_list

# Extract column from table
def extract_column_from_table(table_name, column_name):
    file_name = table_name + ".csv"
    values = []
    fields = tables_info[table_name].keys()
    with open(file_name, 'r') as table_file:
        reader = csv.DictReader(table_file, fields)
        for row in reader:
            values.append(row[column_name])

    return values

# Get entire table
def get_table(table_name):
    file_name = table_name + ".csv"
    values = []
    with open(file_name, 'r') as table_file:
        reader = csv.reader(table_file)
        for row in reader:
            values.append(row)

    return values

def get_column_names(table_name):
    return tables_info[table_name].keys()

def is_aggregate_function(column_name):
    if column_name.lower().startswith("sum"):
        return True
    if column_name.lower().startswith("avg"):
        return True
    if column_name.lower().startswith("max"):
        return True
    if column_name.lower().startswith("min"):
        return True
    if column_name.lower().startswith("count"):
        return True
    return False

def get_aggregate_function_index(function):
    if function.lower().startswith("sum"):
        return 1
    if function.lower().startswith("avg"):
        return 2
    if function.lower().startswith("max"):
        return 3
    if function.lower().startswith("min"):
        return 4
    if function.lower().startswith("count"):
        return 5
    return 0

def build_temp_table(table_names):
    temp_table = []

    # append first row as the names of the columns
    temp_table.append([])

    # append second row as empty table
    temp_table.append([])

    for table_name in table_names:
        temp_table[0].extend(get_column_names(table_name))
        table = temp_table[1]
        table1 = get_table(table_name)
        if len(table) == 0:
            for row in table1:
                table.append(row)
        else:
            new_table = []
            for row in table:
                for row1 in table1:
                    new_row = row + row1
                    new_table.append(new_row)
            temp_table.remove(table)
            temp_table.append(new_table)

    return temp_table

def process_query():
    a = sqlparse.parse("SELECT count(A), E from table1, table2")[0].tokens
    select_params = extract_select_params(a)
    from_params = extract_from_params(a)
    tt = build_temp_table(from_params)
    for item in select_params:
        print(item)
    for item in from_params:
        print(item)
    
    # print(build_header(select_params, tt))

if __name__ == "__main__":
    init()
    print(tables_info)
    sum
    process_query()

    # print(extract_column_from_table("table1", "B"))
    # print(get_table("table1"))
    # tt = build_temp_table("table1", "table2", "table2")

    # extract_where_conditions(a)
