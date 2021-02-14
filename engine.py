import csv
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Comparison
from sqlparse.tokens import Keyword, DML, Wildcard
from collections import OrderedDict
import sys

# Constants
METADATA_FILE = "metadata.txt"

# Global variables
tables_info = OrderedDict()
columns_info = {}

# Print error and exit
def error_and_exit(message):
    print(message)
    exit()

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
        
# Check if table exists
def table_exists(table_names_list):
    for table_name in table_names_list:
        if table_name not in tables_info:
            return False
    return True

# Check if column name exists in loaded data
def column_exists(column_name, temp_table):
    if column_name in temp_table[0]:
        return True
    return False

# Build initial header based on data loaded in memory
def build_header(column_names_list, temp_table):
    header = []
    for column_name in column_names_list:
        if column_name.strip() == "*":
            return build_header(temp_table[0], temp_table)
        header.append(columns_info[column_name] + "." + column_name)
    
    return header

# Build final header for printing
def build_final_header(column_names_list, temp_table_header):
    header = []
    for column_name in column_names_list:
        if column_name.strip() == "*":
            return temp_table_header
        if is_aggregate_function(column_name):
            header.append(column_name)
            continue
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
        if select_seen and token.ttype is Keyword and token.value.lower() != "distinct":
            return select_params_list
        if select_seen and isinstance(token, IdentifierList):
            for tt in token.get_identifiers():
                select_params_list.append(tt.value)
            continue
        if select_seen and isinstance(token, Identifier):
            select_params_list.append(token.value)
            continue
        if select_seen and is_aggregate_function(token.value):
            select_params_list.append(token.value)
            continue
        if select_seen and token.ttype is Wildcard:
            select_params_list.append(token.value)
            continue
    return select_params_list

# Does what it says
def extract_order_by_params(query_tokens):
    order_by_seen = False
    order_by_params_list = []
    for token in query_tokens:
        # print(token.value)
        # print(token.ttype)
        if token.ttype is Keyword and token.value.lower() == 'order by':
            order_by_seen = True
            continue
        if order_by_seen and isinstance(token, IdentifierList):
            for tt in token.get_identifiers():
                order_by_params_list.append(tt.value)
            continue
        if order_by_seen and isinstance(token, Identifier):
            order_by_params_list.append(token.value)
            continue
    return order_by_params_list

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
        if isinstance(token, Where):
            for sub_token in token.tokens:
                if sub_token.value.lower() == "where":
                    continue
                if isinstance(sub_token, Comparison) or sub_token.ttype is Keyword:
                    where_conditions_list.append(sub_token.value)
    return where_conditions_list

# Does what it says
def extract_group_by_params(query_tokens):
    group_by_seen = False
    group_by_params_list = []
    for token in query_tokens:
        if token.ttype is Keyword and token.value.lower() == 'group by':
            group_by_seen = True
            continue
        if group_by_seen and token.ttype is Keyword:
            return group_by_params_list
        if group_by_seen and isinstance(token, IdentifierList):
            for tt in token.get_identifiers():
                group_by_params_list.append(tt.value)
            continue
        if group_by_seen and isinstance(token, Identifier):
            group_by_params_list.append(token.value)
            continue
    return group_by_params_list

# Get enum of operator from the condition
def get_operator(condition):
    if condition.find("=") > 0:
        return 1
    if condition.find(">") > 0:
        return 2
    if condition.find("<") > 0:
        return 3
    if condition.find(">=") > 0:
        return 4
    if condition.find("<=") > 0:
        return 5

# Get index, length of the operator used in the index
def get_operator_index(condition):
    if condition.find(">=") > 0:
        return (condition.find(">="), 2)
    if condition.find("<=") > 0:
        return (condition.find("<="), 2)
    if condition.find("=") > 0:
        return (condition.find("="), 1)
    if condition.find(">") > 0:
        return (condition.find(">"), 1)
    if condition.find("<") > 0:
        return (condition.find("<"), 1)

# Extract column from table file
def extract_column_from_table(table_name, column_name):
    if table_name not in tables_info:
        error_and_exit(table_name + " does not exist")
    if column_name not in tables_info[table_name]:
        error_and_exit("column " + column_name + " does not exist in " + table_name)
    file_name = table_name + ".csv"
    values = []
    fields = tables_info[table_name].keys()
    with open(file_name, 'r') as table_file:
        reader = csv.DictReader(table_file, fields)
        for row in reader:
            values.append(row[column_name])

    return values

# Get entire table from the file
def get_table(table_name):
    if table_name not in tables_info:
        error_and_exit(table_name + " does not exist")
    file_name = table_name + ".csv"
    values = []
    try:
        with open(file_name, 'r') as table_file:
            reader = csv.reader(table_file)
            for row in reader:
                values.append([int(n) for n in row])
    except:
        error_and_exit(table_name + " does not exist")
    return values

# Get column names of the table
def get_column_names(table_name):
    if table_name not in tables_info:
        error_and_exit(table_name + " does not exist")
    return tables_info[table_name].keys()

# Check if column name is an aggregate function
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

# Get enum of aggregate function
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

# Get the argument of aggregate functions
def get_aggregate_function_argument(function):
    start_index = function.find("(") + 1
    end_index = function.find(")")

    return function[start_index: end_index].strip()

# Load data from all the tables requested by the user
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

# Filter loaded data based on WHERE conditions
def filter_temp_table(where_conditions, temp_table):
    modified_conditions_list = []
    for condition in where_conditions:
        if condition.strip().lower() == "or" or condition.strip().lower() == "and":
            modified_conditions_list.append(condition.lower())
            continue

        condition = condition.replace(" ", "")
        operator_index, length = get_operator_index(condition)
        arg_1 = condition[0:operator_index]
        arg_2 = condition[operator_index + length:]

        if not arg_1.isnumeric():
            if arg_1 not in temp_table[0]:
                error_and_exit("Where condition is incorrect")
            column_index = temp_table[0].index(arg_1)
            arg_1 = "temp_table_row[" + str(column_index) + "]"

        if not arg_2.isnumeric():
            if arg_2 not in temp_table[0]:
                error_and_exit("Where condition is incorrect")
            column_index = temp_table[0].index(arg_2)
            arg_2 = "temp_table_row[" + str(column_index) + "]"
        
        if condition[operator_index: operator_index + length].strip() == "=":
            condition = arg_1 + "==" + arg_2
        else:
            condition = arg_1 + condition[operator_index: operator_index + length] + arg_2
        
        modified_conditions_list.append(condition)

    filter_condition = " ".join(modified_conditions_list)
    temp_table[1] = [temp_table_row for temp_table_row in temp_table[1] if eval(filter_condition)]

# Make buckets of data, based on GROUP BY params
def make_buckets(group_by_params, temp_table):
    column_name = group_by_params[0]
    if column_name.strip() not in temp_table[0]:
        error_and_exit("Invalid group by conditions")
    
    buckets = {}
    index = temp_table[0].index(column_name)
    for row in temp_table[1]:
        value = row[index]
        if value not in buckets:
            buckets[value] = [row.copy()]
        else:
            buckets[value].append(row.copy())

    temp_table[1] = []
    for value in buckets:
        for row in buckets[value]:
            temp_table[1].append(row)
    return buckets

# Calculate aggregate value from the data, based on the passed aggregate function
def get_aggregate_value(agg_function, bucket, temp_table):
    arg = get_aggregate_function_argument(agg_function)
    if arg not in temp_table[0] and arg.strip() is not "*":
        error_and_exit("Invalid select parameters")
    
    if arg.strip() is not "*":
        index = temp_table[0].index(arg)
    else:
        index = -1
    
    # Sum
    if get_aggregate_function_index(agg_function) == 1:
        col_values = [row[index] for row in bucket]
        return sum(col_values)

    # average
    if get_aggregate_function_index(agg_function) == 2:
        col_values = [row[index] for row in bucket]
        return sum(col_values)/len(bucket)
    
    # Max
    if get_aggregate_function_index(agg_function) == 3:
        col_values = [row[index] for row in bucket]
        return max(col_values)

    # Min
    if get_aggregate_function_index(agg_function) == 4:
        col_values = [row[index] for row in bucket]
        return min(col_values)


    # Count
    if get_aggregate_function_index(agg_function) == 5:
        return len(bucket)

# Fill aggregate values in the selected data, if requested
def fill_aggregate_values(ans_table, buckets, temp_table, aggregates_list):
    i = 0
    for key in buckets:
        for agg_function in aggregates_list:
            ans_table[i][agg_function[1]] = get_aggregate_value(agg_function[0], buckets[key], temp_table)
        i = i + 1
    return

# Select columns requested by the user from the loaded data
def select_columns(select_params, buckets, temp_table, grouped):
    ans_table = []
    has_aggregates = False
    aggregates_list = []
    for key in buckets:
        for row in buckets[key]:
            ans_row = []
            i = 0
            for param in select_params:
                if is_aggregate_function(param):
                    ans_row.append("agg function")
                    has_aggregates = True
                    aggregates_list.append((param, i))
                elif param.strip() == "*":
                    ans_row = row.copy()
                else:
                    if param.strip() not in temp_table[0]:
                        error_and_exit("Select param is wrong")
                    ans_row.append(row[temp_table[0].index(param)])
                i = i + 1
            ans_table.append(ans_row)
            if grouped:
                break
    if has_aggregates:
        fill_aggregate_values(ans_table, buckets, temp_table, aggregates_list)
    
    temp_table[0] = select_params
    return ans_table

# Process DISTINCT keyword in the query
def process_distinct(query_tokens, temp_table):
    distinct_present = False
    for token in query_tokens:
        if token.ttype is Keyword and token.value.lower() == "distinct":
            distinct_present = True
            break

    if distinct_present:
        row_dict = {}
        copy = temp_table[1].copy()
        for row in temp_table[1]:
            if tuple(row) in row_dict:
                copy.remove(row)
            else:
                row_dict[tuple(row.copy())] = True
        temp_table[1] = copy
    return

# Order the data based on ORDER BY params
def process_order_by(order_by_params, temp_table):
    order_by_params = order_by_params[0]
    param = order_by_params.split()[0].strip()

    if len(order_by_params.split()) > 1:
        order = order_by_params.split()[1].strip()
    else:
        order = "asc"

    if param not in temp_table[0]:
        error_and_exit("Order by params incorrect")
    
    param_index = temp_table[0].index(param)
    if order.lower() == "asc":
        temp_table[1].sort(key = lambda x:x[param_index])
        return
    temp_table[1].sort(reverse=True, key = lambda x:x[param_index])
    return

# Print output to console
def print_output(temp_table, header):
    print(",".join(header))
    for row in temp_table[1]:
        print(",".join([str(num) for num in row]))

# Start query processing, taking input from console
def process_query():
    sql = sys.argv[1]
    if sql.strip()[-1] != ";":
        error_and_exit("Sql syntax error: missing semi-colon")

    a = sqlparse.parse(sql)[0].tokens

    grouped = False
    select_params = extract_select_params(a) # Get SELECT params
    if len(select_params) < 1:
        error_and_exit("Please specify select parameters")
    for item in select_params:
        if is_aggregate_function(item.strip()):
            grouped = True
    
    from_params = extract_from_params(a) # Get FROM params
    if len(from_params) < 1:
        error_and_exit("Please specify tables")
    
    tt = build_temp_table(from_params) # Store all the data from the tables requested
    temp_header = build_header(tt[0], tt) # Build header containing list of column names

    where_conditions = extract_where_conditions(a) # Get WHERE params
    if(len(where_conditions) > 0):
        filter_temp_table(where_conditions, tt) # Filter the data based on where conditions
    
    group_by_params = extract_group_by_params(a) # Get GROUP BY params
    buckets = None
    if len(group_by_params) > 0:
        buckets = make_buckets(group_by_params, tt) # make buckets of grouped data if GROUP BY params exists
        grouped = True
    else:
        buckets = {}
        buckets["a"] = tt[1] # Consider whole data as a single bucket, if GROUP BY params don't exists

    tt[1] = select_columns(select_params, buckets, tt, grouped) # Select requested columns from the data
    
    process_distinct(a, tt) # Process DISTINCT keyword
    
    order_by_params = extract_order_by_params(a) # Get ORDER BY params
    if len(order_by_params) > 0:
        process_order_by(order_by_params, tt) # Order the data based on ORDER BY params

    header = build_final_header(select_params, temp_header) # Build header based on the list of columns requested by the user
    print_output(tt, header) # Print Final output to the console

if __name__ == "__main__":
    init()
    process_query()
