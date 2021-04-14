#!/usr/bin/env python3

import os
import subprocess
import sys
import time

columns = ['Event', 'White', 'Black', 'Result', 'UTCDate', 'UTCTime', 'WhiteElo', 'BlackElo', 'WhiteRatingDiff', 'BlackRatingDiff', 'ECO', 'Opening', 'TimeControl', 'Termination', 'AN']
columns = [x.lower() for x in columns]
ag_options = {'count', 'avg', 'sum', 'min', 'max', 'stdev'}
EXIT_FAILURE = 1

def parse_command(cmd):

    limit = -1
    orderby_field = 0
    orderby_option = 0

    words = cmd.split(' ')
    if len(words) < 1: 
        print('Error parsing command: command must contain "SELECT"')
        return EXIT_FAILURE
    if words[-1][-1] == ';':        # if last word has semicolon at end (like regular SQL), drop it
        words[-1] = words[-1][:-1]

    ops = {'sum': False, 'count': False, 'min': False, 'max': False, 'avg': False, 'stdev': False}
    my_columns = set()

    ''' SELECT '''
    if words[0].lower() != 'select':
        print('Error parsing command: command must begin with "SELECT" keyword.')
        return EXIT_FAILURE

    word_index = 1
    select_set = set()
    while (word_index < len(words)):
        if words[word_index][-1] != ',':    # keep grabbing select keywords until no , 
            break
        select_set.add(words[word_index][:-1])
        word_index += 1
    if word_index >= len(words): 
        print('Error parsing command: command missing field after SELECT.')
        return EXIT_FAILURE
    select_set.add(words[word_index])
    word_index += 1

    ''' APPROVE SELECT FIELDS '''
    for item in select_set: 
        if 'sum' in item.lower(): 
            ops['sum'] = True
        elif 'count' in item.lower(): 
            ops['count'] = True
        elif 'min' in item.lower(): 
            ops['min'] = True
        elif 'max' in item.lower(): 
            ops['max'] = True
        elif 'avg' in item.lower(): 
            ops['avg'] = True
        elif 'stdev' in item.lower(): 
            ops['stdev'] = True
        else:
            item = item.lower()
            if '(' in item or ')' in item:
                print(f'Invalid operation "{item}". Operations are: SUM, COUNT, MIN, MAX, AVG and STDEV.')
                return EXIT_FAILURE
            elif item not in columns: 
                print(f'Invalid column "{item}"". Column options are: {columns}.')
                return EXIT_FAILURE
            my_columns.add(item)

    if word_index >= len(words): 
        if len(my_columns) != len(select_set):     # if len(cols) != len(selected words), must be some operations, which need "GROUP BY"
            print('Cannot have operations without GROUP BY to indicate grouping.')
            return EXIT_FAILURE
        else:
            return my_columns, ops, None, None
    
    while word_index < len(words):

        if word_index < len(words): 
            next1 = words[word_index]
            word_index += 1
        else: 
            print('Syntax error: Missing GROUP BY command.')
            return EXIT_FAILURE
            
        ''' HANDLE GROUP BY '''
        if next1.lower() == 'group': 
            if word_index < len(words): 
                next2 = words[word_index]
                word_index += 1
                if next2.lower() != 'by': 
                    print('Syntax error: GROUP must be followed by BY')
                    return EXIT_FAILURE
            else: 
                print('Syntax error: GROUP must be followed by BY.')
                return EXIT_FAILURE


            ''' APPROVE GROUP BY FIELD '''
            if word_index >= len(words): 
                print(f'GROUP BY must be followed by valid column name. Column options are: {columns}.')
                return EXIT_FAILURE
            groupby_field = words[word_index].lower()
            word_index += 1
            if groupby_field not in columns: 
                print(f'Invalid GROUP BY column {groupby_field}. Column options are: {columns}.')
                return EXIT_FAILURE

        elif next1.lower() == 'order': 
            if word_index < len(words): 
                next2 = words[word_index]
                word_index += 1
                if next2.lower() != 'by': 
                    print('Syntax error: ORDER must be followed by BY')
                    return EXIT_FAILURE
            else: 
                print('Syntax error: ORDER must be followed by BY.')
                return EXIT_FAILURE

            if word_index >= len(words): 
                print(f'ORDER BY must be followed by valid column name or aggregation command. Column options are: {columns}.')
                return EXIT_FAILURE
  
            orderby_field = words[word_index].lower()
            word_index += 1
            if orderby_field not in columns and orderby_field not in ag_options:    # have to fix this still to be specific to operation chosen
                print(f'ORDER BY must be followed by valid column name or aggregation command. Column options are: {columns}.')
                return EXIT_FAILURE
            if orderby_field in columns: 
                orderby_field = 0
            else: 
                orderby_field = 1

            if word_index < len(words): 
                if words[word_index].lower() == 'desc': 
                    orderby_option = 1
                    word_index += 1
                elif words[word_index].lower() == 'asc': 
                    orderby_option = 0
                    word_index += 1

        elif next1.lower() == 'limit':
            limitnum = ''
            if word_index < len(words):
                limitword = words[word_index]
                word_index += 1
            else: 
                print('Syntax error: Missing LIMIT number')
                return EXIT_FAILURE

            if limitword.isdigit(): 
                limit = int(limitword)
            else: 
                print('Syntax error: LIMIT field must be an int')

        else:
            print('SELECT command must be followed by GROUP BY, ORDER BY or LIMIT in our parser.')
            return EXIT_FAILURE



    if word_index < len(words):
        print(f'Unknown words at end of SQL command. Ignoring after {groupby_field}.')

    ''' RETURN PARAMETERS '''
    return my_columns, ops, groupby_field, orderby_field, orderby_option, limit

def verify_input_data(columns, opers, groupby): 
    if groupby == None:        # if no aggregation/group by, must be ok
        return True 
    else: 
        if columns != None and len(columns) > 0: 
            if len(columns) > 1: 
                return False        # if grouping, only allowed to use one column 
            if columns[0] != groupby: 
                return False        # if grouping, column must be equal to groupby field 
        return True

def call_map_reduce(syspath, columns, opers, groupby_field, orderby_field, orderby_option, limit):

    ''' CALL M/R WITH PARAMETERS '''
    for key in opers: 
        if opers[key]: 
            print(f'calling this M/R: {key}, group by {groupby_field}')
            if groupby_field in columns: 
                gindex = columns.index(groupby_field)
            else:
                print('Syntax error: group by field not valid column option.')
                return

            ''' hadoop arguments: 
              order by (field): 0 -> column (default), 1 -> count/aggregation command
              limit: -1 -> no limit, 0-# -> limit
              order by (direction): 0 -> ascending (default), 1 -> descending
            '''

            runstr = 'hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -files ' + key + 'Map.py,' + key + 'Reduce.py -input ' + syspath + '/chess_games.csv -output ' + syspath + '/temp2 -mapper "' + key + 'Map.py ' + str(gindex) + '" -reducer "' + key + 'Reduce.py ' + str(orderby_field) + ' ' + str(orderby_option) + ' ' + str(limit) + '" -numReduceTasks 1'
            print(f'running: {runstr}')
            subprocess.run([runstr], shell=True, stdout=subprocess.PIPE)

            ''' PRINT RESULTS '''
            print('printing results')
            outstr = 'hadoop fs -cat ' + syspath + '/temp2/part-00000'
            subprocess.run([outstr], shell=True)

            cleardirstr = 'hadoop fs -rm -r ' + syspath + '/temp2'
            subprocess.run([cleardirstr], shell=True)

if __name__ == '__main__': 

    if len(sys.argv) != 2: 
        print('To run the SQL command interface shell, run "./sql_shell [path]" with the path of where hadoop can access on disc01 (eg. /users/lbakke)')

    syspath = sys.argv[1]

    help_str = '''Welcome to our SQL command interface. Type your command as so to begin:

    SELECT [col_name, sum(*), count(*), min(*), max(*), avg(*), stdev(*)], * 
    [GROUP BY] [col_name];

    eg. SELECT event, count(*), min(*) GROUP BY event;

Recall in SQL any columns selected with aggregation commands must match the GROUP BY field.

Type 'exit' to quit or 'help' to hear the instructions again.
    '''

    print(help_str)
    command = input('SQL > ')
    while ('exit' not in command): 
        if ('help' in command): 
            print(help_str)
        else: 
            result = parse_command(command)
            if result != EXIT_FAILURE: 
                start = time.time()
                cols, ops, groupby_field, orderby_field, orderby_option, limit = list(result[0]), result[1], result[2], int(result[3]), int(result[4]), int(result[5])
                if verify_input_data(cols, ops, groupby_field):
                    call_map_reduce(syspath, cols, ops, groupby_field, orderby_field, orderby_option, limit)
                    end = time.time()
                    print(f'Run time: {end - start} seconds.')
                else: 
                    print(f'Invalid column selection, any columns displayed must match group by field "{group_by}".')
        command = input('SQL > ')




''' order by -> only two options are the group by column, or the count/etc column 


things to add: LIMIT, ORDER BY 

ORDER BY: 0 -> nothing or column, 1 -> count
LIMIT: -1 -> no limit, 0-? -> limit # 
ORDER BY: 0 -> asc or nothing   , 1 -> desc 

'''
