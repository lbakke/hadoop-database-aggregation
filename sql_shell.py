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
    selectcol = ''
    aggrcommand = ''
    aggrcol = ''

    if len(words) > word_index:
        firstselect = words[word_index][:-1]
        if '(' in firstselect: 
            pieces = firstselect.split('(')
            aggrcommand = pieces[0].lower()
            if ')' in pieces[1]: 
                if aggrcommand != 'count': 
                    pieces2 = pieces[1].split(')')
                    aggrcol = pieces2[0].lower()
            else: 
                print('Error: invalid select aggregation command formatting')
                return EXIT_FAILURE;
        else:
            selectcol = firstselect.lower()
    else:  
        print('Error parsing command: missing field after SELECT.')
        return EXIT_FAILURE
    word_index += 1
    if len(words) > word_index:
        secondselect = words[word_index]
        if '(' in secondselect: 
            pieces = secondselect.split('(')
            aggrcommand = pieces[0]
            if ')' in pieces[1]: 
                if aggrcommand != 'count':
                    pieces2 = pieces[1].split(')')
                    aggrcol = pieces2[0].lower()
            else: 
                print('Error: invalid select aggregation command formatting')
                return EXIT_FAILURE;
        else:
            selectcol = firstselect.lower()
    else: 
        print('Error parsing command: must have both column and aggregation command after SELECT.')
    word_index += 1   

    ''' APPROVE SELECT FIELDS '''
    if word_index >= len(words): 
        print('Cannot have operations without GROUP BY to indicate grouping.')
        return EXIT_FAILURE
    
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
    return selectcol, aggrcol, aggrcommand, groupby_field, orderby_field, orderby_option, limit

def verify_input_data(selectcol, aggrcol, aggrcommand, groupby):
    print(f'SELECT COL: {selectcol}')
    print(f'GROUP BY: {groupby}')
    if selectcol.lower() != groupby.lower():      # column selected must equal group by field 
        return False
    else: 
        return True 

def call_map_reduce(syspath, datafile, reducers, selectcol, aggrcol, aggrcommand, groupby_field, orderby_field, orderby_option, limit):

    ''' CALL M/R WITH PARAMETERS '''
    print(f'calling this M/R: {aggrcol}, group by {groupby_field}')
    if groupby_field in columns: 
        gindex = columns.index(groupby_field)
    else:
        print('Syntax error: group by field not valid column option.')
        return EXIT_FAILURE

    if aggrcol != '': 
        print(aggrcol)
        if aggrcol in columns: 
            aindex = columns.index(aggrcol)
        else: 
            print('Syntax error: aggregation field not valid column option.')
            return EXIT_FAILURE

    ''' hadoop arguments: 
          order by (field): 0 -> column (default), 1 -> count/aggregation command
          limit: -1 -> no limit, 0-# -> limit
          order by (direction): 0 -> ascending (default), 1 -> descending
    '''
    runstr = ''
    if aggrcommand == 'count': 
        runstr = 'hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -files ' + aggrcommand + 'Map.py,' + aggrcommand + 'Reduce.py -input ' + syspath + '/' + datafile + ' -output ' + syspath + '/temp2 -mapper "' + aggrcommand + 'Map.py ' + str(gindex) + '" -reducer "' + aggrcommand + 'Reduce.py ' + str(orderby_field) + ' ' + str(orderby_option) + ' ' + str(limit) + '" -numReduceTasks 4'
        secondstr = 'hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -files ' + 'consSumMap.py,consSumReduce.py -input ' + syspath + '/temp2 -output ' + syspath + '/temp3 -mapper "' + 'consSumMap.py' + '" -reducer "consSumReduce.py ' + str(orderby_field) + ' ' + str(orderby_option) + ' ' + str(limit) + '" -numReduceTasks 1'
 


    elif aggrcommand == 'avg': 
        runstr = 'hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -files ' + aggrcommand + 'Map.py,' + aggrcommand + 'Reduce.py -input ' + syspath + '/' + datafile + ' -output ' + syspath + '/temp2 -mapper "' + aggrcommand + 'Map.py ' + str(gindex) + ' ' + str(aindex) + '" -reducer "' + aggrcommand + 'Reduce.py" -numReduceTasks 4'
        secondstr = 'hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -files ' + 'consAvgMap.py,consAvgReduce.py -input ' + syspath + '/temp2 -output ' + syspath + '/temp3 -mapper "' + 'consAvgMap.py' + '" -reducer "consAvgReduce.py ' + str(orderby_field) + ' ' + str(orderby_option) + ' ' + str(limit) + '" -numReduceTasks 1'
 

        
    elif aggrcommand == 'min': 
        runstr = 'hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -files ' + 'minmaxMap.py,minmaxReduce.py -input ' + syspath + '/' + datafile + ' -output ' + syspath + '/temp2 -mapper "' + 'minmaxMap.py ' + str(gindex) + ' ' + str(aindex) + '" -reducer "' + 'minmaxReduce.py" -numReduceTasks 4'
        secondstr = 'hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -files ' + 'consMinMaxMap.py,consMinMaxReduce.py -input ' + syspath + '/temp2 -output ' + syspath + '/temp3 -mapper "' + 'consMinMaxMap.py' + '" -reducer "consMinMaxReduce.py 0 ' + str(orderby_field) + ' ' + str(orderby_option) + ' ' + str(limit) + '" -numReduceTasks 1'

 

    elif aggrcommand == 'max': 
        runstr = 'hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -files ' + 'minmaxMap.py,minmaxReduce.py -input ' + syspath + '/' + datafile + ' -output ' + syspath + '/temp2 -mapper "' + 'minmaxMap.py ' + str(gindex) + ' ' + str(aindex) + '" -reducer "' + 'minmaxReduce.py" -numReduceTasks 4'
        secondstr = 'hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -files ' + 'consMinMaxMap.py,consMinMaxReduce.py -input ' + syspath + '/temp2 -output ' + syspath + '/temp3 -mapper "' + 'consMinMaxMap.py' + '" -reducer "consMinMaxReduce.py 1 ' + str(orderby_field) + ' ' + str(orderby_option) + ' ' + str(limit) + '" -numReduceTasks 1'


 
    elif aggrcommand == 'sum': 
        runstr = 'hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -files ' + aggrcommand + 'Map.py,' + aggrcommand + 'Reduce.py -input ' + syspath + '/' + datafile + ' -output ' + syspath + '/temp2 -mapper "' + aggrcommand + 'Map.py ' + str(gindex) + ' ' + str(aindex) + '" -reducer "' + aggrcommand + 'Reduce.py" -numReduceTasks 4'
        secondstr = 'hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -files ' + 'consSumMap.py,consSumReduce.py -input ' + syspath + '/temp2 -output ' + syspath + '/temp3 -mapper "' + 'consSumMap.py' + '" -reducer "consSumReduce.py ' + str(orderby_field) + ' ' + str(orderby_option) + ' ' + str(limit) + '" -numReduceTasks 1'


 
    elif aggrcommand == 'stdev':
        runstr = 'hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -files ' + aggrcommand + 'Map.py,' + aggrcommand + 'Reduce.py -input ' + syspath + '/' + datafile + ' -output ' + syspath + '/temp2 -mapper "' + aggrcommand + 'Map.py ' + str(gindex) + ' ' + str(aindex) + '" -reducer "' + aggrcommand + 'Reduce.py" -numReduceTasks 4'
        secondstr = 'hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -files ' + 'consAvgMap.py,consAvgReduce.py -input ' + syspath + '/temp2 -output ' + syspath + '/temp3 -mapper "' + 'consAvgMap.py' + '" -reducer "consAvgReduce.py ' + str(orderby_field) + ' ' + str(orderby_option) + ' ' + str(limit) + '" -numReduceTasks 1'
 
    else: 
        print(f'Invalid aggregation command given. Options are {ag_options}')
    print(f'running: {runstr}')
    subprocess.run([runstr], shell=True, stdout=subprocess.PIPE)
  
    print(f'running: {secondstr}')
    print(secondstr)
    subprocess.run([secondstr], shell=True, stdout=subprocess.PIPE)
       

    ''' PRINT RESULTS '''
    print('printing results')
    outstr = 'hadoop fs -cat ' + syspath + '/temp3/part-00000'
    subprocess.run([outstr], shell=True)

    cleardirstr = 'hadoop fs -rm -r ' + syspath + '/temp3'
    subprocess.run([cleardirstr], shell=True)

    cleardirstr = 'hadoop fs -rm -r ' + syspath + '/temp2'
    subprocess.run([cleardirstr], shell=True)

if __name__ == '__main__': 

    if len(sys.argv) < 4: 
        print('Error: To run the SQL command interface shell, run "./sql_shell [path] [data_file] [# reducers]" with path being the path of where hadoop can access on disc01 (eg. /users/lbakke), data_file being the relative data file path, and # reducers being the intended number of reducers.')
        sys.exit(1)

    syspath = sys.argv[1]
    datafile = sys.argv[2]
    reducers = sys.argv[3]

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
                selectcol, aggrcol, aggrcommand, groupby_field, orderby_field, orderby_option, limit = result[0], result[1], result[2], result[3], int(result[4]), int(result[5]), int(result[6])
                if verify_input_data(selectcol, aggrcol, aggrcommand, groupby_field):
                    call_map_reduce(syspath, datafile, reducers, selectcol, aggrcol, aggrcommand, groupby_field, orderby_field, orderby_option, limit)
                    end = time.time()
                    print(f'Run time: {end - start} seconds.')
                else: 
                    print(f'Invalid column selection, any columns displayed must match group by field "{groupby_field}".')
        command = input('SQL > ')




''' order by -> only two options are the group by column, or the count/etc column 


things to add: LIMIT, ORDER BY 

ORDER BY: 0 -> nothing or column, 1 -> count
LIMIT: -1 -> no limit, 0-? -> limit # 
ORDER BY: 0 -> asc or nothing   , 1 -> desc 

'''
