# -*- coding: utf-8 -*-
import re
import sys
import time

'''
    Converts statistics file (like december_stat2) into .csv format.
    
    # argv[1] - name of the statistics file
    # argv[2] - name of output file
    # argv[3] - name of the cluster (which statistics file is taken from)
    Example usage:
        python parse.py ./december_stat2 ./result_statistics.csv bluegene
'''

def make_output_pattern(term):
    string_terms = ["UserID", "Status", "Class", "Partition Allocated", "Cluster", "Other"]
    # makes strings appear in "s, and numbers - without "s.
    if term in string_terms:
        return '"%(' + term + ')s"'
    return '%(' + term + ')s'

def main(argv=None):
    if argv == None:
        argv = sys.argv

    # argv[1] - name of the statistics file
    fi = open(argv[1], 'r')
    data = fi.read()
    fi.close()

    # argv[2] - name of output file
    result = open(argv[2], 'w')

    # argv[3] - name of the cluster (which statistics file is taken from)
    cluster = argv[3]

    pattern = '=' * 18 + ' Job .*? ' + '=' * 18
    jobs = re.split(pattern, data)

    # header of resulting .csv file
    headers = ["JobID", "UserID", "Queue Date", "Dispatch Time", "Start Time", "Completion Date",\
             "Size Requested", "Status", "Partition Allocated", "Class", "Time limit (seconds)",\
             "Cluster", "Other"]
    result.write('\t'.join(map(lambda t: '"' + t + '"', headers)) + '\n')

    output_pattern = '\t'.join(map(lambda t: make_output_pattern(t), headers)) + '\n'

        
    for job in jobs[1:]:
        lines = job.split('\n')
        
        # some invalid job
        if len(lines) < 3: 
            continue
            
        # store the information in info
        info = dict()
        other_list = []
        
        for line in lines:
            # possibly empty after splitting
            if line == '':
                continue
                

            # otherwise there is a "key: value"
            parts = list(map(lambda x: x.strip(), line.split(':')))

            info[parts[0]] = parts[1]

            # is it unlisted property?
            is_other = parts[0] not in headers

            # store the information about cluster (same for all entries)
            info["Cluster"] = cluster
            
            # convert human-readable date-times to Unix time
            if parts[0].endswith('Time') or parts[0].endswith('Date'):
                s = parts[1] + ':' + parts[2] + ':' + parts[3]
                stm = time.strptime(s)
                info[parts[0]] = int(time.mktime(stm))
                is_other = False
                
            # extract seconds from 'Wall Clk Soft Limit'
            if parts[0].startswith('Wall Clk'):
                begin = parts[-1].find('(') + 1
                end = parts[-1].find(' ', begin)
                info['Time limit (seconds)'] = parts[-1][begin:end]
                is_other = False
            
            # extract JobID (number) from string
            if parts[0].startswith('Job Id'):
                info['JobID'] = parts[1].split('.')[-1]
                is_other = False
            
            # rename 'Submitting Userid' to 'UserID' 
            if parts[0].startswith('Submitting Userid'):
                info['UserID'] = parts[1]
                is_other = False

            if is_other:
                other_list.append((parts[0], parts[1]))

        info['Other'] = ';'.join(map(lambda s: '%s=%s' % (s[0], s[1]), other_list))

        # sometimes not all columns are presented
        try:
            result.write(output_pattern % info)
        except:
            continue


    result.close()
    return 0

if __name__ == "__main__":
    sys.exit(main())
