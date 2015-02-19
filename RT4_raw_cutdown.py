## RT_raw_cutdown.py
#
#  read in an RT export file, and write out a compact,
#   one-record-per-line RT file with limited fields
#  takes "standard" RT dump
#
#  useful for doing analysis over entire dataset
#
# generates 185 errors for records with wrong number of elements  (!= 84)
# most of these are jobs records
#
##########################################################################
# http://docs.python.org/2/library/csv.html
# http://docs.python.org/2/library/datetime.html#strftime-and-strptime-behavior

import csv, time
from datetime import date, datetime

nowstring = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
CURRENT_YEAR = int(datetime.now().strftime('%Y') )
CURRENT_WEEK = int(datetime.now().strftime('%U') )


#######
#DIR = '../data/2014-09-20/'
#infile = '2014-07-21_dump-Results.tsv'
#infile = DIR + '2014-09-20_dump-Results.tsv'


DIR = '/u/if/bhecker/python/RT/data/2014-12-08/'
infile = DIR + '2014-12-08_RT_dump_results.tsv'

outfile = DIR + 'RT-' + nowstring + "_all_manual.csv"



###############################################################################

#field key
k = {}

## k['id'] = 0
## k['Queue'] = 1
## k['Subject'] = 2
## k['Status'] = 3
## k['Owner'] = 9
## k['Created'] = 15
## k['Resolved'] = 16
## k['LastUpdated'] = 17

k['id'] = 0
k['Subject'] = 1
k['Status'] = 2
k['Owner'] = 3
k['Organization'] = 4
k['Requestors'] = 5
k['Queue'] = 6
k['Created'] = 7
k['Resolved'] = 8
k['LasatUpdatedBy'] = 9
k['LastUpdated'] = 10
RecordsProcessed = 0
badRecords = 0
writtenRecords = 0

###########################################
# open output file
OUTX =  open(outfile, 'wb') 
csvwriter = csv.writer(OUTX)
# print title, then label row 
csvwriter.writerow(["Complete record dump " + datetime.now().strftime('%Y-%m-%d')])

csvwriter.writerow(['id', 'queue','status','owner','created yr-wk', 'resolved yr-wk','last update','length of row'])

################################# 
# generate output file

# was 'rb'
with open(infile, 'Ub') as csvfile:
    try:
        datareader = csv.reader(csvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
    except csv.Error as e:
        sys.exit('file %s, line %d: %s' % (infile, datareader.line_num, e))
        print "ERROR!", "Current row is:"
        print datareader

    for row in datareader:
        ##was 83 until 2014-04-30, when dropping ancient queues from the RT query resulted in
        ##  dropping of many bogus fields.  Now set to == 24
        if len(row) == 11:
            if RecordsProcessed != 0:
                ## tally event ticket in appropriate year-week bins
                # x = time.strptime('2006-07-30 17:38:35','%Y-%m-%d %H:%M:%S')
                dcreate = time.strptime(row[k['Created']], '%Y-%m-%d %H:%M:%S')
                createWeek = int(time.strftime('%U',dcreate))
                if row[k['Resolved']] != '' and row[k['Resolved']] != 'Not set':
                    dresolve = time.strptime(row[k['Resolved']], '%Y-%m-%d %H:%M:%S')
                    resolveWeek = int(time.strftime('%U',dresolve))
                else:
                    dresolve = time.strptime('2525-12-31 01:01:01', '%Y-%m-%d %H:%M:%S')
                    resolveWeek = 54

                csvwriter.writerow([ row[k['id']],row[k['Queue']], row[k['Status']], row[k['Owner']], time.strftime('%Y-%U',dcreate), time.strftime('%Y-%U',dresolve),row[k['LastUpdated']],len(row) ])
                writtenRecords += 1
        else:
            badRecords += 1
            print "# %s error, %s items:  " % (badRecords,len(row)), row[k['id']]+":"+row[k['Subject']]

        RecordsProcessed += 1
OUTX.close() 

print "Records processed = %s, written records = %s, bad records = %s" % (RecordsProcessed, writtenRecords, badRecords)
print "Started:", nowstring 
print "Finished:", datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
 
