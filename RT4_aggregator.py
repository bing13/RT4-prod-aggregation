## RT4_aggregator.py
#  created 2014-05-09 [returning from FNAL]
#  big update 2014-09-22 to move to RT4
#
#  input: TSV RT dump of modern queues
#  output: CSV with reconstructed queue bin size for each week, by queue
#     yyyy-ww, queue1 size, queue2 size, queue3 size, etc
#  suitable for reading into excel.

##########################################################################
# http://docs.python.org/2/library/csv.html
# http://docs.python.org/2/library/datetime.html#strftime-and-strptime-behavior

import csv, time
from datetime import date, datetime

nowstring = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
CURRENT_YEAR = int(datetime.now().strftime('%Y') )
CURRENT_WEEK = int(datetime.now().strftime('%U') )

#infile = 'data/2014-05-12/2014-05-12_Results.tsv'
#infile = 'data/2014-06-05/2014-06-05_dump-Results.tsv'
#infile = 'data/2014-07-21/2014-07-21_dump-Results.tsv'
#infile = 'data/2014-08-28/2014-08-28_dump-Results.tsv'

DIR = '~/python/RT/data/2014-12-08/'
DIR = '/u/if/bhecker/python/RT/data/2014-12-08/'       

#infile = DIR + '20140922_short_test.tsv'
#infile = DIR + '2014-09-20_dump-Results.tsv'
infile = DIR + '2014-12-08_RT_dump_results.tsv'
outfile = DIR + 'RT-' + nowstring + "_all_bins.csv"

print "Infile=",infile
print "Outfile=",outfile


QUEUES = ['INSPIRE', 'EXP', 'General', 'Inspire-additions - DELETE', \
          'CitationLoss','Data_Submission',  \
          'INST', 'INST_add_cor', 'INST_add+cor_user', 'INST_add+cor', \
          'CONF', 'CONF_add+cor', 'CONF_cor_user', 'CONF_add_user',\
          'HEP_ref_user', 'HEP_add_user','HEP', 'HEP_ref', \
          'HEPNAMES', \
          'Authors', 'AUTHORS_general', 'AUTHORS_claim_manual', 'AUTHORS_xml', \
          'AUTHORS_long_list', 'AUTHORS_claim_sso','AUTHORS_cor_user',\
          'Feedback', 'HEP_curation', \
          'Inspire-References','JOBS',\
          'HEP', 'HEP_cor_user', 'HEP_ref_user', \
          'Usability-Testing', 'Labs_Feedback' \
          ]

QUEUES.sort()

#####
# epoch = date after which to start outputing records. 2013-17 is the modern era in RT management
#
CURRENT_EPOCH = True
CURRENT_EPOCH_BEGINS = '2013-17'   ## REMEMBER, it's a week, not a date

PLOTTITLE = "tickets by week"

#
#######################################################

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

# set up the weekly bin accumulator array

ywbin = {}

for year in range(2006, CURRENT_YEAR+1):
    ywbin[str(year)] = {}
    for week in range(0, 54):
        ywbin[str(year)][str(week)] = {}
        for queue in QUEUES:
            ywbin[str(year)][str(week)][queue] = 0


print "All queues processing  <=================="

RecordsProcessed = 0
count = 0
badRecords = 0
#with open(infile, 'rb') as csvfile
# rU = universal newline mode:

with open(infile, 'rU') as csvfile:
    try:
        datareader = csv.reader(csvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
    except csv.Error as e:
        sys.exit('file %s, line %d: %s' % (infile, datareader.line_num, e))
        print "ERROR!", "Current row is:"
        print datareader
        
    for row in datareader:
        # set to count == 0 to see the header row
        if count == -1:
            print ', '.join(row)
        count += 1
        if row[k['id']] == '259245xxx': print row

        ## tally event ticket in appropriate year-week bins
        # x = time.strptime('2006-07-30 17:38:35','%Y-%m-%d %H:%M:%S')
        
        # reduced 83 => 24 on 2014-04-30
        # now assumes it's fed only current queues, no SLAC cruft
        # remember to switch RT pref to ISO time!
        if len(row) == 11 and 'Requestors' not in row:
            
            dcreate = time.strptime(row[k['Created']], '%Y-%m-%d %H:%M:%S')
            createWeek = int(time.strftime('%U',dcreate))
            if (row[k['Resolved']] != '') and (row[k['Resolved']] != 'Not set'):
                dresolve = time.strptime(row[k['Resolved']], '%Y-%m-%d %H:%M:%S')
                resolveWeek = int(time.strftime('%U',dresolve))
            else:
                dresolve = time.strptime('2525-12-31 01:01:01', '%Y-%m-%d %H:%M:%S')
                resolveWeek = 54

            #############################################
            # compare dcreate and dresolve to each bin
            for yx in range(dcreate.tm_year,CURRENT_YEAR+1):
                for wx in range(0,53):

                    ##note, some wastage here, starting in Jan of dcreate year
                    #if (dcreate.tm_year <= yx and createWeek <= wx ) and \
                    #(dresolve.tm_year >= yx and resolveWeek > wx ):

                    ## must always compare the year-week, not just the week, or use the datetime
                    ## so ... calculate date in months
                    binW = (yx*52)+wx

                    dCreateW = (dcreate.tm_year*52)+createWeek
                    dResolveW= (dresolve.tm_year*52)+resolveWeek
                    ## ORIGINAL:  dCreateW <= binW and dResolveW > binW
                    if (dCreateW <= binW and dResolveW >= binW):
                        ywbin[str(yx)][str(wx)][ row[k['Queue']] ] += 1
        else:
            badRecords += 1
            print "BAD RECORD: %s  # %s error, %s items:  " % (row[k['Queue']], badRecords,len(row)), row[k['id']]+":"+row[k['Subject']]

        RecordsProcessed += 1

OUTX =  open(outfile, 'wb') 
csvwriter = csv.writer(OUTX)

# print title, then label row 
csvwriter.writerow([PLOTTITLE + datetime.now().strftime('%Y-%m-%d')])
csvwriter.writerow(['year-week']+QUEUES )


for yx in range(2006,CURRENT_YEAR+1):
    for wx in range(0,53):

        padded_date = '{0}-{1:02d}'.format(yx, wx)
        
        # next logic prevents totaling data from weeks before the EPOCH starts
        if (CURRENT_EPOCH and (padded_date >= CURRENT_EPOCH_BEGINS) ) or not CURRENT_EPOCH:
            # this prevents writing bins for future weeks
            # sometimes this creates a null final week which duplicates the
            #   previous week's count, but preventing it would prevent you
            #   from running a report on friday for the week, etc
            if not (yx == CURRENT_YEAR and wx > CURRENT_WEEK):
                totalsForThisWeek = [padded_date]
                for qx in QUEUES:
                    totalsForThisWeek.append(ywbin[str(yx)][str(wx)][ qx ])
                csvwriter.writerow( totalsForThisWeek )

OUTX.close() 

print "ALL QUEUES PROCESSED."
print "Records processed:", RecordsProcessed,"Bad records:", badRecords
print "Started:", nowstring 
print "Finished:", datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
 
