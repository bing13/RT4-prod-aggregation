## RT_series_combo.py
#
#  combined output for creation of graphs that display
#  new and resolved tickets over time
#
#  created 2014-05-09 [returning from FNAL]
# 
#  takes "standard" RT dump, and returns outputfile that has only:
#    ID, status, created date, resolved date, and last modified date
#  suitable for loading into excel
##########################################################################

# http://docs.python.org/2/library/csv.html
# http://docs.python.org/2/library/datetime.html#strftime-and-strptime-behavior


#
# TO MAKE USE OF THE OUTPUT IN EXCEL FOR GRAPHING....
#  select from cell B2 to the enter, then insert graph
#  then deselect unwanted bars, either from graph widget or "select data"
#  


import csv, time
from datetime import date, datetime

nowstring = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

######
#DIR = '../data/2014-09-20/'
DIR = '/u/if/bhecker/python/RT/data/2014-12-08/'

#infile = DIR + '2014-09-20_dump-Results.tsv'
#infile = DIR + '20140922_short_test.tsv'
#infile = DIR + '2014-09-20_dump-Results.tsv'
infile = DIR + '2014-12-08_RT_dump_results.tsv'
outfile = DIR+ 'RT-' + nowstring + "_series_combined.csv"

QUEUES = ['INSPIRE', 'EXP', 'General', 'Inspire-additions - DELETE', \
          'CitationLoss', 'Data_Submission',\
          'INST', 'INST_add_cor', 'INST_add+cor_user', 'INST_add+cor', \
          'CONF', 'CONF_add+cor', 'CONF_cor_user', 'CONF_add_user',\
          'HEP_ref_user', 'HEP_add_user','HEP', 'HEP_ref', \
          'HEP', 'HEP_cor_user', 'HEP_ref_user', \
          'HEPNAMES', \
          'Authors', 'AUTHORS_general', 'AUTHORS_claim_manual', 'AUTHORS_xml', \
          'AUTHORS_long_list', 'AUTHORS_claim_sso','AUTHORS_cor_user',\
          'Feedback', 'HEP_curation', \
          'Inspire-References','JOBS',\
          'Usability-Testing', 'Labs_Feedback' \
          ]

QUEUES.sort()

CURRENT_YEAR = int( datetime.now().strftime( '%Y' ) )
CURRENT_EPOCH = True
CURRENT_EPOCH_BEGINS = '2013-17'   ## REMEMBER, it's a week, not a date


#######################################################

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


yearWeekAccumulator = {}
RecordsProcessed = 0
badRecords = 0
count = 0
year_wk_list = []

#######################################
## set up yearWeekAccumulator
# data structure: queue-created|-resolved[[year][Week]]
#####

for qx in QUEUES:
    yearWeekAccumulator[ qx + '-created' ]  = {}
    yearWeekAccumulator[ qx + '-resolved' ] = {}

    #yearWeekAccumulator[ qx + '-created' ][ year_wk ]  = 0
    yearWeekAccumulator[ qx + '-resolved' ][ 'unresolved' ]  = 0
    
    for year in range(2006, CURRENT_YEAR+1):
        for week in range(0, 54):
            year_wk = str(year)+ '-' + str(week).zfill(2)

            yearWeekAccumulator[ qx + '-created' ][ year_wk ]  = 0
            yearWeekAccumulator[ qx + '-resolved' ][ year_wk ] = 0


## build a list of weeks that we're interested in, i.e.,
##  none before CURRENT_EPOCH_BEGINS and including "unresolved" week
            
for wx in yearWeekAccumulator['HEP_curation-created'].keys():
    if (CURRENT_EPOCH and ( wx >= CURRENT_EPOCH_BEGINS) ) or not CURRENT_EPOCH:
        year_wk_list.append( wx )
year_wk_list.append( 'unresolved' )
year_wk_list.sort()

#############################

##was 'rb'
with open(infile, 'rU') as csvfile:
    try:
        datareader = csv.reader(csvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
    except csv.Error as e:
        sys.exit('file %s, line %d: %s' % (infile, datareader.line_num, e))
        print "ERROR!", "Current row is:"
        print datareader
        
    for row in datareader:
        if count == -1:
            print ', '.join(row)
        count += 1

        ## tally event ticket in appropriate year-week bins
        ## was 83 until 2014-04-30, then 24. B/c ancient crufty queues were eliminated
        ## from RT dump generation.  This dropped many bogus fields
        if len(row) == 11:
            if 'LastUpdated' not in row: 

                dcreate = time.strptime(row[k['Created']], '%Y-%m-%d %H:%M:%S')
                create_yearWeek = time.strftime('%Y-%U', dcreate)
                if row[k['Resolved']] != '' and row[k['Resolved']] != 'Not set':
                    dresolve = time.strptime(row[k['Resolved']], '%Y-%m-%d %H:%M:%S')
                    resolve_yearWeek = time.strftime('%Y-%U', dresolve)
                else:
                    dresolve = ''
                    resolve_yearWeek = 'unresolved'

                yearWeekAccumulator[ row[k['Queue']] + '-created'  ][ create_yearWeek ]  += 1
                yearWeekAccumulator[ row[k['Queue']] + '-resolved' ][ resolve_yearWeek ] += 1
            
        else:
            badRecords += 1
            print "# %s error, %s items:  " % (badRecords,len(row)), row[k['id']]+":"+row[k['Subject']] 
        RecordsProcessed += 1


qywa_keys = yearWeekAccumulator.keys()
qywa_keys.sort()

OUTX =  open(outfile, 'wb') 
csvwriter = csv.writer(OUTX) 

# print title row, then label row
csvwriter.writerow( ["created & resolved by week. " + datetime.now().strftime('%Y-%m-%d')])
csvwriter.writerow( ['', '','week'])
csvwriter.writerow( ['', ''] + year_wk_list )

firstQueue = True

print "beginning write prep....................."

for qx in qywa_keys:
    if firstQueue:
        col1 = 'queue'
        firstQueue = False
    else:
        col1 = ''



    print "writing ", qx, '.........................'

    #for yw in year_wk_list:
    
    ## don't try to write "resolved" column for "created" row
    result_list = []                
    for yw in year_wk_list:
        ## generate list of dict values
        
        if  not ( ('created' in qx) and (yw == 'unresolved') ):
            result_list.append( yearWeekAccumulator[qx][yw] )
    csvwriter.writerow([ col1, qx ] + result_list )


OUTX.close()

print "Done <================================"
print "Records processed:", RecordsProcessed,"Bad records:", badRecords
print "Started:", nowstring 
print "Finished:", datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

