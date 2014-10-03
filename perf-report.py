import sys, getopt
import MySQLdb
import pandas.io.sql as psql
import numpy as np
import pandas as pd

def main(argv):
        # variables
        startDate = '2013-01-01'
        endDate = '2013-01-15'
        outputfile = 'availability.out'

        # get command line args
        try:
                opts, args = getopt.getopt(argv, "hs:e:f:",["help", "start=", "end=", "file="])
        except getopt.GetoptError:
                print 'perf-report.py --start <yyyy-mm-dd> --end <yyyy-mm-dd> --file <output-filename>'
                sys.exit(2)

        for opt, arg in opts:
                if opt in ("-h", "--help"):
                        print 'perf-report.py --start <yyyy-mm-dd> --end <yyyy-mm-dd> --file <output-filename>'
                        sys.exit()
                elif opt in ("-s", "--start"):
                        startDate = arg
                elif opt in ("-e", "--end"):
                        endDate = arg
                elif opt in ("-f", "--file"):
                        outputfile = arg

        # get the data from the database
        mysql_cn = MySQLdb.connect(host='<db server name>', port=3306, user='<db username>', passwd='<db password>', db='<db name>')
        df = psql.frame_query("SELECT IpSlaResults.OperID, IpSlaResults.OperTime, IpSlaResults.Rtt FROM IpSlaResults WHERE OperTime >= '" + startDate  +  " 00:00:00' AND OperTime < '" + endDate  +  " 00:00:00';", con=mysql_cn)
        mysql_cn.close()

        # find the baseline response times per OperID (level=0)
        # create a baseline dataframe containing the mean value of each OperID
        # this prevents losing OperID's when all of the measurements return zero for the period
        baseline = pd.DataFrame(df.groupby(['OperID']).agg(np.mean)['Rtt'], columns=['Rtt'])
        # set the indexes
        df.set_index(['OperID', 'OperTime'], inplace=True)
        # replace with actual baseline values
        baseline['Rtt'] = df[df.Rtt != 0].min(level=0)
        # replace the missing baselines with 99 (so it does not show as degraded)
        baseline.fillna(99, inplace=True)

        # remove the indexes for a bit
        baseline.reset_index(inplace=True)
        df.reset_index(inplace=True)

        # merge the baselines into the main dataframe
        df = df.merge(baseline, on=['OperID'], suffixes=['', '_baseline'])

        # bookend each Tag with a non-zero measurement to avoid issues
        # with the first of last measurement being a zero

        # get a list of all unique Tags
        ender = baseline.copy(deep=True)
        # add an OperTime at the ends of the window
        baseline['OperTime'] = startDate + ' 00:00:00'
        ender['OperTime'] = endDate + ' 00:00:00'

        # add a non-zero start and end record for each Tag so that we do not have issues
        # with calculating proper outage windows
        df = df.append(baseline, ignore_index=True).append(ender, ignore_index=True)

        # don't need the baseline dataframe anymore, delete it
        del baseline
        del ender

        # sort the data
        df.sort(columns=['OperID', 'OperTime'], inplace=True)

        # add column showing the delta from baseline
        df['delta'] = df['Rtt'] - df['Rtt_baseline']

        # delete columns that are no longer required
        df.drop(['Rtt', 'Rtt_baseline'], 1, inplace=True)

        # anything less than 0 should be a timeout, set to -1
        df['delta'] = np.where(df['delta'] < 0, -1, df['delta'])

        # bin the data into levels of degradation
        df['exceed1'] = (df['delta'] >= 100)
        df['exceed2'] = (df['delta'] >= 200)
        df['exceed3'] = (df['delta'] >= 400)
        df['timeout'] = (df['delta'] == -1)

        # find edges of degradation periods
        df['e1_edge'] = np.hstack((np.nan, np.diff(df['exceed1'].values)))
        df['e2_edge'] = np.hstack((np.nan, np.diff(df['exceed2'].values)))
        df['e3_edge'] = np.hstack((np.nan, np.diff(df['exceed3'].values)))
        df['out_edge'] = np.hstack((np.nan, np.diff(df['timeout'].values)))

        # create dataframes consisting of only the edge values
        dfe1 = pd.concat([df[df['e1_edge']==1]])
        dfe2 = pd.concat([df[df['e2_edge']==1]])
        dfe3 = pd.concat([df[df['e3_edge']==1]])
        dfout = pd.concat([df[df['out_edge']==1]])

        # mark if the datapoint is the start of end of a series
        dfe1['e1_state'] = np.where(dfe1['exceed1'], 'Start', 'End')
        dfe2['e2_state'] = np.where(dfe2['exceed2'], 'Start', 'End')
        dfe3['e3_state'] = np.where(dfe3['exceed3'], 'Start', 'End')
        dfout['out_state'] = np.where(dfout['timeout'], 'Start', 'End')

        # create unique indexices for the upcoming pivot
        # idx will be in the form of <OperID>@<integer>
        # each Start/End Pair will have the same idx (needed for pivot)
        dfe1['idx2'] = np.arange(len(dfe1))/2
        dfe2['idx2'] = np.arange(len(dfe2))/2
        dfe3['idx2'] = np.arange(len(dfe3))/2
        dfout['idx2'] = np.arange(len(dfout))/2
        dfe1['idx'] = dfe1.OperID.astype(int).map(str) + "@" + dfe1.idx2.map(str)
        dfe2['idx'] = dfe2.OperID.astype(int).map(str) + "@" + dfe2.idx2.map(str)
        dfe3['idx'] = dfe3.OperID.astype(int).map(str) + "@" + dfe3.idx2.map(str)
        dfout['idx'] = dfout.OperID.astype(int).map(str) + "@" + dfout.idx2.map(str)

        # pivot the dataframes
        pf1 = pd.DataFrame(dfe1.pivot(index='idx', columns='e1_state', values='OperTime'))
        pf2 = pd.DataFrame(dfe2.pivot(index='idx', columns='e2_state', values='OperTime'))
        pf3 = pd.DataFrame(dfe3.pivot(index='idx', columns='e3_state', values='OperTime'))
        pfout = pd.DataFrame(dfout.pivot(index='idx', columns='out_state', values='OperTime'))

        # get back the OperID
        pf1['OperID'] = pf1.index.values
        pf2['OperID'] = pf2.index.values
        pf3['OperID'] = pf3.index.values
        pfout['OperID'] = pfout.index.values

        pf1['OperID'] = pf1['OperID'].apply(lambda x: x[0:x.index('@')])
        pf2['OperID'] = pf2['OperID'].apply(lambda x: x[0:x.index('@')])
        pf3['OperID'] = pf3['OperID'].apply(lambda x: x[0:x.index('@')])
        pfout['OperID'] = pfout['OperID'].apply(lambda x: x[0:x.index('@')])

        pf1['OperID'] = pf1['OperID'].astype(int)
        pf2['OperID'] = pf2['OperID'].astype(int)
        pf3['OperID'] = pf3['OperID'].astype(int)
        pfout['OperID'] = pfout['OperID'].astype(int)

        # add the TypeID column
        # 1=Degraded, 2=Highly Degraded, 3=Severly Degraded, 4=Outage
        pf1['TypeID'] = 1
        pf2['TypeID'] = 2
        pf3['TypeID'] = 3
        pfout['TypeID'] = 4

        # create a dataframe with the desired columns
        cols = ['OperID', 'Start', 'End', 'TypeID']
        odf1 = pd.DataFrame(pf1, columns=cols)
        odf2 = pd.DataFrame(pf2, columns=cols)
        odf3 = pd.DataFrame(pf3, columns=cols)
        odfout = pd.DataFrame(pfout, columns=cols)

        # write list of degradation periods and outages to the DB
        mysql_cn2 = MySQLdb.connect(host='<db server name>', port=3306, user='<db username>', passwd='<db password>', db='<db name>')
        odf1.to_sql('RangeLog', mysql_cn2, flavor='mysql', if_exists='append')
        odf2.to_sql('RangeLog', mysql_cn2, flavor='mysql', if_exists='append')
        odf3.to_sql('RangeLog', mysql_cn2, flavor='mysql', if_exists='append')
        odfout.to_sql('RangeLog', mysql_cn2, flavor='mysql', if_exists='append')

        # consolidate ranges that have an End that equals the new Start that we just inserted
        psql.frame_query("CALL range_cleaner", con=mysql_cn2)
        mysql_cn2.close()

if __name__ == "__main__":
   main(sys.argv[1:])
