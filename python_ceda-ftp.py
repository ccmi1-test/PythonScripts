#!/usr/bin/env python
"""
   Use the MLSD functionality in ftp to get a list of all models/experiments/variables
   that are available in the CEDA CCMI-2022 archive and, if requested, download a
   set of files for the specified experiment, variable and table.

     Parameters to set:
         cedauser - your CEDA username
         trgexpt  - the list of experiments that you want to download data for using
                       the CCMI-2022 experiment_id
         trgdata  - the first part of the file name for the specific data that is
                       constructed as <variable_id>_<table_id>
         ddir     - the local directory where downloaded files are put

   If trgexpt is empty (trgexpt=[]) then no files are downloaded and only the
         listing of the archive is produced
   The CEDA password is entered at a prompt when the script is run
   The script requires Python 3.3 or higher

   Author: David Plummer, Environment and Climate Change Canada
"""
#
# Import required python modules
from ftplib import FTP
import getpass
import numpy as np
import pprint
from copy import deepcopy
from datetime import datetime
import os
import sys
#
# Specify your CEDA username
cedauser ='dplummer'
#
# Specify the experiments for which data is requested
#  --- leave an empty list if there is no download requested
trgexpt = []   # empty list to turn off file downloading
#trgexpt = [ 'refD2' , 'senD2-sai' ]
#
# Specify the data files being requested - the first two parts of the filename
#    giving variable_id and table_id
#    e.g. [ 'o3_Amon', 'ua100_Aday' ]
trgdata = [ 'ta_AmonZ', 'wtem_AmonZ' , 'vtem_AmonZ' , 'o3_AmonZ' , 'o3strat_AmonZ', 'toz_Amon' ]
#
# Define the local directory where data would be put
ddir='/space/hall4/sitestore/eccc/crd/ccrn/users/rdp001/ccmi-2022/import'
#
#  ----  pull apart the filenames to get the variables and tables that are
#        being looked for
trgxvar = []
for name in trgdata:
    trgxvar.append(name.split('_')[0])
#
trgtble = []
for name in trgdata:
    trgtble.append(name.split('_')[1])
#
nvsrch = len(trgxvar)
if nvsrch != len(trgtble):
    print('  ------ Problems with variable/table combinations')
    exit()
#
print(' -- Variables being searched for ', nvsrch)
for ia in range(nvsrch):
    print(trgxvar[ia] + ' in ' + trgtble[ia])
#
#  ---- get login credentials
prompt = f'Username (default: '+cedauser+'): '
if sys.stdout.isatty():
    cuser = input(prompt)
else:
    print(prompt, end='', file=sys.stderr)
    cuser = input()
cuser = cuser.strip()
cuser = cuser or cedauser
#
#  ---- login to FTP
ftpc=FTP('ftp.ceda.ac.uk')
ftpc.login(user=cuser, passwd=getpass.getpass())
#
#  ----  go directly to the top of the CCMI archive
droot = '/badc/ccmi/data/post-cmip6/ccmi-2022'
ftpc.cwd(droot)
#
#  ----  get the list of institutes
instts=[]
inner_list = list(ftpc.mlsd())
for name, properties in inner_list:
    if properties['type'] == 'dir':
        instts.append(name)
ninst = len(instts)
print('Number of institutions ', ninst)
print(instts)
#
#  ----  the directory structure will be constructed in the
#        nested list invntry
invntry=[]
#
#  ----  get the list of models
nmdls = np.zeros(ninst, dtype=int)
for ia in range(ninst):
    ftpc.cwd(droot+'/'+instts[ia])
    mname=[]
    inner_list = list(ftpc.mlsd())
    for name, properties in inner_list:
        if properties['type'] == 'dir':
            mname.append(name)
    nmdls[ia] = len(mname)
    work = [instts[ia], mname]
    invntry.append(work)
#
tnmdls = sum(nmdls)
print('Total number of models', tnmdls)
print('Number of models for each institution')
print(nmdls)
#
#  ----  get the list of experiments
#    ----  direct reference to list avoids recursive problems
icnt1 = 0
nxpts = np.zeros(tnmdls, dtype=int)
for ia in range(ninst):
    twork = deepcopy(invntry[ia])
    for ib in range(nmdls[ia]):
        ftpc.cwd(droot+'/'+twork[0]+'/'+twork[1][ib])
        inner_list = list(ftpc.mlsd())
        ip=0
        for name, properties in inner_list:
            if properties['type'] == 'dir':
                invntry[ia][1].insert(2*ib+ip+1, [name])
                ip += 1
        nxpts[icnt1] = ip
        icnt1 += 1
#
tnxpts = sum(nxpts)
print('Total number of experiments ', tnxpts)
print('Number of experiments ', nxpts)
#
#  ----  for each experiment, get the runs
icnt1 = 0
icnt2 = 0
nsims = np.zeros(tnxpts, dtype=int)
for ia in range(ninst):
    for ib in range(1,nmdls[ia]+1):
        mdl = invntry[ia][ib][0]
        twork = deepcopy(invntry[ia][ib])
        for ic in range(1,nxpts[icnt1]+1):
            expt = twork[ic][0]
            ftpc.cwd(droot+'/'+invntry[ia][0]+'/'+mdl+'/'+expt)
            inner_list = list(ftpc.mlsd())
            ip=0
            for name, properties in inner_list:
                if properties['type'] == 'dir':
                    invntry[ia][ib][ic].insert(1+ip, [name])
                    ip +=1
            nsims[icnt2] = ip
            icnt2 += 1
        icnt1 += 1
#
tnsims = sum(nsims)
print('Total number of simulations ',tnsims)
print('Number of simulations for each experiment')
print(nsims)
#
#  ---- for each experiment get the MIP tables (Amon, AmonZ, etc.) for
#       which variables have been provided
icnt1 = 0
icnt2 = 0
icnt3 = 0
ntabs = np.zeros(tnsims, dtype=int)
for ia in range(ninst):
    for ib in range(1,nmdls[ia]+1):
        mdl = invntry[ia][ib][0]
        for ic in range(1,nxpts[icnt1]+1):
            expt = invntry[ia][ib][ic][0]
            twork = deepcopy(invntry[ia][ib][ic])
            for id in range(1,nsims[icnt2]+1):
                rsim = twork[id][0]
                ip=0
                ftpc.cwd(droot+'/'+invntry[ia][0]+'/'+mdl+'/'+expt+'/'+rsim)
                inner_list = list(ftpc.mlsd())
                for name, properties in inner_list:
                    if properties['type'] == 'dir':
                        invntry[ia][ib][ic][id].insert(1+ip, [name])
                        ip +=1
                ntabs[icnt3] = ip
                icnt3 += 1
            icnt2 += 1
        icnt1 += 1
#
tntabs = sum(ntabs)
print('Total number of individual MIP tables ',tntabs)
print('Number of MIP tables provided for each simulation')
print(ntabs)
#
#  ---- deduce the available variables from the directory names, rather
#       than the individual files
icnt1 = 0
icnt2 = 0
icnt3 = 0
icnt4 = 0
nvars = np.zeros(tntabs, dtype=int)
for ia in range(ninst):
    for ib in range(1,nmdls[ia]+1):
        mdl = invntry[ia][ib][0]
        for ic in range(1,nxpts[icnt1]+1):
            expt = invntry[ia][ib][ic][0]
            for id in range(1,nsims[icnt2]+1):
                rsim = invntry[ia][ib][ic][id][0]
                twork = deepcopy(invntry[ia][ib][ic][id])
                for ie in range(1,ntabs[icnt3]+1):
                    mtab = twork[ie][0]
                    print('Searching ---', ia, ib, ic, id, ie, mdl, expt, rsim, mtab)
                    ip=0
                    ftpc.cwd(droot+'/'+invntry[ia][0]+'/'+mdl+'/'+expt+'/'+rsim+
                               '/'+mtab)
                    inner_list = list(ftpc.mlsd())
                    for name, properties in inner_list:
                        if properties['type'] == 'dir':
                            invntry[ia][ib][ic][id][ie].insert(1+ip, [name])
                            ip +=1
                    nvars[icnt4] = ip
                    icnt4 += 1
                icnt3 += 1
            icnt2 += 1
        icnt1 += 1
#
tnvars = sum(nvars)
print('Total number of individual variables ',tnvars)
print('Number of variables in each table directory')
print(nvars)
#
#  ----  if targets are specified for download, dive down to the bottom of the
#        directory structure and retrieve them
tfsize=0.0
if trgexpt:
    wdir = os.getcwd()
#
# If directory doesn't exist make it
    if not os.path.isdir(ddir):
        os.mkdir(ddir)
#
# Change the local directory to where you want to put the data
    os.chdir(ddir)
#
    icnt1 = 0
    icnt2 = 0
    icnt3 = 0
    icnt4 = 0
    for ia in range(ninst):
        for ib in range(1,nmdls[ia]+1):
            mdl = invntry[ia][ib][0]
            for ic in range(1,nxpts[icnt1]+1):
                expt = invntry[ia][ib][ic][0]
                for id in range(1,nsims[icnt2]+1):
                    rsim = invntry[ia][ib][ic][id][0]
                    for ie in range(1,ntabs[icnt3]+1):
                        mtab = invntry[ia][ib][ic][id][ie][0]
                        for ig in range(1,nvars[icnt4]+1):
                            xvar = invntry[ia][ib][ic][id][ie][ig][0]
                            if expt in trgexpt:
                                vpull=False
                                for ih in range(nvsrch):
                                    if mtab == trgtble[ih] and xvar == trgxvar[ih]: vpull=True
                                if vpull:
                                    print('Found ---', ia, ib, ic, id, ie, ig, mdl, expt,
                                       rsim, mtab, xvar)
                                    ftpc.cwd(droot+'/'+invntry[ia][0]+'/'+mdl+'/'+
                                             expt+'/'+rsim+'/'+mtab+'/'+xvar)
                                    inner_list = list(ftpc.mlsd())
                                    ldirs=[]
                                    for name, properties in inner_list:
                                        if properties['type'] == 'dir':
                                            ldirs.append(name)
                                    ftpc.cwd(ldirs[0])
#
#                ------ there may be more than one version directory so we take
#                       the last on the list since it should be the latest date
                                    inner_list = list(ftpc.mlsd())
                                    ldirs=[]
                                    for name, properties in inner_list:
                                        if properties['type'] == 'dir':
                                            ldirs.append(name)
                                    print(ldirs)
                                    ftpc.cwd(ldirs[-1])
#
#                ------ finally, the list of files
                                    inner_list = list(ftpc.mlsd())
                                    lfiles=[]
                                    for name, properties in inner_list:
                                        if properties['type'] == 'file':
                                            lfiles.append(name)
                                    for dfile in lfiles:
                                        ftpc.retrbinary('RETR %s' % dfile, open(dfile, "wb").write)
#
                        icnt4 += 1
                    icnt3 += 1
                icnt2 += 1
            icnt1 += 1
#
#  ---- go back to the original directory
    os.chdir(wdir)
#
# Close FTP connection
ftpc.close()
#
#  ---- dump a list of all models/experiments/variables found in the archive
tday = datetime.utcnow()
dstring =  tday.strftime('%Y%m%d')
with open('CCMI-2022_archive_'+dstring, 'wt') as out:
    pp = pprint.PrettyPrinter(indent=4, compact=True, stream=out)
    pp.pprint(invntry)
#
