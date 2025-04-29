#!/usr/bin/env python
"""
   Uses THREDDS catalog data on CEDA in XML to inventory the available model
   data in the ccmi-2022 archive. If a combination of experiments (trgexpt)
   and variables (trgdata) are specified the script will search for these
   and download all matching files

     Parameters to set:
         trgexpt  - the list of experiments that you want to download data for using
                       the CCMI-2022 experiment_id
         trgdata  - the first part of the file name for the specific data that you
                       are looking for, constructed as <variable_id>_<table_id>
         use_existing_inventory_file - if you already have an existing inventory of
                       the CEDA archive from a previous run of the script you can
                       reference it and avoid querying the servers at CEDA

   If trgexpt is empty (trgexpt=[]) then no files are downloaded and only the
         listing of the archive is produced

   While the ccmi-2022 archive has restricted access, it is necessary to pass user
         authentication. For more information on the methods available see
           https://help.ceda.ac.uk/article/4442-ceda-opendap-scripted-interactions

   Author: David Plummer, Environment and Climate Change Canada

   Revisions: Sean Davis, NOAA, Chemical Sciences Laboratory
               - significant rewrite, reorganization into functions and addition of
                     ability to use an existing directory listing
               - addition of ability to download files from CEDA using authentication
                     via certificates
"""

from urllib.request import urlopen
import urllib.error
import xml.etree.ElementTree as ET
import numpy as np
from copy import deepcopy
from datetime import datetime
import pprint
import requests
import os

def get_elements(url, tag1, tag2, attribute_name,verbose=False):
    """Get elements from an XML file"""
    #
    # ---- name spaces used in catalog.xml on CEDA
    cedans = { 'default':
              'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0',
              'xlink': 'http://www.w3.org/1999/xlink'}
    if verbose:
        print(url)
    for ntry in range(5):
        try:
            usock = urlopen(url)
            break
        except urllib.error.HTTPError:
            print('Possible communication error. Trying again - ',ntry)
    xmldoc = ET.parse(usock)
    usock.close()
    root = xmldoc.getroot()
    #
    #  --- digging down into the elements of the XML is a bit clunky with
    #        ElementTree because of the namespaces in the attribute
    #  --- it is made a bit more clunky because the lowest level directory
    #        (where the file is) has a different application of namespaces
    attributes=[]
    for child in root.findall(tag1, cedans):
        for work1 in child.findall(tag2, cedans):
            attribute=work1.attrib[attribute_name]
            attributes.append(attribute)
  
    return attributes

# %% Routine to create the inventory file
def create_inventory_file(inventory_file,verbose=False,root_url='https://dap.ceda.ac.uk/thredds/badc/ccmi/data/post-cmip6/ccmi-2022/'):    
    
    url = root_url + 'catalog.xml'
    instts = get_elements(url, 'default:dataset', 'default:catalogRef',
                          '{http://www.w3.org/1999/xlink}title')
    ninst = len(instts) 
    if verbose:
        print('Number of institutions ', ninst)
        print(instts)
    #
    #  ----  the directory structure will be constructed in the
    #        nested list invntry
    invntry=[]
    #
    nmdls = np.zeros(ninst, dtype=int)
    for ii in range(ninst):
        url = root_url + instts[ii] + '/catalog.xml'
        mname = get_elements(url, 'default:dataset', 'default:catalogRef',
                             '{http://www.w3.org/1999/xlink}title')
        nmdls[ii] = len(mname)
        work = [instts[ii], mname]
        invntry.append(work)
    #
    tnmdls = sum(nmdls)
    if verbose:
        print('Total number of models ', tnmdls)
        print('Number of models for each institution')
        print(nmdls)
    #
    #  ----  get the list of experiments
    #    ----  direct reference to list avoids recursive problems
    icnt1 = 0
    nxpts = np.zeros(tnmdls, dtype=int)
    for ii in range(ninst):
        twork = deepcopy(invntry[ii])
        for ij in range(nmdls[ii]):
            url = root_url + twork[0]+'/' + twork[1][ij] + '/catalog.xml'
            expname = get_elements(url, 'default:dataset', 'default:catalogRef',
                                   '{http://www.w3.org/1999/xlink}title')
            if verbose:
                print(expname)
            for ik in range(len(expname)):
                invntry[ii][1].insert(2*ij+ik+1,[expname[ik]])
            nxpts[icnt1] = len(expname)
            icnt1 +=1
    #
    tnxpts = sum(nxpts)
    if verbose:
        print('Total number of experiments ', tnxpts)
        print('Number of experiments for each model')
        print(nxpts)
    #
    #  ---- for each experiment, get the runs
    icnt1 = 0
    icnt2 = 0
    nsims = np.zeros(tnxpts, dtype=int)
    for ii in range(ninst):
        for ij in range(1,nmdls[ii]+1):
            twork = deepcopy(invntry[ii][ij])
            for ik in range(1,nxpts[icnt1]+1):
                url = (root_url + invntry[ii][0]+'/' +
                       invntry[ii][ij][0]+'/'+ twork[ik][0] + '/catalog.xml')
                sims = get_elements(url, 'default:dataset', 'default:catalogRef',
                                    '{http://www.w3.org/1999/xlink}title')
                if verbose:
                    print(sims)
                for il in range(len(sims)):
                    invntry[ii][ij][ik].insert(il+1,[sims[il]])
                nsims[icnt2] = len(sims)
                icnt2 += 1
            icnt1 += 1
    #
    tnsims = sum(nsims)
    if verbose:
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
    for ii in range(ninst):
        for ij in range(1,nmdls[ii]+1):
            for ik in range(1,nxpts[icnt1]+1):
                twork = deepcopy(invntry[ii][ij][ik])
                for il in range(1,nsims[icnt2]+1):
                    url = (root_url + invntry[ii][0]+'/' +
                           invntry[ii][ij][0]+'/'+ invntry[ii][ij][ik][0]+'/' +
                           twork[il][0] + '/catalog.xml')
                    tables = get_elements(url, 'default:dataset', 'default:catalogRef',
                                          '{http://www.w3.org/1999/xlink}title')
                    if verbose:
                        print(tables)
                    for im in range(len(tables)):
                        invntry[ii][ij][ik][il].insert(im+1,[tables[im]])
                    ntabs[icnt3] = len(tables)
                    icnt3 += 1
                icnt2 += 1
            icnt1 += 1
    #
    tntabs = sum(ntabs)
    if verbose:
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
    for ii in range(ninst):
        for ij in range(1,nmdls[ii]+1):
            mdl = invntry[ii][ij][0]
            for ik in range(1,nxpts[icnt1]+1):
                expt = invntry[ii][ij][ik][0]
                for il in range(1,nsims[icnt2]+1):
                    rsim = invntry[ii][ij][ik][il][0]
                    twork = deepcopy(invntry[ii][ij][ik][il])
                    for im in range(1,ntabs[icnt3]+1):
                        url = (root_url + invntry[ii][0]+'/' +
                               invntry[ii][ij][0]+'/'+ invntry[ii][ij][ik][0]+'/' +
                               invntry[ii][ij][ik][il][0]+'/' + twork[im][0] +
                               '/catalog.xml')
                        varbls = get_elements(url, 'default:dataset',
                                              'default:catalogRef',
                                              '{http://www.w3.org/1999/xlink}title')
    #                    print(varbls)
                        invntry[ii][ij][ik][il][im].insert(1,[varbls[0]])
                        for ip in range(1,len(varbls)):
                            invntry[ii][ij][ik][il][im][1].insert(ip,varbls[ip])
                        nvars[icnt4] = len(varbls)
                        icnt4 += 1
                    icnt3 += 1
                icnt2 += 1
            icnt1 += 1
    #
    tnvars = sum(nvars)
    if verbose:
        print('Total number of individual variables ',tnvars)
        print('Number of variables in each table directory')
        print(nvars)
    #
    #  ---- end of inventory
    #
    #  ---- dump a list of all models/experiments/variables found in the archive
    # need to include nmdls, nxpts,nsims,ntabs,nvars
    fullinvntry = [invntry, [ninst], list(nmdls), list(nxpts),list(nsims),list(ntabs),list(nvars)]
    with open(inventory_file, 'wt') as out:
        pp = pprint.PrettyPrinter(indent=4, compact=True, stream=out)
        pp.pprint(fullinvntry)
    return fullinvntry

# %% Routine to search the inventory
def search_inventory(fullinvntry, trgexpt, trgdata, trgxvar, trgtble,verbose=False):
    #  ----  if particular files/experiments are specified for the search, dive
    #        down to the bottom of the directory and take the location
    
    invntry = fullinvntry[0]
    ninst = fullinvntry[1][0]
    nmdls = np.array(fullinvntry[2])
    nxpts = np.array(fullinvntry[3])
    nsims = np.array(fullinvntry[4])
    ntabs = np.array(fullinvntry[5])
    nvars = np.array(fullinvntry[6])
    
    if trgexpt:
    #
        flist = []
        for ii in range(ninst):
            flist.append([invntry[ii][0],[]])
        if verbose:        
            print(flist)
    #
        icnt1 = 0
        icnt2 = 0
        icnt3 = 0
        icnt4 = 0
        for ii in range(ninst):
            for ij in range(1,nmdls[ii]+1):
                mdl = invntry[ii][ij][0]
                for ik in range(1,nxpts[icnt1]+1):
                    expt = invntry[ii][ij][ik][0]
                    for il in range(1,nsims[icnt2]+1):
                        rsim = invntry[ii][ij][ik][il][0]
                        for im in range(1,ntabs[icnt3]+1):
                            mtab = invntry[ii][ij][ik][il][im][0]
                            for ip in range(nvars[icnt4]):
                                xvar = invntry[ii][ij][ik][il][im][1][ip]
                                ifound=1
                                if expt in trgexpt and mtab in trgtble and xvar in trgxvar:
                                    if verbose:
                                        print('Found ---', ii, ij, ik, il, im, ip, mdl, expt,
                                               rsim, mtab, xvar)
                                    url = (root_url + invntry[ii][0]+'/' +
                                           invntry[ii][ij][0]+'/'+ invntry[ii][ij][ik][0]+'/' +
                                           invntry[ii][ij][ik][il][0]+'/' +
                                           invntry[ii][ij][ik][il][im][0]+'/' +
                                           invntry[ii][ij][ik][il][im][1][ip] +
                                           '/catalog.xml')
                                    ldirs = get_elements(url, 'default:dataset',
                                                         'default:catalogRef',
                                                         '{http://www.w3.org/1999/xlink}title')
    #
    #                ------ assuming there should only be one type of grid label for each field
                                    url = (root_url + invntry[ii][0]+'/' +
                                           invntry[ii][ij][0]+'/'+ invntry[ii][ij][ik][0]+'/' +
                                           invntry[ii][ij][ik][il][0]+'/' +
                                           invntry[ii][ij][ik][il][im][0]+'/' +
                                           invntry[ii][ij][ik][il][im][1][ip]+'/' +
                                           ldirs[0] + '/catalog.xml')
                                    vers = get_elements(url, 'default:dataset',
                                                        'default:catalogRef',
                                                        '{http://www.w3.org/1999/xlink}title')
    #
    #                ------ there may be more than one version directory so we take the
    #                       one with the latest date
                                    if(len(vers) > 1):
                                        nwork = np.zeros(len(vers), dtype=int)
                                        for iv in range(len(vers)):
                                            nwork[iv] = int(vers[iv][1:])
                                        vertrg = 'v' + str(max(nwork))
                                    else:
                                        vertrg = vers[0]
    #
                                    url = (root_url + invntry[ii][0]+'/' +
                                           invntry[ii][ij][0]+'/'+ invntry[ii][ij][ik][0]+'/' +
                                           invntry[ii][ij][ik][il][0]+'/' +
                                           invntry[ii][ij][ik][il][im][0]+'/' +
                                           invntry[ii][ij][ik][il][im][1][ip]+'/' + 
                                           ldirs[0]+'/' + vertrg + '/catalog.xml')
                                    files = get_elements(url, 'default:dataset',
                                                         'default:dataset',
                                                         'urlPath')
    #
                                    for iq in range(len(files)):
                                        flist[ii][1].insert(ifound,files[iq])
                                        ifound += 1
                            icnt4 += 1
                        icnt3 += 1
                    icnt2 += 1
                icnt1 += 1
    
    # Create log of files matching search
    searchfile = 'CCMI-2022_search.log'
    with open(searchfile, 'wt') as out:
        pp = pprint.PrettyPrinter(indent=4, compact=True, stream=out)
        pp.pprint(flist)
    print('Wrote log file of files matching search criteria: ' + searchfile)

    return flist

# %% USER SETTINGS SECTION

# User setting for the base directory in which to download files
#   Note that in the code snipped below, files are organized into
#   subdirectories by model experiment (e.g., downloaddir/refD1)
downloaddir = '.../myhome/downloaddir/refD1'
    
# Specify the experiments for which data is requested
#  --- leave an empty list if only an inventory of available data
#      is requested
#trgexpt = []   # specification for an empty list
# trgexpt = [ 'refD1' , 'refD2' ]
trgexpt = [ 'refD1' ]

#
# Specify the data files being requested - the first two parts of the filename
#    giving variable_id and table_id
#    e.g. [ 'o3_Amon', 'ua100_Aday' ]
#trgdata = ['h2o_AmonZ', 'ta_AmonZ', 'wtem_AmonZ' , 'vtem_AmonZ' , 'o3_AmonZ' ]
#           'o3strat_AmonZ', 'toz_Amon' , 'ps_AmonZ' ,'ua_AmonZ' , 'va_AmonZ' ,
#           'tatp_AmonZ' , 'ptp_AmonZ' , 'pr_AmonZ' ]
trgdata = [ 'h2o_AmonZ' ]

# File path to the credentials file
# To create this file, follow the directions at https://help.ceda.ac.uk/article/4442-ceda-opendap-scripted-interactions
# Specificially, follow the directions under:
#   1. "Getting Started" - https://help.ceda.ac.uk/article/4442-ceda-opendap-scripted-interactions#start
#   2. "Getting a Security Certificate" - https://help.ceda.ac.uk/article/4442-ceda-opendap-scripted-interactions#cert
# The variable below should point to the creds.pem file that you create
# NOTE - The credentials file must be re-generated every 3 days!
cert = 'my/directory/ceda_pydap_cert_code/creds.pem'

# User setting to determine whether to download/overwrite existing local files
OverwriteExistingFile=False

# Specify whether or not we want to generate a new inventory file -- inventory_file must be a valid file if so!
use_existing_inventory_file=False

# The inventory file must be specified here if use_existing_inventory_file=True
# The file name convention for the inventory file includes a yyyymmddhhmm datestring on the end of the file name
# If use_existing_inventory_file=False, the file name is generated below
if use_existing_inventory_file:
    inventory_file = 'CCMI-2022_archive_202504031936'

# If verbose is True, the status of every file will be printed to screen
# If False, only newly downloaded files will be printed to screen
verbose = True

# URL for the download - this shouldn't need to change
root_url='https://dap.ceda.ac.uk/thredds/badc/ccmi/data/post-cmip6/ccmi-2022/'

## END USER SETTINGS SECTION

# %% ---- start of the script

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
print(' -- Variables being searched for ')
for ia in range(len(trgxvar)):
    print(trgxvar[ia] + ' in ' + trgtble[ia])


# %% Create or read in the inventory file
if use_existing_inventory_file:
    dstring= inventory_file.split('_')[-1]
    print('Using existing inventory file: ' + inventory_file)
    with open(inventory_file, 'r') as f:
        fullinvntry = eval(f.read())
else:
    tday = datetime.utcnow()
    dstring =  tday.strftime('%Y%m%d%H%M')
    inventory_file = 'CCMI-2022_archive_'+dstring
    fullinvntry = create_inventory_file(inventory_file, verbose=verbose, root_url=root_url)
    print('Generated inventory file: ' + inventory_file)

# %% Now search the inventory for the given variables/experiments, and create a list of files to download
flist = search_inventory(fullinvntry, trgexpt, trgdata, trgxvar, trgtble, verbose=verbose)

# Create a single list of files to download from the variable flist
# flist is a list where each element is a list of ['modelname',[list,of,files]]
filelist = []
for fentry in flist:    
    filelist+=fentry[1]

# %% Now download the actual files

for afile in filelist:
    # This is the URL of the file to download
    fileURL = 'https://dap.ceda.ac.uk/' + afile
    
    # Determine the output (destination) file path
    # The output file path == the download directory + the experiment name as a subdirectory + the file name
    destfilebase = os.path.basename(afile)           # e.g., 'cly_AmonZ_CNRM-MOCAGE_refD1_r1i1p1f1_gnz_197201-197212.nc'
    experiment = destfilebase.split('_')[3]          # e.g., 'refD1'
    destfile = os.path.join( downloaddir, experiment, destfilebase) # e.g., '/my/directory/refD1/cly_AmonZ_CNRM-MOCAGE_refD1_r1i1p1f1_gnz_197201-197212.nc'
    
    #If the destination file exists and we are NOT overwriting
    if not OverwriteExistingFile and os.path.exists(destfile):
        if verbose:
            print(destfile + ' already exists. Skipping!')
        continue
    
    if verbose:
        print('Downloading: ' + fileURL)
        print('Destination file:' + destfile)
    try:
        with requests.Session() as session:
            response = session.get(fileURL,cert=cert)
            with open(destfile, 'wb') as f:
                f.write(response.content)
            print('Downloaded ' + destfile)        
    except:
        print('Download failed: ' + fileURL )
