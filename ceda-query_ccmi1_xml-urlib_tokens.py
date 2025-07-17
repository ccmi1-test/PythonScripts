#!/usr/bin/env python
"""
   Uses THREDDS catalog data on CEDA in XML to inventory the available model
   data in the CCMI-1 archive. If a combination of experiments (trgexpt)
   and variables (trgvar) are specified the script will search for these
   and download all matching files

   ADAPTED FROM CCMI-2022 SCRIPT FOR CCMI-1 ARCHIVE

     Parameters to set:
         trgexpt  - the list of experiments that you want to download data for using
                       the CCMI-1 experiment_id (e.g., 'refC1', 'refC2', 'senC2fEmis')
         trgvar   - the list of variable names that you are looking for
                       (e.g., ['zmcly', 'o3', 'ta', 'h2o'])
         trgfreq  - the frequency domain (e.g., 'mon', 'day')
         trgrealm - the realm (e.g., 'atmos', 'ocean')
         trgfreqname - the frequency name (e.g., 'monthly', 'daily')
         use_existing_inventory_file - if you already have an existing inventory of
                       the CEDA archive from a previous run of the script you can
                       reference it and avoid querying the servers at CEDA

   If trgexpt is empty (trgexpt=[]) then no files are downloaded and only the
         listing of the archive is produced

   CCMI-1 Directory Structure:
   /Institution/Model/Experiment/FreqDomain/Realm/FreqName/EnsembleMember/Version/Variable/

   Example: CCCma/CMAM/refC1/mon/atmos/monthly/r1i1p1/v1/zmcly/

   Author: David Plummer, Environment and Climate Change Canada
   
   Revisions: Sean Davis, NOAA, Chemical Sciences Laboratory
               - significant rewrite, reorganization into functions and addition of
                     ability to use an existing directory listing
               - addition of ability to download files from CEDA using authentication
                     via certificates

   Adapted for CCMI-1 by: Andreas Chrysanthou, Institute of Geosciences (IGEO), 
                          Spanish National Research Council (CSIC), Madrid, Spain
"""

from urllib.request import urlopen
import urllib.error
import xml.etree.ElementTree as ET
import numpy as np
from copy import deepcopy
import datetime
import pprint
import requests
import os

def get_elements(url, tag1, tag2, attribute_name, verbose=False):
    """Get elements from an XML file"""
    #
    # ---- name spaces used in catalog.xml on CEDA
    cedans = { 'default':
              'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0',
              'xlink': 'http://www.w3.org/1999/xlink'}
    if verbose:
        print(url)
    
    usock = None
    for ntry in range(5):
        try:
            usock = urlopen(url)
            break
        except urllib.error.HTTPError as e:
            print(f'Possible communication error (attempt {ntry+1}/5): {e}')
            if ntry == 4:  # Last attempt
                print(f'Failed to access URL after 5 attempts: {url}')
                return []
        except Exception as e:
            print(f'Unexpected error accessing {url}: {e}')
            return []
    
    if usock is None:
        print(f'Could not open URL: {url}')
        return []
    
    try:
        xmldoc = ET.parse(usock)
        usock.close()
        root = xmldoc.getroot()
    except Exception as e:
        print(f'Error parsing XML from {url}: {e}')
        if usock:
            usock.close()
        return []
    
    #
    #  --- digging down into the elements of the XML is a bit clunky with
    #        ElementTree because of the namespaces in the attribute
    #  --- it is made a bit more clunky because the lowest level directory
    #        (where the file is) has a different application of namespaces
    attributes=[]
    for child in root.findall(tag1, cedans):
        for work1 in child.findall(tag2, cedans):
            if attribute_name in work1.attrib:
                attribute=work1.attrib[attribute_name]
                attributes.append(attribute)
  
    return attributes

# %% Routine to create the inventory file for CCMI-1
def create_inventory_file(inventory_file, verbose=False, root_url='https://dap.ceda.ac.uk/thredds/badc/wcrp-ccmi/data/CCMI-1/output/'):    
    """
    Create inventory of CCMI-1 archive with structure:
    Institution/Model/Experiment/FreqDomain/Realm/FreqName/EnsembleMember/Version/Variable/
    """
    
    url = root_url + 'catalog.xml'
    instts = get_elements(url, 'default:dataset', 'default:catalogRef',
                          '{http://www.w3.org/1999/xlink}title')
    ninst = len(instts) 
    if verbose:
        print('Number of institutions ', ninst)
        print(instts)
    
    # Initialize inventory structure
    invntry = []
    
    # Level 1: Institutions
    nmdls = np.zeros(ninst, dtype=int)
    for ii in range(ninst):
        url = root_url + instts[ii] + '/catalog.xml'
        mname = get_elements(url, 'default:dataset', 'default:catalogRef',
                             '{http://www.w3.org/1999/xlink}title')
        nmdls[ii] = len(mname)
        work = [instts[ii], mname]
        invntry.append(work)
    
    tnmdls = sum(nmdls)
    if verbose:
        print('Total number of models ', tnmdls)
        print('Number of models for each institution')
        print(nmdls)
    
    # Level 2: Models -> Experiments
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
    
    tnxpts = sum(nxpts)
    if verbose:
        print('Total number of experiments ', tnxpts)
        print('Number of experiments for each model')
        print(nxpts)
    
    # For CCMI-1, create simplified inventory
    fullinvntry = [invntry, [ninst], list(nmdls), list(nxpts)]
    with open(inventory_file, 'wt') as out:
        pp = pprint.PrettyPrinter(indent=4, compact=True, stream=out)
        pp.pprint(fullinvntry)
    
    return fullinvntry

# %% Routine to search the CCMI-1 archive
def search_inventory_ccmi1(root_url, trgexpt, trgvar, trgfreq='mon', trgrealm='atmos', 
                          trgfreqname='monthly', verbose=False):
    """
    Search CCMI-1 archive directly by constructing expected paths
    Since CCMI-1 has a fixed structure: Institution/Model/Experiment/FreqDomain/Realm/FreqName/EnsembleMember/Version/Variable/
    """
    
    # Get institutions
    url = root_url + 'catalog.xml'
    instts = get_elements(url, 'default:dataset', 'default:catalogRef',
                          '{http://www.w3.org/1999/xlink}title')
    
    found_files = []
    
    for inst in instts:
        if verbose:
            print(f'Checking institution: {inst}')
        
        # Get models for this institution
        url = root_url + inst + '/catalog.xml'
        try:
            models = get_elements(url, 'default:dataset', 'default:catalogRef',
                                 '{http://www.w3.org/1999/xlink}title')
        except:
            if verbose:
                print(f'Could not access models for {inst}')
            continue
        
        for model in models:
            if verbose:
                print(f'  Checking model: {model}')
            
            for expt in trgexpt:
                # Construct path to experiment
                expt_path = f'{inst}/{model}/{expt}'
                
                # Check if frequency domain exists
                freq_url = root_url + expt_path + f'/{trgfreq}/catalog.xml'
                try:
                    realms = get_elements(freq_url, 'default:dataset', 'default:catalogRef',
                                         '{http://www.w3.org/1999/xlink}title')
                    if trgrealm not in realms:
                        continue
                except:
                    if verbose:
                        print(f'    No {trgfreq} data for {expt_path}')
                    continue
                
                # Check realm and frequency name
                realm_path = f'{expt_path}/{trgfreq}/{trgrealm}'
                freqname_url = root_url + realm_path + '/catalog.xml'
                try:
                    freqnames = get_elements(freqname_url, 'default:dataset', 'default:catalogRef',
                                           '{http://www.w3.org/1999/xlink}title')
                    if trgfreqname not in freqnames:
                        continue
                except:
                    if verbose:
                        print(f'    No {trgrealm} data for {realm_path}')
                    continue
                
                # Check ensemble members
                ensemble_path = f'{realm_path}/{trgfreqname}'
                ensemble_url = root_url + ensemble_path + '/catalog.xml'
                try:
                    ensembles = get_elements(ensemble_url, 'default:dataset', 'default:catalogRef',
                                           '{http://www.w3.org/1999/xlink}title')
                except:
                    if verbose:
                        print(f'    No ensemble data for {ensemble_path}')
                    continue
                
                for ensemble in ensembles:
                    # Check versions
                    version_path = f'{ensemble_path}/{ensemble}'
                    version_url = root_url + version_path + '/catalog.xml'
                    try:
                        versions = get_elements(version_url, 'default:dataset', 'default:catalogRef',
                                              '{http://www.w3.org/1999/xlink}title')
                    except:
                        continue
                    
                    # Take the latest version (assuming v1, v2, etc.)
                    for version in sorted(versions):
                        
                        # Check variables
                        var_path = f'{version_path}/{version}'
                        var_url = root_url + var_path + '/catalog.xml'
                        try:
                            variables = get_elements(var_url, 'default:dataset', 'default:catalogRef',
                                                   '{http://www.w3.org/1999/xlink}title')
                        except:
                            continue
                        
                        for var in trgvar:
                            if var in variables:
                                if verbose:
                                    print(f'    Found {var} in {var_path}')
                                
                                # Get files for this variable
                                file_path = f'{var_path}/{var}'
                                file_url = root_url + file_path + '/catalog.xml'
                                try:
                                    files = get_elements(file_url, 'default:dataset',
                                                       'default:dataset', 'urlPath')
                                    for file_path in files:
                                        found_files.append(file_path)
                                        if verbose:
                                            print(f'      File: {file_path}')
                                except:
                                    if verbose:
                                        print(f'      Could not get files for {file_path}')
    
    # Create log of files matching search
    searchfile = 'CCMI-1_search.log'
    with open(searchfile, 'wt') as out:
        pp = pprint.PrettyPrinter(indent=4, compact=True, stream=out)
        pp.pprint(found_files)
    print('Wrote log file of files matching search criteria: ' + searchfile)
    
    return found_files

# %% USER SETTINGS SECTION

# User setting for the base directory in which to download files
#   Note that in the code snipped below, files are organized into
#   subdirectories by model experiment (e.g., downloaddir/refC1)
downloaddir = '.../myhome/downloaddir/ccmi1'
    
# Specify the experiments for which data is requested
#  --- leave an empty list if only an inventory of available data
#      is requested
#trgexpt = []   # specification for an empty list
# trgexpt = [ 'refC1' , 'refC2' ]
trgexpt = [ 'refC1' ]

#
# Specify the variable names being requested
#    e.g. [ 'o3', 'ta', 'h2o' ]
#trgvar = ['h2o', 'ta', 'wtem' , 'vtem' , 'o3' ]
#         'o3strat', 'toz' , 'ps' ,'ua' , 'va' ,
#         'tatp' , 'ptp' , 'pr' ]
trgvar = [ 'zmcly' ]

# Specify frequency domain (mon = monthly, day = daily, etc.)
trgfreq = 'mon'

# Specify realm (atmos = atmosphere, ocean = ocean, etc.)
trgrealm = 'atmos'

# Specify frequency name (monthly, daily, etc.)
#   --- a bit of a redundant element in the directory structure
freqdict = { 'fx'  : 'fixed',
             'day' : 'daily',
             'mon' : 'monthly' ,
             'yr'  : 'annual' }
             
trgfreqname = freqdict[trgfreq]

# Bearer token generated through the CEDA website
#   "How to Generate an Access Token"
#          - https://help.ceda.ac.uk/article/5100-archive-access-tokens#how
# NOTE - The token is valid for three days after it is generated!
token = 'Copy and paste the token string here'

# User setting to determine whether to download/overwrite existing local files
OverwriteExistingFile=False

# Specify whether or not we want to generate a new inventory file -- inventory_file must be a valid file if so!
use_existing_inventory_file=False

# The inventory file must be specified here if use_existing_inventory_file=True
# The file name convention for the inventory file includes a yyyymmddhhmm datestring on the end of the file name
# If use_existing_inventory_file=False, the file name is generated below
if use_existing_inventory_file:
    inventory_file = 'CCMI-1_archive_202504031936'

# If verbose is True, the status of every file will be printed to screen
# If False, only newly downloaded files will be printed to screen
verbose = True

# URL for the download - this is the main difference from CCMI-2022
root_url='https://dap.ceda.ac.uk/thredds/badc/wcrp-ccmi/data/CCMI-1/output/'

## END USER SETTINGS SECTION

# %% ---- start of the script

print(' -- Variables being searched for ')
for var in trgvar:
    print(f'{var} in {trgfreq}/{trgrealm}/{trgfreqname}')

# %% Create or read in the inventory file
if use_existing_inventory_file:
    dstring= inventory_file.split('_')[-1]
    print('Using existing inventory file: ' + inventory_file)
    with open(inventory_file, 'r') as f:
        fullinvntry = eval(f.read())
else:
    tday = datetime.datetime.now(datetime.timezone.utc)
    dstring =  tday.strftime('%Y%m%d%H%M')
    inventory_file = 'CCMI-1_archive_'+dstring
    fullinvntry = create_inventory_file(inventory_file, verbose=verbose, root_url=root_url)
    print('Generated inventory file: ' + inventory_file)

# %% Now search the inventory for the given variables/experiments, and create a list of files to download
if trgexpt:
    filelist = search_inventory_ccmi1(root_url, trgexpt, trgvar, trgfreq, trgrealm, 
                                     trgfreqname, verbose=verbose)
else:
    print('No experiments specified - inventory only')
    filelist = []

# %% Now download the actual files

headers = {
  "Authorization": f"Bearer {token}"
}

for afile in filelist:
    # This is the URL of the file to download
    fileURL = 'https://dap.ceda.ac.uk/' + afile
    
    # Determine the output (destination) file path
    # The output file path == the download directory + the experiment name as a subdirectory + the file name
    destfilebase = os.path.basename(afile)           # e.g., 'zmcly_mon_CMAM_refC1_r1i1p1_197201-197212.nc'
    experiment = destfilebase.split('_')[3]          # e.g., 'refC1'
    destfile = os.path.join( downloaddir, experiment, destfilebase) # e.g., '/my/directory/refC1/zmcly_mon_CMAM_refC1_r1i1p1_197201-197212.nc'
    
    # Create the destination directory if it doesn't exist
    os.makedirs(os.path.dirname(destfile), exist_ok=True)
    
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
            response = session.get(fileURL, headers=headers)
            response.raise_for_status()  # Raise an exception for bad status codes
            with open(destfile, 'wb') as f:
                f.write(response.content)
            print('Downloaded ' + destfile)        
    except requests.exceptions.RequestException as e:
        print(f'Download failed: {fileURL} - Error: {e}')
    except Exception as e:
        print(f'Unexpected error downloading {fileURL}: {e}')

print('Download process completed!')
