#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""This module implements permanent store for data.

"""
from __future__ import division, print_function, absolute_import
from __future__ import unicode_literals

__author__ = "Marek Rudnicki"

import os
import datetime
import logging
from itertools import izip_longest
import shelve
import collections
import time

import tables

import numpy as np
import pandas as pd


logger = logging.getLogger('thorns')



def get_store(name='store', workdir='work'):
    """Return a quick and dirty shelve based persisten dict-like store.

    """

    fname = os.path.join(workdir, name + '.db')

    if not os.path.exists(workdir):
        os.makedirs(workdir)

    store = shelve.open(fname, protocol=-1)

    return store




def dumpdb(data, name='dump', workdir='work', kwargs=None):
    """Dump data in order to recall the most up-to-date records later.

    Parameters
    ----------
    data : pd.DataFrame
        Data that will be appended to the database.
    name : str, optional
        Base name of the pickle file.
    workdir : str, optional
        Directory for the data.
    kwargs : dict, optional
        Additional parameters common for all data (MultiIndex will be
        extended).

    """
    fname = os.path.join(workdir, name+'.h5')

    if not os.path.exists(workdir):
        os.makedirs(workdir)

    logger.info("Dumping data into {}.".format(fname))


    if kwargs is not None:
        for k,v in kwargs.items():
            data[k] = v
        data = data.set_index(kwargs.keys(), append=True)


    now = datetime.datetime.now()
    key = now.strftime("T%Y%m%d_%H%M%S_%f")

    store = pd.io.pytables.HDFStore(fname, 'a')
    store[key] = data

    store.close()


def dbinfo(workdir='work'):
    '''Get a list of all dumps in the work directory

    This function can be used to query a list of dumps that are
    currently located in the work directory. It also returns the most
    current timestamp and the filesize.

    Parameters:
    -----------
    workdir: str, optional
        The directory to look for data dumps. default is ./work
    
    Returns:
    --------
    pd.DataFrame
        Data about the dumps found in the work directory

    '''
    
    #Generate a list of h5 files in workdir
    file_list = []
    for fn in next(os.walk(workdir))[2]:
        if os.path.isfile(os.path.join(workdir, fn)) and fn.endswith('.h5'):
            file_list.append(os.path.join(workdir, fn))


    # Scan through the list of filenames and add those which can be
    # opend to a dataframe. also extract the wanted information and
    # add it to the df. Tables is used instead of the loaddb function
    # to save execution time for large files.
    df = pd.DataFrame([])
    for fl in file_list:
        try:
            h5file = tables.open_file(fl, mode = "r")
            timestamps = [node._v_name for node in h5file.list_nodes('/')]
            timestamps.sort()

            time_s = time.strptime(timestamps[-1][1:], "%Y%m%d_%H%M%S_%f")
            df = df.append({'name':fl.split('/')[-1].split('.')[0],
                            'timestamp': datetime.datetime(*(time_s[0:6])),
                            'filesize':h5file.get_filesize()}
                           ,ignore_index=True)

            h5file.close()

        except:
            pass

    return df

def loaddb(name='dump', workdir='work', timestamp=False, load_all=False):
    """Recall dumped data discarding duplicated records.

    Parameters
    ----------
    name : str, optional
        Base of the data filename.
    workdir : str, optional
        Directory where the data is stored.
    timestamp : bool, optional
        Add an extra column with timestamps to the index.
    load_all : bool, optional
        If True, data from all experiments will be loaded from the
        dumpdb file.  The default is to load only the most recent
        data.

    Returns
    -------
    pd.DataFrame
        Data without duplicates.

    """

    fname = os.path.join(workdir, name+'.h5')
    store = pd.io.pytables.HDFStore(fname, 'r')


    logger.info("Loading data from {}".format(fname))

    if load_all:
        xkeys = collections.OrderedDict() # poor-man's ordered set
        dbs = []

        ### Get all tables from the store
        for t in sorted(store.keys()):
            df = store[t]

            # Just want ordered unique values in xkeys (ordered set would
            # simplify it: orderedset.update(df.index.names))
            for name in df.index.names:
                xkeys.setdefault(name)

            df = df.reset_index()

            # Add a Timestamp to the results 
            if timestamp:
                time_s = time.strptime(t[1:], "T%Y%m%d_%H%M%S_%f")
                df['timestamp'] = datetime.datetime(*(time_s[0:6]))
                
            dbs.append(df)


        db = pd.concat(dbs)


    else:
        last_key = sorted(store.keys())[-1]
        df = store[last_key]

        xkeys = df.index.names

        # Add a Timestamp to the results 
        if timestamp:
            time_s = time.strptime(last_key[1:], "T%Y%m%d_%H%M%S_%f")
            df['timestamp'] = datetime.datetime(*(time_s[0:6]))

        db = df.reset_index()


    store.close()



    ### Drop duplicates and set index
    db = db.drop_duplicates(
        subset=list(xkeys),
        take_last=True,
    )

    db = db.set_index(list(xkeys))



    return db
