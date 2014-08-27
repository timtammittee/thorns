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

import tables

import numpy as np
import pandas as pd
import tables as tbl


logger = logging.getLogger('thorns')



def get_store(workdir='work'):

    fname = os.path.join(workdir, 'store.db')

    if not os.path.exists(workdir):
        os.makedirs(workdir)

    store = shelve.open(fname, protocol=-1)

    return store




def dumpdb(data, name='dump', workdir='work', pickle=True ,kwargs=None):
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

    if pickle == True:
        store = pd.io.pytables.HDFStore(fname, 'a')
        store[key] = data

        store.close()
    else:
        _np_store(fname, key, data)


def _np_store(fname, key, data):

    is_array = lambda d : isinstance(d, np.ndarray)

    '''Find out which which columns contain numpy arrays'''
    storekeys = []
    seperatekeys = []
    for col in data:
        if True in map(is_array, data[col]):
            seperatekeys.append(col)
        else:
            storekeys.append(col)

    '''Store all non numpy columns'''
    store = pd.io.pytables.HDFStore(fname, 'a')
    store.put(key, data[storekeys])
    store.close()

    store = tbl.openFile(fname, 'a')
    for col in seperatekeys:
        group = store.createGroup("/" + key, "numpy_cols")
        atom = tbl.Atom.from_dtype(data[col][0].dtype)
        vlarray = store.createVLArray(group, col, atom)
        map(vlarray.append, data[col])
    store.close()



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

    if timestamp:
        raise NotImplementedError("Should add extra columnt with timestamps to the index of the output.")


    fname = os.path.join(workdir, name+'.h5')
    store = pd.io.pytables.HDFStore(fname, 'r')


    logger.info("Loading data from {}".format(fname))


    if load_all:
        xkeys = collections.OrderedDict() # poor-man's ordered set
        db = []

        ### Get all tables from the store
        for t in sorted(store.keys()):
            df = store[t]

            # Just want ordered unique values in xkeys (ordered set would
            # simplify it: orderedset.update(df.index.names))
            for name in df.index.names:
                xkeys[name] = None

            df = df.reset_index()
            db.append(df)

        store.close()


        db = pd.concat(db)

        db = db.drop_duplicates(
            subset=list(xkeys),
            take_last=True,
        )

        db = db.set_index(list(xkeys))

    else:
        last_key = sorted(store.keys())[-1]
        db = store[last_key]


    return db