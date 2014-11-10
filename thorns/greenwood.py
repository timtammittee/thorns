# -*- coding: utf-8 -*-
#
# Copyright 2014 Michael Schutte
#
# This file is part of thorns.
#
# thorns is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# thorns is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with thorns.  If not, see <http://www.gnu.org/licenses/>.


"""The Greenwood cochlear position/frequency function.

"""

from __future__ import division, print_function, absolute_import

__author__ = "Michael Schutte"

import pandas as pd

HUMAN = {'A': 165.4, 'a': 0.06e3, 'k': 1}

def place_to_frequency(place, species=HUMAN):
    """Convert a place on the basilar membrane (distance from the
    cochlear apex in meters) to its center frequency according to
    Greenwood, J. Acoust. Soc. Am. 87(6):2592-2605, 1990.

    Parameters
    ----------
    place : (array of) float or pandas.DataFrame
        Either a distance from the cochlear apex, measured in meters, or
        a pandas.DataFrame with a column named `x` containing such data.

    Returns
    -------
    (array of) float or pandas.DataFrame
        The computed frequency values.  If `place` is a
        pandas.DataFrame, the result is a new pandas.DataFrame with the
        results in a column named `f`.

    """

    A, a, k = species['A'], species['a'], species['k']
    convert = lambda x: A * (10 ** (a*x) - k)
    if isinstance(place, pd.DataFrame) and 'x' in place:
        return convert(pd.DataFrame.from_dict({'f': place['x']}))
    else:
        return convert(place)
