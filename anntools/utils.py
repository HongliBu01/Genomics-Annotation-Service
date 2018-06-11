#!/usr/bin/env python


################################################################################
#   Nov 17, 2011
#   Authors: Vlad Makarov, Chris Yoon
#   Language: Python
#   OS: UNIX/Linux, MAC OSX
#   Copyright (c) 2011, The Mount Sinai School of Medicine

#   Available under BSD  licence

#   Redistribution and use in source and binary forms, with or without modification,
#   are permitted provided that the following conditions are met:
#
#   Redistributions of source code must retain the above copyright notice,
#   this list of conditions and the following disclaimer.
#
#   Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation and/or
#   other materials provided with the distribution.
#
#   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
#   ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
#   WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
#   IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
#   INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
#   BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
#   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
#   OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
#   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
#   EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
################################################################################

""" Column inices for pileup and VCF"""
def getFormatSpecificIndices(format='vcf'):
    chr_ind = 0
    pos_ind = 1
    ref_ind = 3
    alt_ind = 4

    if (format !='vcf'):
        ref_ind = 2
        alt_ind = 3

    return [chr_ind, pos_ind, ref_ind, alt_ind]

""" Helper method to determine if two regions overlap"""
def isOverlap(testStart, testEnd, refStart, refEnd):
    if (((testStart <= refStart) and (testEnd >= refStart)) or ((testStart >= refStart) and (testStart <= refEnd))) :
        return True
    else:
        return False

""" Overlap between segments """
def getOverlap(testStart, testEnd, refStart, refEnd):
    return max(0,  ( min(testEnd, refEnd) - max(testStart, refStart) +1)  )


""" Helper method to calculate proportion of a CNV is in the segdup or other region
    Accepts numeric data, such as integer or float type
"""

def proportionOverlap(testStart, testEnd, refStart, refEnd):
    cnvlength=(testEnd - testStart) +1
    overlaplength=getOverlap(testStart, testEnd, refStart, refEnd)
    pctover=(float(overlaplength)/cnvlength)*100
    return round(pctover, 2)


""" Helper method to determine if the location is within the region"""
def isBetween(testStart, refStart, refEnd):
    if ((refStart<=testStart) and (testStart <= refEnd)) :
        return True
    else:
        return False

""" Helper method to deduplicate the list"""
def dedup(mylist):
    outlist = []
    for element in mylist:
        if element not in outlist:
            outlist.append(element)
    return outlist

""" Helper method to parse fields """
def parse_field(text, key, sep1, sep2):
    fields = text.strip().split(sep1)
    for f in fields:
        pairs = f.split(sep2)
        #if str(pairs[0]) == str(key):
        if str(pairs[0]).find(str(key)) > -1:
            return str(pairs[1])
    return '.'
