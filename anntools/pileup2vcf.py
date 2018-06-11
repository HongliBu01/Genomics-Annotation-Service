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

import os
import datetime
import file_utils as fu


HETERO = {'M':'AC', 'R':'AG', 'W':'AT', 'S':'CG', 'Y':'CT', 'K':'GT'}
ACCEPTED_CHR = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20","21","22", "X", "Y", "MT"]
#ACCEPTED_CHR = ["X"]
#http://www.broadinstitute.org/gsa/wiki/index.php/Understanding_the_Unified_Genotyper's_VCF_files
def count_alt(depth, bases):
    bases = bases.upper()
    lst = list(bases)
    ast=0
    match_sum=0

    for l in lst:
        l=str(l)

        if str(l)=='.' or str(l)==',':
            match_sum=match_sum+1
        elif l=='*':
            ast=ast+1

    return int(depth) - (match_sum+ast)


def vcfheader(pileup):
    """ Generates VCF header """
    pileup = os.path.basename(pileup)
    pileup = os.path.splitext(pileup)[0]
    now = datetime.datetime.now()
    curdate=str(now.year)+'-'+str(now.month)+'-'+str(now.day)
    lines=[]
    lines.append('##fileformat=VCFv4.0')
    lines.append('##fileDate='+curdate)
    lines.append('##reference=1000Genomes-NCBI37')
    lines.append('##INFO=<ID=DB,Number=0,Type=Flag,Description="dbSNP membership, build 132">')
    lines.append('##FILTER=<ID=q30,Description="Quality below 30">')
    lines.append('##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">')
    lines.append('##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">')
    lines.append('##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read Depth">')
    lines.append('##FORMAT=<ID=AD,Number=1,Type=Integer,Description="Read Depth of Alternative Allele">')
    lines.append('#CHROM'+'\t'+'POS'+'\t'+'ID'+'\t'+'REF'+'\t'+'ALT'+'\t'+'QUAL'+'\t'+'FILTER'+'\t'+'INFO'+'\t'+'FORMAT' + '\t'+ pileup)
    return '\n'.join(lines)


def hetero2homo(ref, alt):
    """ Converts heterozygous symbols from Samtools pileup to A, G, T, C """
    if fu.isOnTheList(HETERO.keys(), alt) == False:
        return alt
    else:
        alt_x=HETERO[alt]
        if str(ref)==str(alt_x)[0]:
            return str(alt_x)[1]
        else:
            return str(alt_x)[0]


def varpileup_line2vcf_line(pileupfields):
    """ Converts Variant Pileup format to VCF format """

    t='\t'
    chr=str(pileupfields[0])
    pos=str(pileupfields[1])
    ref=str(pileupfields[2])
    alt=str(pileupfields[3])
    consqual=str(pileupfields[4])
    snpqual=str(pileupfields[5])
    mapqual=str(pileupfields[6])
    depth=str(pileupfields[7])
    alt_count=str(count_alt(depth, pileupfields[8]))

    GT='1/1'
    if fu.isOnTheList(HETERO.keys(), alt):
        GT='0/1'
        alt=hetero2homo(ref,alt)


    # 4 - Phred-scaled likelihood that the genotype is wrong, which is also called `consensus quality'.
    # 5 - Phred-scaled likelihood that the genotype is identical to the reference, which is also called `SNP quality'.
    #    Suppose the reference base is A and in alignment we see 17 G and 3 A. We will get a low consensus quality because
    #    it is difficult to distinguish an A/G heterozygote from a G/G homozygote. We will get a high SNP quality, though,
    #    because the evidence of a SNP is very strong.
    # 6 - root mean square (RMS) mapping quality
    return chr+t+pos+t+'.' +t+ ref +t+ alt +t+ mapqual +t+ 'PASS' +t+ '.' + t + 'GT:GQ:DP:AD'+t+GT+':'+consqual+':'+depth+':'+alt_count

def filter_pileup(pileup, outfile=None, chr_col=0, ref_col=2, alt_col=3, sep='\t'):


    fh = open(pileup, "r")
    if outfile is None:
        outfile=pileup+'.vcf'

    fu.delete(outfile)
    fh_out = open(outfile, "w")
    fh_out.write(vcfheader(pileup)+'\n')


    for line in fh:

        line = line.strip()
        fields=line.split(sep)

        chr=str(fields[chr_col])
        ref=str(fields[ref_col])
        alt=str(fields[alt_col])

        if (alt != ref) and (fu.find_first_index(ACCEPTED_CHR, chr.strip()) > -1):
            fh_out.write(varpileup_line2vcf_line(fields[0:9]) +'\n' )




def filter_vcf(pileup, outfile=None,  chr_col=0, ref_col=3, alt_col=4, sep='\t'):
    """ Removes lines where ALT==REF and chromosomes other than 1 - 22, X, Y and MT"""

    fh = open(pileup, "r")
    if outfile is None:
        outfile=pileup+'.filt'

    fu.delete(outfile)
    fh_out = open(outfile, "w")


    for line in fh:
        line = line.strip()
        if line.startswith('#'):
            fh_out.write(str(line)+'\n')
        else:

            fields=line.split(sep)
            if(len(fields)>=8):
                chr=str(fields[chr_col])
                ref=str(fields[ref_col])
                alt=str(fields[alt_col])

                if (alt != ref) and (fu.find_first_index(ACCEPTED_CHR, chr.strip()) > -1):
                    fh_out.write(str(line)+'\n')
