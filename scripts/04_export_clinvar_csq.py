#!/usr/bin/env python3

import hail as hl
import argparse
import pandas
import os

from gnomad.utils.vep import process_consequences
from ukb_utils import hail_init
from ko_utils import io


def main(args):

    # parser
    input_path = args.input_path
    out_prefix = args.out_prefix
    
    hail_init.hail_bmrc_init_local('logs/hail/export_clinvar.log', 'GRCh38')
    recode = {f"{i}":f"chr{i}" for i in (list(range(1, 23)) + ['X', 'Y'])}
    ht = hl.import_vcf(input_path, force=True, force_bgz=True, skip_invalid_loci=True, contig_recoding=recode)
    ht = ht.explode_rows(ht.info.CLNSIG)
    ht = ht.rows()
    ht.describe()
    ht.write(out_prefix + ".ht", overwrite=True)
    ht = ht.flatten()
    ht.export(out_prefix + ".txt.gz")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # initial params
    parser.add_argument('--input_path', default=None, help='Path to input')
    parser.add_argument('--out_prefix', default=None,help='Path prefix for output dataset')

    args = parser.parse_args()

    main(args)

