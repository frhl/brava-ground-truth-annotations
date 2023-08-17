#!/usr/bin/env python3

import hail as hl
import argparse
import pandas
import os

from gnomad.utils.vep import process_consequences
from ukb_utils import hail_init
from ko_utils import io

def convert_revel_scores_to_unique_float(table):
    # Define a transformation function
    def transform_string_to_float_array(s):
        parts = hl.str(s).split(',')

        float_parts = parts.map(
            lambda x: hl.if_else((x != ".") & hl.is_defined(
                hl.float64(x)), hl.float64(x), hl.null('float64'))
        ).filter(lambda x: hl.is_defined(x))

        return float_parts.first()

    # Apply the transformation function to the revel_score field
    new_table = table.annotate(
        vep=table.vep.annotate(
            transcript_consequences=table.vep.transcript_consequences.map(
                lambda tc: tc.annotate(
                    revel_score=transform_string_to_float_array(tc.revel_score)
                )
            )
        )
    )

    return new_table


def main(args):

    # parser
    input_path = args.input_path
    out_prefix = args.out_prefix

    # standard revel usage
    ht = hl.read_table(input_path)
    ht = convert_revel_scores_to_unique_float(ht)
    ht = process_consequences(ht)

    dbNSFP = hl.import_table(f'/well/lindgren/barney/brava_annotation/misc/bNSFP4.3a_chr21.bgz', no_header=True)
    dbNSFP = dbNSFP.rename({"f0":"#chr","f1":"pos(1-based)","f2":"ref","f3":"alt", "f4":"gnomAD_exomes_POPMAX_AF"})
    dbNSFP = dbNSFP.annotate(pos_int = hl.int(dbNSFP['pos(1-based)']))

    # Add 'chr' prefix to the chromosome field
    dbNSFP = dbNSFP.annotate(chr_prefixed = hl.str('chr') + dbNSFP['#chr'])

    # Now, create a locus<GRCh38> field
    dbNSFP = dbNSFP.annotate(locus = hl.locus(dbNSFP.chr_prefixed, dbNSFP.pos_int, reference_genome='GRCh38'))

    # Annotate combined_table with dbNSFP info
    ht = ht.key_by('locus', 'alleles')
    dbNSFP = dbNSFP.key_by('locus', 'ref', 'alt')

    # Merge the tables based on key
    ht = ht.annotate(gnomAD_exomes_POPMAX_AF = dbNSFP[ht.locus, ht.alleles[0], ht.alleles[1]].gnomAD_exomes_POPMAX_AF)

    # Filter down the table
    ht = ht.filter(hl.is_missing(ht.gnomAD_exomes_POPMAX_AF) |
                                     (ht.gnomAD_exomes_POPMAX_AF == ".") |
                                     (hl.float(ht.gnomAD_exomes_POPMAX_AF) < 0.01))

    ht.write(out_prefix + ".ht", overwrite=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # initial params
    parser.add_argument('--input_path', default=None, help='Path to input')
    parser.add_argument('--out_prefix', default=None,help='Path prefix for output dataset')

    args = parser.parse_args()

    main(args)

