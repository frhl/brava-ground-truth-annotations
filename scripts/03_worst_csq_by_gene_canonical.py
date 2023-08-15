#!/usr/bin/env python3

import hail as hl
import argparse

from ukb_utils import hail_init
from ko_utils import io
from ko_utils import ko

def main(args):

    # parser
    vep_path = args.vep_path
    out_prefix = args.out_prefix
    spliceai_path = args.spliceai_path

    # setup flags
    hail_init.hail_bmrc_init_local('logs/hail/hail_format.log', 'GRCh38')
    hl._set_flags(no_whole_stage_codegen='1') # from zulip
    ht = hl.read_table(vep_path)
    
    #spliceai = hl.read_table(spliceai_path)
    
    # get variant by canonical gene transcripts
    ht = ht.explode(ht.vep.worst_csq_by_gene_canonical)


    # assume spliceai is missing
    ht = ht.annotate(vep = ht.vep.annotate(
        worst_csq_by_gene_canonical = ht.vep.worst_csq_by_gene_canonical.annotate(SpliceAI_DS_max=hl.null('float64'))
    ))


    # get brava csqs
    ht = ht.annotate(
        brava_csqs=ko.csqs_case_builder_brava(
                worst_csq_expr=ht.vep.worst_csq_by_gene_canonical
        )
    )    
 

    # quick annotated with some useful info
    ht = ht.annotate(
            gene_symbol=ht.vep.worst_csq_by_gene_canonical.gene_symbol,
            gene_id=ht.vep.worst_csq_by_gene_canonical.gene_id,        
            transcript=ht.vep.worst_csq_by_gene_canonical.transcript_id,
            biotype=ht.vep.worst_csq_by_gene_canonical.biotype,
            mane_select=ht.vep.worst_csq_by_gene_canonical.mane_select,
            canonical=ht.vep.worst_csq_by_gene_canonical.canonical,
            csqs=ht.vep.worst_csq_by_gene_canonical.most_severe_consequence,
            revel_score=ht.vep.worst_csq_by_gene_canonical.revel_score,
            cadd_phred=ht.vep.worst_csq_by_gene_canonical.cadd_phred,
            spliceai_max_ds=ht.vep.worst_csq_by_gene_canonical.SpliceAI_DS_max,
            loftee_lof=ht.vep.worst_csq_by_gene_canonical.lof
    ) 
    
    # annotate with actual variant ID
    ht = ht.annotate(
        varid=hl.delimit([
            hl.str(ht.locus.contig),
            hl.str(ht.locus.position),
            ht.alleles[0],
            ht.alleles[1]],':')
    )

    ht = ht.select(*[ht.varid, ht.gene_symbol, ht.gene_id, ht.transcript, ht.biotype, ht.mane_select, ht.canonical, ht.csqs, ht.brava_csqs, ht.revel_score,ht.cadd_phred,ht.loftee_lof,ht.spliceai_max_ds])
    ht.write(out_prefix + ".ht", overwrite=True)
    ht.export(out_prefix + ".txt.gz")


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    # initial params
    parser.add_argument('--vep_path', default=None, help='Path to input')
    parser.add_argument('--spliceai_path', default=None, help='Path to spliceai VCF')
    parser.add_argument('--out_prefix', default=None, help='Path prefix for output dataset')

    args = parser.parse_args()

    main(args)



