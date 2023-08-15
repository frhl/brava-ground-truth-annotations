#!/usr/bin/env bash
#
#SBATCH --account=lindgren.prj
#SBATCH --job-name=worst_csq_by_gene_canonical
##SBATCH --chdir=/well/lindgren-ukbb/projects/ukbb-11867/flassen/projects/KO/brava-ground-truth-annotations
#SBATCH --chdir=/well/lindgren/barney/brava-ground-truth-annotations/
#SBATCH --output=logs/worst_csq_by_gene_canonical.log
#SBATCH --error=logs/worst_csq_by_gene_canonical.errors.log
#SBATCH --partition=short
#SBATCH --cpus-per-task 1
#SBATCH --array=21

source utils/qsub_utils.sh
source utils/hail_utils.sh

readonly spark_dir="data/tmp/spark"

readonly array_idx=$( get_array_task_id )
readonly chr=$( get_chr ${array_idx} )

readonly in_dir="data/vep-hail-out"
readonly in="${in_dir}/genebass.hailvep.gnomad_process_csqs.chr${chr}.ht"

readonly out_dir="data/vep-hail-out"
readonly out_prefix="${out_dir}/genebass.hailvep.gnomad_process_csqs.worst_csq_by_gene_canonical.more.chr${chr}"
readonly hail_script="scripts/03_worst_csq_by_gene_canonical.py"

mkdir -p ${out_dir}
mkdir -p ${spark_dir}

set_up_hail 0.2.97
set_up_pythonpath_legacy  
python3 ${hail_script} \
     --vep_path "${in}" \
     --out_prefix "${out_prefix}"




