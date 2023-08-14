#!/usr/bin/env bash
#
#SBATCH --account=lindgren.prj
#SBATCH --job-name=export_clinvar_csq
#SBATCH --chdir=/well/lindgren-ukbb/projects/ukbb-11867/flassen/projects/KO/brava-ground-truth-annotations
#SBATCH --output=logs/export_clinvar_csq.log
#SBATCH --error=logs/export_clinvar_csq.errors.log
#SBATCH --partition=short
#SBATCH --cpus-per-task 1

set -o errexit
set -o nounset

source utils/qsub_utils.sh
source utils/hail_utils.sh

readonly spark_dir="data/tmp/spark"

readonly array_idx=$( get_array_task_id )
readonly chr=$( get_chr ${array_idx} )

readonly in_dir="/well/lindgren/flassen/ressources/clinvar/ftp2023"
readonly in="${in_dir}/clinvar.vcf.gz"

readonly out_dir="data/clinvar"
readonly out_prefix="${out_dir}/clinvar_csqs"
readonly hail_script="scripts/04_export_clinvar_csq.py"

mkdir -p ${out_dir}
mkdir -p ${spark_dir}

if [ ! -f "${out_prefix}_vep.ht/_SUCCESS" ]; then
  set_up_hail 0.2.97
  set_up_vep105
  set_up_pythonpath_legacy  
  python3 ${hail_script} \
       --input_path "${in}" \
       --out_prefix "${out_prefix}"
else
  >&2 echo "${out_prefix}* already exists."
fi




