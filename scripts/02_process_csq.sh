#!/usr/bin/env bash
#
#SBATCH --account=lindgren.prj
#SBATCH --job-name=process_csq
#SBATCH --chdir=/well/lindgren-ukbb/projects/ukbb-11867/flassen/projects/KO/brava-ground-truth-annotations
#SBATCH --output=logs/process_csq.log
#SBATCH --error=logs/process_csq.errors.log
#SBATCH --partition=short
#SBATCH --cpus-per-task 2
#SBATCH --array=21

set -o errexit
set -o nounset

source utils/qsub_utils.sh
source utils/hail_utils.sh

readonly spark_dir="data/tmp/spark"

readonly array_idx=$( get_array_task_id )
readonly chr=$( get_chr ${array_idx} )

readonly in_dir="data/vep-hail-out"
readonly in="${in_dir}/genebass.hailvep.newdb.chr${chr}.ht"

readonly out_dir="data/vep-hail-out"
readonly out_prefix="${out_dir}/genebass.hailvep.gnomad_process_csqs.newdb.chr${chr}"
readonly hail_script="scripts/02_process_csq.py"

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




